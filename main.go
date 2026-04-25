// Command crawler-worker consumes URLs from the "discovered-urls" Kafka topic,
// fetches each page, and publishes the result to the "crawled-urls" topic.
//
// Multiple instances can run in parallel under the same consumer group
// ("crawler-workers") – Kafka will distribute partitions across them.
//
// Usage:
//
//	crawler-worker [flags]
//
// Flags:
//
//	-kafka         Kafka broker address (default localhost:9092)
//	-redis         Redis address for dedup (default localhost:6379)
//	-workers       parallel fetch goroutines (default 8)
//	-timeout       per-request HTTP timeout (default 15s)
//	-max-body      maximum response body bytes (default 5 MiB)
//	-crawl-delay   per-domain politeness delay (default 1s)
//	-agent         User-Agent string
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shakhawathossain/crawler-worker/events"
	"github.com/shakhawathossain/crawler-worker/internal/fetcher"
	"github.com/shakhawathossain/crawler-worker/internal/ratelimiter"
	"github.com/shakhawathossain/crawler-worker/internal/robots"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerGroup  = "crawler-workers"
	redisSeenKey   = "webcrawler:seen_urls"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker address")
	redisAddr   := flag.String("redis", "localhost:6379", "Redis address for URL dedup")
	numWorkers  := flag.Int("workers", 8, "Parallel fetch goroutines")
	timeout     := flag.Duration("timeout", 15*time.Second, "Per-request HTTP timeout")
	maxBody     := flag.Int64("max-body", 5<<20, "Maximum response body bytes")
	crawlDelay  := flag.Duration("crawl-delay", 1*time.Second, "Per-domain politeness delay")
	agent       := flag.String("agent", "go-web-crawler/1.0", "User-Agent string")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("shutting down"); cancel() }()

	// ── Redis dedup ───────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}
	defer rdb.Close()

	// ── Kafka consumer + producer ─────────────────────────────────────────────
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*kafkaBroker),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(events.TopicDiscovered),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	// ── Shared components ─────────────────────────────────────────────────────
	fet := fetcher.New(*timeout, *maxBody, *agent)
	rob := robots.New(*agent)
	rl  := ratelimiter.New(*crawlDelay)

	sem := make(chan struct{}, *numWorkers) // bounded concurrency

	log.Printf("crawler-worker started: group=%s workers=%d", consumerGroup, *numWorkers)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error: %v", e.Err)
			}
			continue
		}

		var wg sync.WaitGroup
		fetches.EachRecord(func(r *kgo.Record) {
			var ev events.DiscoveredURL
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}

			// Dedup check via Redis SADD (returns 1 if new, 0 if exists)
			added, err := rdb.SAdd(ctx, redisSeenKey, ev.URL).Result()
			if err != nil || added == 0 {
				return // already seen or Redis error
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(ev events.DiscoveredURL) {
				defer func() { <-sem; wg.Done() }()
				processURL(ctx, ev, cl, fet, rob, rl)
			}(ev)
		})
		wg.Wait()

		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}

func processURL(
	ctx context.Context,
	ev events.DiscoveredURL,
	cl *kgo.Client,
	fet *fetcher.Fetcher,
	rob *robots.Checker,
	rl *ratelimiter.DomainLimiter,
) {
	// ① Robots.txt
	if !rob.Allowed(ev.URL) {
		log.Printf("[robots]  %s", ev.URL)
		return
	}

	// ② Rate limit
	rl.Wait(ev.URL)

	// ③ Fetch
	resp := fet.Fetch(ctx, ev.URL)
	if resp.Err != nil {
		if ctx.Err() == nil {
			log.Printf("[err]     %s — %v", ev.URL, resp.Err)
		}
		return
	}
	if resp.StatusCode >= 400 {
		log.Printf("[%d]       %s", resp.StatusCode, ev.URL)
		return
	}

	finalURL := resp.FinalURL
	if finalURL == "" {
		finalURL = ev.URL
	}

	// ④ Gzip-compress the body to keep Kafka message sizes small
	compressed, err := gzipCompress(resp.Body)
	if err != nil {
		log.Printf("[compress] %s — %v", finalURL, err)
		compressed = resp.Body // fall back to raw
	}

	// ⑤ Publish to crawled-urls
	page := events.CrawledPage{
		URL:        ev.URL,
		FinalURL:   finalURL,
		Depth:      ev.Depth,
		HTTPStatus: resp.StatusCode,
		Body:       compressed,
		CrawledAt:  time.Now().UTC(),
	}
	val, err := json.Marshal(page)
	if err != nil {
		log.Printf("[marshal]  %s — %v", finalURL, err)
		return
	}
	rec := &kgo.Record{
		Topic: events.TopicCrawled,
		Key:   []byte(finalURL),
		Value: val,
	}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		if ctx.Err() == nil {
			log.Printf("[kafka]    %s — %v", finalURL, err)
		}
		return
	}
	log.Printf("[%d] depth=%-2d  %s", resp.StatusCode, ev.Depth, finalURL)
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

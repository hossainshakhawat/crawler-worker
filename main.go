// Command crawler-worker consumes URLs from the "discovered-urls" Kafka topic,
// fetches each page, and publishes the result to the "crawled-urls" topic.
//
// Multiple instances can run in parallel under the same consumer group
// ("crawler-workers") – Kafka will distribute partitions across them.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. CLI flags
//  2. Environment variables  (prefix WORKER_, e.g. WORKER_KAFKA_BROKER)
//  3. config.yml             (must be in the working directory)
//  4. Built-in defaults
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
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hossainshakhawat/crawler-worker/events"
	"github.com/hossainshakhawat/crawler-worker/internal/fetcher"
	"github.com/hossainshakhawat/crawler-worker/internal/kafkaconn"
	"github.com/hossainshakhawat/crawler-worker/internal/ratelimiter"
	"github.com/hossainshakhawat/crawler-worker/internal/redisconn"
	"github.com/hossainshakhawat/crawler-worker/internal/robots"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerGroup = "crawler-workers"
	redisSeenKey  = "webcrawler:seen_urls"
)

type config struct {
	kafkaBroker string
	redisAddr   string
	numWorkers  int
	timeout     time.Duration
	maxBody     int64
	crawlDelay  time.Duration
	agent       string
}

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetDefault("kafka_broker", "localhost:9092")
	viper.SetDefault("redis_addr", "localhost:6379")
	viper.SetDefault("workers", 8)
	viper.SetDefault("timeout", "15s")
	viper.SetDefault("max_body", 5<<20)
	viper.SetDefault("crawl_delay", "1s")
	viper.SetDefault("agent", "go-web-crawler/1.0")

	viper.SetEnvPrefix("WORKER")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	// Flags override env vars and config.yml; defaults come from Viper
	// so env vars and config.yml flow through when flags are not set.
	kafka := flag.String("kafka", viper.GetString("kafka_broker"), "Kafka broker address")
	redisAddr := flag.String("redis", viper.GetString("redis_addr"), "Redis address for URL dedup")
	workers := flag.Int("workers", viper.GetInt("workers"), "Parallel fetch goroutines")
	timeout := flag.Duration("timeout", viper.GetDuration("timeout"), "Per-request HTTP timeout")
	maxBody := flag.Int64("max-body", viper.GetInt64("max_body"), "Maximum response body bytes")
	crawlDelay := flag.Duration("crawl-delay", viper.GetDuration("crawl_delay"), "Per-domain politeness delay")
	agent := flag.String("agent", viper.GetString("agent"), "User-Agent string")
	flag.Parse()

	return config{
		kafkaBroker: *kafka,
		redisAddr:   *redisAddr,
		numWorkers:  *workers,
		timeout:     *timeout,
		maxBody:     *maxBody,
		crawlDelay:  *crawlDelay,
		agent:       *agent,
	}
}

func listenForShutdown(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("shutting down"); cancel() }()
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenForShutdown(cancel)

	redisClient, err := redisconn.New(ctx, cfg.redisAddr)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	kafkaClient, err := kafkaconn.New(cfg.kafkaBroker, consumerGroup)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	httpFetcher := fetcher.New(cfg.timeout, cfg.maxBody, cfg.agent)
	robotsChecker := robots.New(cfg.agent)
	rateLimiter := ratelimiter.New(cfg.crawlDelay)

	log.Printf("crawler-worker started: group=%s workers=%d", consumerGroup, cfg.numWorkers)

	run(ctx, kafkaClient, redisClient, httpFetcher, robotsChecker, rateLimiter, cfg.numWorkers)
}

func run(
	ctx context.Context,
	kafkaClient *kgo.Client,
	redisClient *redis.Client,
	httpFetcher *fetcher.Fetcher,
	robotsChecker *robots.Checker,
	rateLimiter *ratelimiter.DomainLimiter,
	numWorkers int,
) {
	semaphore := make(chan struct{}, numWorkers)

	for {
		fetches := kafkaClient.PollFetches(ctx)
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
		fetches.EachRecord(func(record *kgo.Record) {
			var event events.DiscoveredURL
			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}

			// Dedup check via Redis SADD (returns 1 if new, 0 if exists)
			added, err := redisClient.SAdd(ctx, redisSeenKey, event.URL).Result()
			if err != nil {
				log.Printf("[skip:redis-err] %s — %v", event.URL, err)
				return
			}
			if added == 0 {
				log.Printf("[skip:duplicate] %s", event.URL)
				return
			}

			semaphore <- struct{}{}
			wg.Add(1)
			go func(event events.DiscoveredURL) {
				defer func() { <-semaphore; wg.Done() }()
				processURL(ctx, event, kafkaClient, httpFetcher, robotsChecker, rateLimiter)
			}(event)
		})
		wg.Wait()

		if err := kafkaClient.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}

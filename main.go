// Command crawler-worker consumes URLs from the "discovered-urls" Kafka topic,
// fetches each page, and publishes the result to the "crawled-urls" topic.
//
// Multiple instances can run in parallel under the same consumer group
// ("crawler-workers") – Kafka will distribute partitions across them.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. Environment variables  (prefix WORKER_, e.g. WORKER_KAFKA_BROKER)
//  2. config.yml             (must be in the working directory)
//  3. Built-in defaults
package main

import (
	"context"
	"encoding/json"
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
	kafkaBroker     string
	redisAddr       string
	numWorkers      int
	timeout         time.Duration
	maxBody         int64
	crawlDelay      time.Duration
	agent           string
	topicDiscovered string
	topicCrawled    string
}

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetEnvPrefix("WORKER")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	return config{
		kafkaBroker:     viper.GetString("kafka_broker"),
		redisAddr:       viper.GetString("redis_addr"),
		numWorkers:      viper.GetInt("workers"),
		timeout:         viper.GetDuration("timeout"),
		maxBody:         viper.GetInt64("max_body"),
		crawlDelay:      viper.GetDuration("crawl_delay"),
		agent:           viper.GetString("agent"),
		topicDiscovered: viper.GetString("topic_discovered"),
		topicCrawled:    viper.GetString("topic_crawled"),
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

	kafkaClient, err := kafkaconn.New(cfg.kafkaBroker, consumerGroup, cfg.topicDiscovered)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	httpFetcher := fetcher.New(cfg.timeout, cfg.maxBody, cfg.agent)
	robotsChecker := robots.New(cfg.agent)
	rateLimiter := ratelimiter.New(cfg.crawlDelay)

	log.Printf("crawler-worker started: group=%s workers=%d", consumerGroup, cfg.numWorkers)

	run(ctx, kafkaClient, redisClient, httpFetcher, robotsChecker, rateLimiter, cfg.numWorkers, cfg.topicCrawled)
}

func run(
	ctx context.Context,
	kafkaClient *kgo.Client,
	redisClient *redis.Client,
	httpFetcher *fetcher.Fetcher,
	robotsChecker *robots.Checker,
	rateLimiter *ratelimiter.DomainLimiter,
	numWorkers int,
	topicCrawled string,
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
					processURL(ctx, event, kafkaClient, httpFetcher, robotsChecker, rateLimiter, topicCrawled)
			}(event)
		})
		wg.Wait()

		if err := kafkaClient.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}

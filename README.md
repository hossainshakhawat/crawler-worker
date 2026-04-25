# crawler-worker

Consumes URLs from the `discovered-urls` Kafka topic, fetches each page respecting `robots.txt` and per-domain rate limits, deduplicates via Redis, and publishes gzip-compressed HTML to the `crawled-urls` topic for downstream parsing.

Seed URLs are injected into the pipeline by [`crawler-seed`](../crawler-seed), which exposes a `POST /seed` HTTP API.

## Architecture overview

```
POST /seed
    │
    ▼
crawler-seed (HTTP server)
    │
    ▼
[discovered-urls]  ──►  crawler-worker  ──►  [crawled-urls]
                              │
                        Redis (dedup)
                        robots.txt cache
                        per-domain rate limiter
```

Multiple instances of `crawler-worker` can run in parallel under the same Kafka consumer group (`crawler-workers`). Kafka distributes `discovered-urls` partitions across all active instances automatically.

## Prerequisites

| Dependency | Minimum version | Notes |
|------------|-----------------|-------|
| Go         | 1.25            |       |
| Kafka      | 3.x             | Topics `discovered-urls` and `crawled-urls` must exist |
| Redis      | 6.x             | Used for URL deduplication (`SADD` on `webcrawler:seen_urls`) |

## Configuration

Configuration is read from `config.yml` in the working directory. All keys can be overridden by environment variables with the `WORKER_` prefix (e.g. `WORKER_KAFKA_BROKER`).

| Key                | Default              | Env var                    | Description                                       |
|--------------------|----------------------|----------------------------|---------------------------------------------------|
| `kafka_broker`     | `localhost:9092`     | `WORKER_KAFKA_BROKER`      | Kafka broker address                              |
| `redis_addr`       | `localhost:6379`     | `WORKER_REDIS_ADDR`        | Redis address for URL deduplication               |
| `workers`          | `8`                  | `WORKER_WORKERS`           | Number of parallel fetch goroutines               |
| `timeout`          | `15s`                | `WORKER_TIMEOUT`           | Per-request HTTP timeout                          |
| `max_body`         | `5242880` (5 MiB)    | `WORKER_MAX_BODY`          | Maximum response body size in bytes               |
| `crawl_delay`      | `100ms`              | `WORKER_CRAWL_DELAY`       | Minimum delay between requests to the same domain |
| `agent`            | `go-web-crawler/1.0` | `WORKER_AGENT`             | `User-Agent` header sent with every request       |
| `topic_discovered` | `discovered-urls`    | `WORKER_TOPIC_DISCOVERED`  | Kafka topic to consume URLs from                  |
| `topic_crawled`    | `crawled-urls`       | `WORKER_TOPIC_CRAWLED`     | Kafka topic to publish crawled pages to           |

## Running

```bash
# Build
go build -o crawler-worker ./...

# Run with defaults (reads config.yml in the working directory)
./crawler-worker

# Override individual values with environment variables
WORKER_KAFKA_BROKER=broker:9092 \
WORKER_REDIS_ADDR=redis:6379 \
WORKER_WORKERS=16 \
WORKER_TIMEOUT=30s \
WORKER_CRAWL_DELAY=2s \
WORKER_AGENT=mybot/1.0 \
./crawler-worker

# Use custom Kafka topics
WORKER_TOPIC_DISCOVERED=my-urls \
WORKER_TOPIC_CRAWLED=my-pages \
./crawler-worker

# Run multiple instances for higher throughput (same consumer group)
./crawler-worker &
./crawler-worker &
./crawler-worker &
```

Shut down gracefully with `SIGINT` or `SIGTERM`. The worker commits Kafka offsets before exiting.

## What it does

For every URL consumed from `discovered-urls`:

1. **Deduplication** — Attempts `SADD webcrawler:seen_urls <url>` in Redis. Skips the URL if it was already seen (returns `0`).
2. **Robots.txt** — Checks whether the URL is allowed by the target site's `robots.txt`. Skips disallowed URLs.
3. **Rate limiting** — Waits until the per-domain politeness delay has elapsed since the last request to that host.
4. **Fetch** — Issues an HTTP GET with a configurable `User-Agent`, follows redirects, and caps the response body at `-max-body` bytes.
5. **Publish** — Gzip-compresses the body and publishes a `CrawledPage` event to `crawled-urls`:
   ```json
   {
     "url": "https://example.com",
     "final_url": "https://www.example.com/",
     "depth": 0,
     "http_status": 200,
     "body": "<base64-encoded gzip>",
     "crawled_at": "2026-04-25T10:00:01Z"
   }
   ```
6. **Offset commit** — Commits Kafka consumer offsets after each poll batch.

Pages with HTTP status ≥ 400 are logged and dropped (not published).

## Internal packages

| Package                    | Responsibility                                          |
|----------------------------|---------------------------------------------------------|
| `internal/fetcher`         | HTTP client with configurable timeout, body cap, and UA |
| `internal/robots`          | In-memory `robots.txt` cache per host                   |
| `internal/ratelimiter`     | Per-domain delay tracker using a mutex-protected map    |
| `internal/redisconn`       | Redis client factory with connectivity check            |
| `internal/kafkaconn`       | Kafka consumer-producer client factory                  |

## Kafka topics

Topic names are configurable via `config.yml` or environment variables (see Configuration above).

| Config key         | Default           | Direction | Message type    |
|--------------------|-------------------|-----------|-----------------|
| `topic_discovered` | `discovered-urls` | Consume   | `DiscoveredURL` |
| `topic_crawled`    | `crawled-urls`    | Produce   | `CrawledPage`   |

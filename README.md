# crawler-worker

Consumes URLs from the `discovered-urls` Kafka topic, fetches each page respecting `robots.txt` and per-domain rate limits, deduplicates via Redis, and publishes gzip-compressed HTML to the `crawled-urls` topic for downstream parsing.

## Architecture overview

```
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

All options are passed as command-line flags.

| Flag           | Default              | Description                                       |
|----------------|----------------------|---------------------------------------------------|
| `-kafka`       | `localhost:9092`     | Kafka broker address                              |
| `-redis`       | `localhost:6379`     | Redis address for URL deduplication               |
| `-workers`     | `8`                  | Number of parallel fetch goroutines               |
| `-timeout`     | `15s`                | Per-request HTTP timeout                          |
| `-max-body`    | `5242880` (5 MiB)    | Maximum response body size in bytes               |
| `-crawl-delay` | `1s`                 | Minimum delay between requests to the same domain |
| `-agent`       | `go-web-crawler/1.0` | `User-Agent` header sent with every request       |

## Running

```bash
# Build
go build -o crawler-worker ./...

# Run with defaults (Kafka and Redis on localhost)
./crawler-worker

# Run with custom settings
./crawler-worker \
  -kafka broker:9092 \
  -redis redis:6379 \
  -workers 16 \
  -timeout 30s \
  -crawl-delay 2s \
  -agent "mybot/1.0"

# Run multiple instances for higher throughput (same consumer group)
./crawler-worker -kafka broker:9092 &
./crawler-worker -kafka broker:9092 &
./crawler-worker -kafka broker:9092 &
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

| Topic             | Direction | Message type    |
|-------------------|-----------|-----------------|
| `discovered-urls` | Consume   | `DiscoveredURL` |
| `crawled-urls`    | Produce   | `CrawledPage`   |

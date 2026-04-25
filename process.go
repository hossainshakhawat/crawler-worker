package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hossainshakhawat/crawler-worker/events"
	"github.com/hossainshakhawat/crawler-worker/internal/fetcher"
	"github.com/hossainshakhawat/crawler-worker/internal/ratelimiter"
	"github.com/hossainshakhawat/crawler-worker/internal/robots"
	"github.com/twmb/franz-go/pkg/kgo"
)

func processURL(
	ctx context.Context,
	event events.DiscoveredURL,
	kafkaClient *kgo.Client,
	httpFetcher *fetcher.Fetcher,
	robotsChecker *robots.Checker,
	rateLimiter *ratelimiter.DomainLimiter,
) {
	// ① Robots.txt
	if !robotsChecker.Allowed(event.URL) {
		log.Printf("[robots]  %s", event.URL)
		return
	}

	// ② Rate limit
	rateLimiter.Wait(event.URL)

	// ③ Fetch
	response := httpFetcher.Fetch(ctx, event.URL)
	if response.Err != nil {
		if ctx.Err() == nil {
			log.Printf("[err]     %s — %v", event.URL, response.Err)
		}
		return
	}
	if response.StatusCode >= 400 {
		log.Printf("[%d]       %s", response.StatusCode, event.URL)
		return
	}

	finalURL := response.FinalURL
	if finalURL == "" {
		finalURL = event.URL
	}

	// ④ Gzip-compress the body to keep Kafka message sizes small
	compressed, err := gzipCompress(response.Body)
	if err != nil {
		log.Printf("[compress] %s — %v", finalURL, err)
		compressed = response.Body // fall back to raw
	}

	// ⑤ Publish to crawled-urls
	page := events.CrawledPage{
		URL:        event.URL,
		FinalURL:   finalURL,
		Depth:      event.Depth,
		HTTPStatus: response.StatusCode,
		Body:       compressed,
		CrawledAt:  time.Now().UTC(),
	}
	payload, err := json.Marshal(page)
	if err != nil {
		log.Printf("[marshal]  %s — %v", finalURL, err)
		return
	}
	kafkaRecord := &kgo.Record{
		Topic: events.TopicCrawled,
		Key:   []byte(finalURL),
		Value: payload,
	}
	if err := kafkaClient.ProduceSync(ctx, kafkaRecord).FirstErr(); err != nil {
		if ctx.Err() == nil {
			log.Printf("[kafka]    %s — %v", finalURL, err)
		}
		return
	}
	log.Printf("[%d] depth=%-2d  %s", response.StatusCode, event.Depth, finalURL)
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

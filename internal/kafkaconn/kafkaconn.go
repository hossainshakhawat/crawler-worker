package kafkaconn

import (
	"fmt"

	"github.com/shakhawathossain/crawler-worker/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// New creates a Kafka client configured as a consumer-producer for the crawler worker.
func New(broker, group string) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(events.TopicDiscovered),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return cl, nil
}

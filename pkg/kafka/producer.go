package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// Producer wraps Kafka producer functionality
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{}, // Hash-based partitioning
		// Idempotent producer (exactly-once semantics)
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Producer{writer: writer}
}

// PublishEvent publishes an event to Kafka
func (p *Producer) PublishEvent(ctx context.Context, key string, event interface{}) error {
	// Serialize event to JSON
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create Kafka message
	msg := kafka.Message{
		Key:   []byte(key), // Used for partitioning
		Value: eventBytes,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}

	// Write message
	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	log.Printf("✓ Published event to Kafka: key=%s, topic=%s\n", key, p.writer.Topic)
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.writer.Close()
}

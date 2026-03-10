package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler is a function that processes messages
type MessageHandler func(ctx context.Context, message []byte) error

// Consumer wraps Kafka consumer functionality
type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(brokers []string, topic, groupID string, handler MessageHandler) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // Start from beginning for new consumers
		// Exactly-once semantics
		IsolationLevel: kafka.ReadCommitted,
	})

	return &Consumer{
		reader:  reader,
		handler: handler,
	}
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("🎧 Starting Kafka consumer for topic: %s\n", c.reader.Config().Topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Stopping consumer...")
			return c.reader.Close()
		default:
			// Fetch message
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("❌ Error fetching message: %v\n", err)
				continue
			}

			// Process message
			if err := c.processMessage(ctx, msg); err != nil {
				log.Printf("❌ Error processing message: %v\n", err)
				// In production, send to DLQ
				continue
			}

			// Commit offset after successful processing
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("❌ Error committing message: %v\n", err)
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	log.Printf("📨 Received message: partition=%d, offset=%d, key=%s\n",
		msg.Partition, msg.Offset, string(msg.Key))

	// Call handler
	return c.handler(ctx, msg.Value)
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}

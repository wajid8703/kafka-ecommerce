package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageHandler func(ctx context.Context, message []byte) error

type DLQProducer interface {
	SendToDLQ(ctx context.Context, msg kafka.Message, err error) error
}
type Consumer struct {
	reader      *kafka.Reader
	handler     MessageHandler
	dlqProducer DLQProducer
	MaxRetries  int
}

func NewConsumer(brokers []string, topic, groupID string, handler MessageHandler, dlqProducer DLQProducer) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		// Exactly-once semantics
		IsolationLevel: kafka.ReadCommitted,
		// Retry configuration
		MaxWait: 1 * time.Second,
	})

	return &Consumer{
		reader:      reader,
		handler:     handler,
		dlqProducer: dlqProducer,
		MaxRetries:  3,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("🎧 Starting Kafka consumer: topic=%s, group=%s\n",
		c.reader.Config().Topic, c.reader.Config().GroupID)

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Stopping consumer...")
			return c.reader.Close()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("❌ Error fetching message: %v\n", err)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				log.Printf("❌ Error processing message: %v\n", err)
				// TODO: Send to Dead Letter Queue
				continue
			}

			// Commit only after successful processing
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("❌ Error committing message: %v\n", err)
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	log.Printf("📨 Received: partition=%d, offset=%d, key=%s\n",
		msg.Partition, msg.Offset, string(msg.Key))

	return c.handler(ctx, msg.Value)
}

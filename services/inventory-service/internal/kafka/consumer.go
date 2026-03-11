package kafka

import (
	"context"
	"fmt"
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
	maxRetries  int
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
		IsolationLevel: kafka.ReadCommitted,
		MaxWait:        1 * time.Second,
	})

	return &Consumer{
		reader:      reader,
		handler:     handler,
		dlqProducer: dlqProducer,
		maxRetries:  3, // Retry 3 times before DLQ
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

			if err := c.processMessageWithRetry(ctx, msg); err != nil {
				log.Printf("❌ Failed after retries, sending to DLQ: %v\n", err)

				// Send to DLQ
				if c.dlqProducer != nil {
					if dlqErr := c.dlqProducer.SendToDLQ(ctx, msg, err); dlqErr != nil {
						log.Printf("❌ Failed to send to DLQ: %v\n", dlqErr)
					}
				}
			}

			// Always commit - we've either processed or sent to DLQ
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("❌ Error committing message: %v\n", err)
			}
		}
	}
}

func (c *Consumer) processMessageWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		log.Printf("📨 Processing (attempt %d/%d): partition=%d, offset=%d\n",
			attempt, c.maxRetries, msg.Partition, msg.Offset)

		err := c.handler(ctx, msg.Value)
		if err == nil {
			return nil // Success!
		}

		lastErr = err
		log.Printf("⚠️  Processing failed (attempt %d/%d): %v\n", attempt, c.maxRetries, err)

		// Exponential backoff
		if attempt < c.maxRetries {
			backoff := time.Duration(attempt*attempt) * time.Second
			log.Printf("⏳ Retrying in %v...\n", backoff)
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed after %d retries: %w", c.maxRetries, lastErr)
}

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	ensureTopic(brokers, topic)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}

	return &Producer{writer: writer}
}

func ensureTopic(brokers []string, topic string) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("⚠ Could not connect to Kafka to ensure topic: %v", err)
		return
	}
	defer func(conn *kafka.Conn) {
		if err := conn.Close(); err != nil {
			log.Printf("⚠ Failed to close Kafka connection: %v", err)
		}
	}(conn)

	controller, err := conn.Controller()
	if err != nil {
		log.Printf("⚠ Could not get Kafka controller: %v", err)
		return
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		log.Printf("⚠ Could not connect to Kafka controller: %v", err)
		return
	}
	defer func(controllerConn *kafka.Conn) {
		if err := controllerConn.Close(); err != nil {
			log.Printf("⚠ Failed to close Kafka controller connection: %v", err)
		}
	}(controllerConn)

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("⚠ Could not create topic %s (may already exist): %v", topic, err)
		return
	}

	log.Printf("✓ Kafka topic ensured: %s", topic)
}

func (p *Producer) PublishEvent(ctx context.Context, key string, event any) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: eventBytes,
	}

	var writeErr error
	for attempt := range 3 {
		writeErr = p.writer.WriteMessages(ctx, msg)
		if writeErr == nil {
			break
		}
		if !isRetryableKafkaError(writeErr) {
			break
		}
		wait := time.Duration(attempt+1) * 500 * time.Millisecond
		log.Printf("⚠ Kafka write error (attempt %d/3), retrying in %s: %v", attempt+1, wait, writeErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	if writeErr != nil {
		return fmt.Errorf("failed to write message: %w", writeErr)
	}

	log.Printf("✓ Published to Kafka: topic=%s, key=%s\n", p.writer.Topic, key)
	return nil
}

func isRetryableKafkaError(err error) bool {
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		switch kafkaErr {
		case kafka.LeaderNotAvailable, kafka.NotLeaderForPartition,
			kafka.RequestTimedOut, kafka.BrokerNotAvailable:
			return true
		}
	}
	return false
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

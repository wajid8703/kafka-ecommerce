package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// FailedMessage represents a message that failed processing
type FailedMessage struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalKey       string    `json:"original_key"`
	OriginalValue     string    `json:"original_value"`
	ErrorMessage      string    `json:"error_message"`
	ErrorStackTrace   string    `json:"error_stack_trace"`
	FailedAt          time.Time `json:"failed_at"`
	RetryCount        int       `json:"retry_count"`
	ServiceName       string    `json:"service_name"`
}

// DLQProducer handles sending failed messages to DLQ
type DLQProducer struct {
	writer      *kafka.Writer
	serviceName string
}

// NewDLQProducer creates a new DLQ producer
func NewDLQProducer(brokers []string, serviceName string) *DLQProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "dead-letter-queue",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &DLQProducer{
		writer:      writer,
		serviceName: serviceName,
	}
}

// SendToDLQ sends a failed message to the dead letter queue
func (d *DLQProducer) SendToDLQ(ctx context.Context, msg kafka.Message, err error) error {
	failedMsg := FailedMessage{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		OriginalKey:       string(msg.Key),
		OriginalValue:     string(msg.Value),
		ErrorMessage:      err.Error(),
		FailedAt:          time.Now(),
		RetryCount:        0,
		ServiceName:       d.serviceName,
	}

	data, jsonErr := json.Marshal(failedMsg)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", jsonErr)
	}

	dlqMsg := kafka.Message{
		Key:   []byte(fmt.Sprintf("dlq-%s", msg.Key)),
		Value: data,
	}

	if writeErr := d.writer.WriteMessages(ctx, dlqMsg); writeErr != nil {
		return fmt.Errorf("failed to write to DLQ: %w", writeErr)
	}

	log.Printf("💀 Sent to DLQ: service=%s, original_topic=%s, error=%s\n",
		d.serviceName, msg.Topic, err.Error())

	return nil
}

// Close closes the DLQ producer
func (d *DLQProducer) Close() error {
	return d.writer.Close()
}

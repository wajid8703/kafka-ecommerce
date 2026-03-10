package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type KafkaProducer interface {
	PublishEvent(ctx context.Context, key string, event interface{}) error
}

type IdempotencyChecker interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
}

type ShippingService struct {
	producer           KafkaProducer
	idempotencyChecker IdempotencyChecker
}

func NewShippingService(producer KafkaProducer, checker IdempotencyChecker) *ShippingService {
	return &ShippingService{
		producer:           producer,
		idempotencyChecker: checker,
	}
}

// ProcessPaymentCompleted handles payment.completed events
func (s *ShippingService) ProcessPaymentCompleted(ctx context.Context, eventData []byte) error {
	var event PaymentCompletedEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	log.Printf("📦 Processing payment.completed: order_id=%s\n", event.OrderID)

	// Check idempotency
	processed, err := s.idempotencyChecker.IsProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("failed to check idempotency: %w", err)
	}
	if processed {
		log.Printf("⚠️  Event already processed: %s\n", event.EventID)
		return nil
	}

	// Simulate shipping processing
	time.Sleep(300 * time.Millisecond)

	trackingNumber := fmt.Sprintf("TRACK-%s", uuid.New().String()[:8])

	// Publish order.shipped event
	shippedEvent := OrderShippedEvent{
		EventID:        uuid.New().String(),
		EventType:      "order.shipped",
		Timestamp:      time.Now(),
		OrderID:        event.OrderID,
		TrackingNumber: trackingNumber,
		Carrier:        "FastShip",
	}

	if err := s.producer.PublishEvent(ctx, event.OrderID, shippedEvent); err != nil {
		return fmt.Errorf("failed to publish shipped event: %w", err)
	}

	log.Printf("✓ Order shipped: order_id=%s, tracking=%s\n", event.OrderID, trackingNumber)

	// Mark as processed
	if err := s.idempotencyChecker.MarkProcessed(ctx, event.EventID); err != nil {
		log.Printf("⚠️  Failed to mark event as processed: %v\n", err)
	}

	return nil
}

// Event models
type PaymentCompletedEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	OrderID   string    `json:"order_id"`
	PaymentID string    `json:"payment_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
}

type OrderShippedEvent struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	Timestamp      time.Time `json:"timestamp"`
	OrderID        string    `json:"order_id"`
	TrackingNumber string    `json:"tracking_number"`
	Carrier        string    `json:"carrier"`
}

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type KafkaProducer interface {
	PublishEvent(ctx context.Context, key string, event any) error
}

type IdempotencyChecker interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
}

type PaymentService struct {
	producer           KafkaProducer
	idempotencyChecker IdempotencyChecker
}

func NewPaymentService(producer KafkaProducer, checker IdempotencyChecker) *PaymentService {
	return &PaymentService{
		producer:           producer,
		idempotencyChecker: checker,
	}
}

func (s *PaymentService) ProcessInventoryReserved(ctx context.Context, eventData []byte) error {
	var event InventoryReservedEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}
	log.Printf("Processing inventory.reserved: order_id=%s, amount=%f, currency=%s\n", event.OrderID, event.Amount, event.Currency)

	processed, err := s.idempotencyChecker.IsProcessed(ctx, event.OrderID)
	if err != nil {
		return fmt.Errorf("failed to check idempotency: %w", err)
	}
	if processed {
		log.Printf("Event already processed: %s\n", event.OrderID)
		return nil
	}

	time.Sleep(500 * time.Millisecond)
	paymentID := uuid.New().String()
	success := rand.Float32() > 0.1

	if success {
		successEvent := PaymentCompletedEvent{
			EventID:   uuid.New().String(),
			EventType: "payment.completed",
			Timestamp: time.Now(),
			OrderID:   event.OrderID,
			PaymentID: paymentID,
			Amount:    event.Amount,
			Currency:  event.Currency,
		}

		if err := s.producer.PublishEvent(ctx, event.OrderID, successEvent); err != nil {
			return fmt.Errorf("failed to publish success event: %w", err)
		}

		log.Printf("payment completed for order %s: payment_id: %s\n", event.OrderID, paymentID)
	} else {
		failureEvent := PaymentFailedEvent{
			EventID:   uuid.New().String(),
			EventType: "payment.failed",
			Timestamp: time.Now(),
			OrderID:   event.OrderID,
			Reason:    "Insufficient funds",
		}

		if err := s.producer.PublishEvent(ctx, event.OrderID, failureEvent); err != nil {
			return fmt.Errorf("failed to publish failure event: %w", err)
		}

		log.Printf("payment failed for order %s: insufficient funds\n", event.OrderID)
	}

	if err := s.idempotencyChecker.MarkProcessed(ctx, event.EventID); err != nil {
		log.Printf("Failed to mark event as processed: %v\n", err)
	}

	return nil
}

// Event models
type InventoryReservedEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	OrderID       string    `json:"order_id"`
	ReservationID string    `json:"reservation_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
}

type PaymentCompletedEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	OrderID   string    `json:"order_id"`
	PaymentID string    `json:"payment_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
}

type PaymentFailedEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	OrderID   string    `json:"order_id"`
	Reason    string    `json:"reason"`
}

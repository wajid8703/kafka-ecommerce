package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"inventory-service/internal/repository"
)

type KafkaProducer interface {
	PublishEvent(ctx context.Context, key string, event any) error
}

type IdempotencyChecker interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
}

type InventoryService struct {
	repo               *repository.InventoryRepository
	producer           KafkaProducer
	idempotencyChecker IdempotencyChecker
}

func NewInventoryService(repo *repository.InventoryRepository, producer KafkaProducer, checker IdempotencyChecker) *InventoryService {
	return &InventoryService{
		repo:               repo,
		producer:           producer,
		idempotencyChecker: checker,
	}
}

// ProcessOrderCreated handles order.created events
func (s *InventoryService) ProcessOrderCreated(ctx context.Context, eventData []byte) error {
	var event OrderCreatedEvent
	if err := json.Unmarshal(eventData, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	log.Printf("📦 Processing order.created: order_id=%s\n", event.OrderID)

	// Check idempotency (prevent duplicate processing)
	processed, err := s.idempotencyChecker.IsProcessed(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("failed to check idempotency: %w", err)
	}
	if processed {
		log.Printf("⚠️  Event already processed: %s\n", event.EventID)
		return nil
	}

	// Try to reserve stock for each item
	reservationID := uuid.New().String()
	for _, item := range event.Items {
		_, err := s.repo.ReserveStock(ctx, event.OrderID, item.ProductID, item.Quantity)
		if err != nil {
			// Reservation failed - publish failure event
			failureEvent := InventoryFailedEvent{
				EventID:   uuid.New().String(),
				EventType: "inventory.failed",
				Timestamp: time.Now(),
				OrderID:   event.OrderID,
				Reason:    err.Error(),
			}

			if err := s.producer.PublishEvent(ctx, event.OrderID, failureEvent); err != nil {
				return fmt.Errorf("failed to publish failure event: %w", err)
			}

			log.Printf("❌ Inventory reservation failed for order %s: %v\n", event.OrderID, err)

			// Mark as processed to avoid retries
			if err := s.idempotencyChecker.MarkProcessed(ctx, event.EventID); err != nil {
				log.Printf("⚠️  Failed to mark event as processed: %v\n", err)
			}
			return nil
		}
	}

	// All reservations successful - publish success event
	successEvent := InventoryReservedEvent{
		EventID:       uuid.New().String(),
		EventType:     "inventory.reserved",
		Timestamp:     time.Now(),
		OrderID:       event.OrderID,
		ReservationID: reservationID,
	}

	if err := s.producer.PublishEvent(ctx, event.OrderID, successEvent); err != nil {
		return fmt.Errorf("failed to publish success event: %w", err)
	}

	// Mark event as processed
	if err := s.idempotencyChecker.MarkProcessed(ctx, event.EventID); err != nil {
		log.Printf("⚠️  Failed to mark event as processed: %v\n", err)
	}

	log.Printf("✓ Inventory reserved for order %s\n", event.OrderID)
	return nil
}

// Event models
type OrderCreatedEvent struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	Timestamp   time.Time `json:"timestamp"`
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	Items       []Item    `json:"items"`
	TotalAmount float64   `json:"total_amount"`
	Currency    string    `json:"currency"`
}

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type InventoryReservedEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	OrderID       string    `json:"order_id"`
	ReservationID string    `json:"reservation_id"`
}

type InventoryFailedEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	OrderID   string    `json:"order_id"`
	Reason    string    `json:"reason"`
}

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"order-service/internal/models"
	"order-service/internal/repository"
)

// KafkaProducer interface for publishing events
type KafkaProducer interface {
	PublishEvent(ctx context.Context, key string, event interface{}) error
}

type OrderService struct {
	repo     *repository.OrderRepository
	producer KafkaProducer
}

func NewOrderService(repo *repository.OrderRepository, producer KafkaProducer) *OrderService {
	return &OrderService{
		repo:     repo,
		producer: producer,
	}
}

// CreateOrder creates a new order and publishes event
func (s *OrderService) CreateOrder(ctx context.Context, req *models.CreateOrderRequest) (*models.Order, error) {
	// Calculate total amount
	totalAmount := 0.0
	for _, item := range req.Items {
		totalAmount += item.Price * float64(item.Quantity)
	}

	// Create order
	order := &models.Order{
		ID:          uuid.New().String(),
		CustomerID:  req.CustomerID,
		Items:       req.Items,
		TotalAmount: totalAmount,
		Currency:    req.Currency,
		Status:      models.OrderStatusPending,
	}

	// Save to database
	if err := s.repo.Create(ctx, order); err != nil {
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	// Create event for event sourcing
	if err := s.repo.CreateEvent(ctx, order.ID, "order.created", order); err != nil {
		return nil, fmt.Errorf("failed to create event: %w", err)
	}

	// Publish to Kafka (async saga starts here)
	event := OrderCreatedEvent{
		EventID:     uuid.New().String(),
		EventType:   "order.created",
		Timestamp:   time.Now(),
		OrderID:     order.ID,
		CustomerID:  order.CustomerID,
		Items:       convertItems(order.Items),
		TotalAmount: order.TotalAmount,
		Currency:    order.Currency,
	}

	if err := s.producer.PublishEvent(ctx, order.ID, event); err != nil {
		return nil, fmt.Errorf("failed to publish event: %w", err)
	}

	return order, nil
}

// GetOrder retrieves an order by ID
func (s *OrderService) GetOrder(ctx context.Context, id string) (*models.Order, error) {
	return s.repo.FindByID(ctx, id)
}

// UpdateOrderStatus updates order status
func (s *OrderService) UpdateOrderStatus(ctx context.Context, id string, status models.OrderStatus) error {
	return s.repo.UpdateStatus(ctx, id, status)
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

func convertItems(items []models.OrderItem) []Item {
	result := make([]Item, len(items))
	for i, item := range items {
		result[i] = Item{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Price:     item.Price,
		}
	}
	return result
}

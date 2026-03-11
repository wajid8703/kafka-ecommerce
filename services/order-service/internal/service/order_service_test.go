package service

import (
	"context"
	"testing"

	"order-service/internal/models"
)

// Mock repository
type MockOrderRepository struct {
	createFunc func(ctx context.Context, order *models.Order) error
	findFunc   func(ctx context.Context, id string) (*models.Order, error)
}

func (m *MockOrderRepository) Create(ctx context.Context, order *models.Order) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, order)
	}
	return nil
}

func (m *MockOrderRepository) FindByID(ctx context.Context, id string) (*models.Order, error) {
	if m.findFunc != nil {
		return m.findFunc(ctx, id)
	}
	return &models.Order{ID: id}, nil
}

func (m *MockOrderRepository) UpdateStatus(ctx context.Context, id string, status models.OrderStatus) error {
	return nil
}

func (m *MockOrderRepository) CreateEvent(ctx context.Context, orderID, eventType string, eventData interface{}) error {
	return nil
}

// Mock Kafka producer
type MockKafkaProducer struct {
	publishFunc func(ctx context.Context, key string, event interface{}) error
}

func (m *MockKafkaProducer) PublishEvent(ctx context.Context, key string, event interface{}) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, key, event)
	}
	return nil
}

func TestCreateOrder(t *testing.T) {
	// Setup
	mockRepo := &MockOrderRepository{}
	mockProducer := &MockKafkaProducer{}
	service := NewOrderService(mockRepo, mockProducer)

	req := &models.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []models.OrderItem{
			{ProductID: "prod-001", Quantity: 2, Price: 29.99},
		},
		Currency: "USD",
	}

	// Execute
	order, err := service.CreateOrder(context.Background(), req)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if order.CustomerID != "cust-123" {
		t.Errorf("Expected customer_id 'cust-123', got: %s", order.CustomerID)
	}

	expectedTotal := 59.98
	if order.TotalAmount != expectedTotal {
		t.Errorf("Expected total %.2f, got: %.2f", expectedTotal, order.TotalAmount)
	}

	if order.Status != models.OrderStatusPending {
		t.Errorf("Expected status 'pending', got: %s", order.Status)
	}
}

func TestCreateOrderCalculatesTotalCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		items    []models.OrderItem
		expected float64
	}{
		{
			name: "Single item",
			items: []models.OrderItem{
				{ProductID: "prod-001", Quantity: 1, Price: 99.99},
			},
			expected: 99.99,
		},
		{
			name: "Multiple items",
			items: []models.OrderItem{
				{ProductID: "prod-001", Quantity: 2, Price: 50.00},
				{ProductID: "prod-002", Quantity: 1, Price: 25.00},
			},
			expected: 125.00,
		},
		{
			name: "Large quantity",
			items: []models.OrderItem{
				{ProductID: "prod-001", Quantity: 100, Price: 1.50},
			},
			expected: 150.00,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := &MockOrderRepository{}
			mockProducer := &MockKafkaProducer{}
			service := NewOrderService(mockRepo, mockProducer)

			req := &models.CreateOrderRequest{
				CustomerID: "cust-123",
				Items:      tt.items,
				Currency:   "USD",
			}

			order, err := service.CreateOrder(context.Background(), req)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if order.TotalAmount != tt.expected {
				t.Errorf("Expected total %.2f, got: %.2f", tt.expected, order.TotalAmount)
			}
		})
	}
}

func TestGetOrder(t *testing.T) {
	mockRepo := &MockOrderRepository{
		findFunc: func(ctx context.Context, id string) (*models.Order, error) {
			return &models.Order{
				ID:         id,
				CustomerID: "cust-123",
				Status:     models.OrderStatusPending,
			}, nil
		},
	}
	mockProducer := &MockKafkaProducer{}
	service := NewOrderService(mockRepo, mockProducer)

	order, err := service.GetOrder(context.Background(), "order-123")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if order.ID != "order-123" {
		t.Errorf("Expected order ID 'order-123', got: %s", order.ID)
	}
}

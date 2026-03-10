package models

import "time"

// Order represents an order in the system
type Order struct {
	ID          string      `json:"id"`
	CustomerID  string      `json:"customer_id"`
	Items       []OrderItem `json:"items"`
	TotalAmount float64     `json:"total_amount"`
	Currency    string      `json:"currency"`
	Status      OrderStatus `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// OrderItem represents a line item in an order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// OrderStatus represents the order lifecycle
type OrderStatus string

const (
	OrderStatusPending           OrderStatus = "pending"
	OrderStatusInventoryReserved OrderStatus = "inventory_reserved"
	OrderStatusPaymentCompleted  OrderStatus = "payment_completed"
	OrderStatusShipped           OrderStatus = "shipped"
	OrderStatusCompleted         OrderStatus = "completed"
	OrderStatusCancelled         OrderStatus = "cancelled"
	OrderStatusFailed            OrderStatus = "failed"
)

// CreateOrderRequest represents request to create an order
type CreateOrderRequest struct {
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	Currency   string      `json:"currency"`
}

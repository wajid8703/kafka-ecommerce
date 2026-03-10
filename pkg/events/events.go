package events

import (
	"time"
)

type EventType string

const (
	OrderCreated      EventType = "order.created"
	OrderShipped      EventType = "order.shipped"
	OrderCancelled    EventType = "order.cancelled"
	OrderCompleted    EventType = "order.completed"
	InventoryReserved EventType = "inventory.reserved"
	InventoryFailed   EventType = "inventory.failed"
	PaymentCompleted  EventType = "payment.completed"
)

type BaseEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
}

type OrderCreatedEvent struct {
	BaseEvent
	OrderID     string       `json:"order_id"`
	CustomerID  string       `json:"customer_id"`
	Items       []OrderItems `json:"items"`
	TotalAmount float64      `json:"total_amount"`
	Currency    string       `json:"currency"`
}

type OrderItems struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Price     int    `json:"price"`
}

type InventoryReservedEvent struct {
	BaseEvent
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
}

type InventoryFailedEvent struct {
	BaseEvent
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type PaymentCompletedEvent struct {
	BaseEvent
	OrderID   string  `json:"order_id"`
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type PaymentFailedEvent struct {
	BaseEvent
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type OrderShippedEvent struct {
	BaseEvent
	OrderID        string `json:"order_id"`
	TrackingNumber string `json:"tracking_number"`
	Carrier        string `json:"carrier"`
}

type OrderCancelledEvent struct {
	BaseEvent
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

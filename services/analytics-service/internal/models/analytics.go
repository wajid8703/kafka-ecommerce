package models

import "time"

// OrderMetrics represents aggregated order metrics
type OrderMetrics struct {
	TotalOrders       int64     `json:"total_orders"`
	TotalRevenue      float64   `json:"total_revenue"`
	AverageOrderValue float64   `json:"average_order_value"`
	SuccessfulOrders  int64     `json:"successful_orders"`
	FailedOrders      int64     `json:"failed_orders"`
	SuccessRate       float64   `json:"success_rate"`
	LastUpdated       time.Time `json:"last_updated"`
}

// ProductMetrics represents per-product metrics
type ProductMetrics struct {
	ProductID     string    `json:"product_id"`
	TotalQuantity int       `json:"total_quantity"`
	TotalRevenue  float64   `json:"total_revenue"`
	OrderCount    int64     `json:"order_count"`
	LastOrdered   time.Time `json:"last_ordered"`
}

// RealtimeEvent represents a real-time event for streaming
type RealtimeEvent struct {
	EventType string      `json:"event_type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

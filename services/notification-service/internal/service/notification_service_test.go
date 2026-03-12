package service

import (
	"context"
	"encoding/json"
	"testing"
)

func TestProcessEvent_OrderCreated(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type":  "order.created",
		"order_id":    "order-123",
		"customer_id": "cust-456",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_InventoryReserved(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type": "inventory.reserved",
		"order_id":   "order-123",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_InventoryFailed(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type": "inventory.failed",
		"order_id":   "order-123",
		"reason":     "out of stock",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_PaymentCompleted(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type": "payment.completed",
		"order_id":   "order-123",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_PaymentFailed(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type": "payment.failed",
		"order_id":   "order-123",
		"reason":     "insufficient funds",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_OrderShipped(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type":      "order.shipped",
		"order_id":        "order-123",
		"tracking_number": "TRACK-789",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestProcessEvent_UnknownEventType(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"event_type": "unknown.event",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err != nil {
		t.Fatalf("Expected no error for unknown event, got: %v", err)
	}
}

func TestProcessEvent_InvalidJSON(t *testing.T) {
	svc := NewNotificationService()

	if err := svc.ProcessEvent(context.Background(), []byte("not json")); err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestProcessEvent_MissingEventType(t *testing.T) {
	svc := NewNotificationService()

	event := map[string]interface{}{
		"order_id": "order-123",
	}
	data, _ := json.Marshal(event)

	if err := svc.ProcessEvent(context.Background(), data); err == nil {
		t.Fatal("Expected error for missing event_type, got nil")
	}
}

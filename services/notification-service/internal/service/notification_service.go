package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

type NotificationService struct {
	// In production: email service, SMS service, etc.
}

func NewNotificationService() *NotificationService {
	return &NotificationService{}
}

// ProcessEvent handles all events and sends appropriate notifications
func (s *NotificationService) ProcessEvent(ctx context.Context, eventData []byte) error {
	// Parse as generic event first
	var baseEvent map[string]interface{}
	if err := json.Unmarshal(eventData, &baseEvent); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	eventType, ok := baseEvent["event_type"].(string)
	if !ok {
		return fmt.Errorf("missing event_type field")
	}

	switch eventType {
	case "order.created":
		return s.handleOrderCreated(baseEvent)
	case "inventory.reserved":
		return s.handleInventoryReserved(baseEvent)
	case "inventory.failed":
		return s.handleInventoryFailed(baseEvent)
	case "payment.completed":
		return s.handlePaymentCompleted(baseEvent)
	case "payment.failed":
		return s.handlePaymentFailed(baseEvent)
	case "order.shipped":
		return s.handleOrderShipped(baseEvent)
	default:
		log.Printf("⚠️  Unknown event type: %s\n", eventType)
		return nil
	}
}

func (s *NotificationService) handleOrderCreated(event map[string]interface{}) error {
	orderID := event["order_id"].(string)
	customerID := event["customer_id"].(string)

	log.Printf("📧 Sending notification: Order Created\n")
	log.Printf("   To: Customer %s\n", customerID)
	log.Printf("   Subject: Order %s confirmed\n", orderID)
	log.Printf("   Message: Thank you for your order!\n")

	// In production: Send actual email/SMS
	// emailService.Send(customerEmail, subject, body)

	return nil
}

func (s *NotificationService) handleInventoryReserved(event map[string]interface{}) error {
	orderID := event["order_id"].(string)

	log.Printf("📧 Sending notification: Inventory Reserved\n")
	log.Printf("   Order: %s\n", orderID)
	log.Printf("   Message: Your items are reserved!\n")

	return nil
}

func (s *NotificationService) handleInventoryFailed(event map[string]interface{}) error {
	orderID := event["order_id"].(string)
	reason := event["reason"].(string)

	log.Printf("📧 Sending notification: Inventory Failed\n")
	log.Printf("   Order: %s\n", orderID)
	log.Printf("   Message: Sorry, item out of stock: %s\n", reason)

	return nil
}

func (s *NotificationService) handlePaymentCompleted(event map[string]interface{}) error {
	orderID := event["order_id"].(string)

	log.Printf("📧 Sending notification: Payment Completed\n")
	log.Printf("   Order: %s\n", orderID)
	log.Printf("   Message: Payment successful!\n")

	return nil
}

func (s *NotificationService) handlePaymentFailed(event map[string]interface{}) error {
	orderID := event["order_id"].(string)
	reason := event["reason"].(string)

	log.Printf("📧 Sending notification: Payment Failed\n")
	log.Printf("   Order: %s\n", orderID)
	log.Printf("   Message: Payment failed: %s\n", reason)

	return nil
}

func (s *NotificationService) handleOrderShipped(event map[string]interface{}) error {
	orderID := event["order_id"].(string)
	trackingNumber := event["tracking_number"].(string)

	log.Printf("📧 Sending notification: Order Shipped\n")
	log.Printf("   Order: %s\n", orderID)
	log.Printf("   Tracking: %s\n", trackingNumber)
	log.Printf("   Message: Your order is on the way!\n")

	return nil
}

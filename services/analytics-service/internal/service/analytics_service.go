package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"analytics-service/internal/models"
)

type AnalyticsService struct {
	mu              sync.RWMutex
	orderMetrics    models.OrderMetrics
	productMetrics  map[string]*models.ProductMetrics
	realtimeEvents  []models.RealtimeEvent
	maxRealtimeSize int
}

func NewAnalyticsService() *AnalyticsService {
	return &AnalyticsService{
		productMetrics:  make(map[string]*models.ProductMetrics),
		realtimeEvents:  make([]models.RealtimeEvent, 0),
		maxRealtimeSize: 100, // Keep last 100 events
	}
}

// ProcessEvent processes events and updates metrics
func (s *AnalyticsService) ProcessEvent(ctx context.Context, eventData []byte) error {
	var baseEvent map[string]interface{}
	if err := json.Unmarshal(eventData, &baseEvent); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	eventType, ok := baseEvent["event_type"].(string)
	if !ok {
		return fmt.Errorf("missing event_type field")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Add to realtime events
	s.addRealtimeEvent(eventType, baseEvent)

	switch eventType {
	case "order.created":
		s.processOrderCreated(baseEvent)
	case "payment.completed":
		s.processPaymentCompleted(baseEvent)
	case "payment.failed":
		s.processPaymentFailed(baseEvent)
	case "order.shipped":
		s.processOrderShipped(baseEvent)
	}

	s.updateMetrics()
	return nil
}

func (s *AnalyticsService) processOrderCreated(event map[string]interface{}) {
	s.orderMetrics.TotalOrders++

	// Extract items
	items, ok := event["items"].([]interface{})
	if !ok {
		return
	}

	for _, item := range items {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		productID, _ := itemMap["product_id"].(string)
		quantity, _ := itemMap["quantity"].(float64)

		// Update product metrics
		if _, exists := s.productMetrics[productID]; !exists {
			s.productMetrics[productID] = &models.ProductMetrics{
				ProductID: productID,
			}
		}

		pm := s.productMetrics[productID]
		pm.TotalQuantity += int(quantity)
		pm.OrderCount++
		pm.LastOrdered = time.Now()
	}

	log.Printf("📊 Order created processed: total_orders=%d\n", s.orderMetrics.TotalOrders)
}

func (s *AnalyticsService) processPaymentCompleted(event map[string]interface{}) {
	s.orderMetrics.SuccessfulOrders++

	amount, ok := event["amount"].(float64)
	if ok {
		s.orderMetrics.TotalRevenue += amount
	}

	log.Printf("📊 Payment completed: revenue=%.2f, success_rate=%.2f%%\n",
		s.orderMetrics.TotalRevenue, s.orderMetrics.SuccessRate)
}

func (s *AnalyticsService) processPaymentFailed(event map[string]interface{}) {
	s.orderMetrics.FailedOrders++

	log.Printf("📊 Payment failed: failed_orders=%d\n", s.orderMetrics.FailedOrders)
}

func (s *AnalyticsService) processOrderShipped(event map[string]interface{}) {
	log.Printf("📊 Order shipped event processed\n")
}

func (s *AnalyticsService) updateMetrics() {
	s.orderMetrics.LastUpdated = time.Now()

	if s.orderMetrics.SuccessfulOrders > 0 {
		s.orderMetrics.AverageOrderValue = s.orderMetrics.TotalRevenue / float64(s.orderMetrics.SuccessfulOrders)
	}

	totalProcessed := s.orderMetrics.SuccessfulOrders + s.orderMetrics.FailedOrders
	if totalProcessed > 0 {
		s.orderMetrics.SuccessRate = (float64(s.orderMetrics.SuccessfulOrders) / float64(totalProcessed)) * 100
	}
}

func (s *AnalyticsService) addRealtimeEvent(eventType string, data map[string]interface{}) {
	event := models.RealtimeEvent{
		EventType: eventType,
		Timestamp: time.Now(),
		Data:      data,
	}

	s.realtimeEvents = append(s.realtimeEvents, event)

	// Keep only last N events
	if len(s.realtimeEvents) > s.maxRealtimeSize {
		s.realtimeEvents = s.realtimeEvents[1:]
	}
}

// GetMetrics returns current metrics
func (s *AnalyticsService) GetMetrics() models.OrderMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.orderMetrics
}

// GetProductMetrics returns product metrics
func (s *AnalyticsService) GetProductMetrics() map[string]*models.ProductMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create copy
	metrics := make(map[string]*models.ProductMetrics)
	for k, v := range s.productMetrics {
		metrics[k] = v
	}

	return metrics
}

// GetRealtimeEvents returns recent events
func (s *AnalyticsService) GetRealtimeEvents() []models.RealtimeEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return copy
	events := make([]models.RealtimeEvent, len(s.realtimeEvents))
	copy(events, s.realtimeEvents)

	return events
}

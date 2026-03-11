package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	kafkaConsumer "notification-service/internal/kafka"
	"notification-service/internal/service"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	ctx := context.Background()

	// Initialize service
	notificationService := service.NewNotificationService()

	// Start multiple consumers (one per topic)
	topics := []string{
		"order.events",
		"inventory.events",
		"payment.events",
		"shipping.events",
	}

	for _, topic := range topics {
		consumer := kafkaConsumer.NewConsumer(
			[]string{kafkaBrokers},
			topic,
			"notification-service-group",
			notificationService.ProcessEvent,
			nil, // No DLQ for notification service
		)

		go func(t string) {
			log.Printf("🎧 Starting consumer for topic: %s\n", t)
			if err := consumer.Start(ctx); err != nil {
				log.Printf("Consumer error for %s: %v\n", t, err)
			}
		}(topic)
	}

	log.Println("✓ Notification Service started successfully")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Notification Service...")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

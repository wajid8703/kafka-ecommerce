package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"shipping-service/internal/idempotency"
	"shipping-service/internal/kafka"
	"shipping-service/internal/service"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	ctx := context.Background()

	producer := kafka.NewProducer([]string{kafkaBrokers}, "shipping.events")
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close Kafka producer: %v", err)
		}
	}()

	idempotencyChecker := idempotency.NewRedisChecker(redisAddr)
	shippingService := service.NewShippingService(producer, idempotencyChecker)

	consumer := kafka.NewConsumer(
		[]string{kafkaBrokers},
		"payment.events",
		"shipping-service-group",
		shippingService.ProcessPaymentCompleted,
	)

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
	}()

	log.Println("✓ Shipping Service started successfully")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Shipping Service...")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

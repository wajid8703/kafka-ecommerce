package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"payment-service/internal/idempotency"
	kafkaConsumer "payment-service/internal/kafka"
	kafkaProducer "payment-service/internal/kafka"
	"payment-service/internal/service"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	ctx := context.Background()

	// Initialize Kafka producer
	producer := kafkaProducer.NewProducer([]string{kafkaBrokers}, "payment.events")
	defer producer.Close()

	// Initialize Redis idempotency checker
	idempotencyChecker := idempotency.NewRedisChecker(redisAddr)

	// Initialize service
	paymentService := service.NewPaymentService(producer, idempotencyChecker)

	// Start Kafka consumer
	consumer := kafkaConsumer.NewConsumer(
		[]string{kafkaBrokers},
		"inventory.events",
		"payment-service-group",
		paymentService.ProcessInventoryReserved,
	)

	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
	}()

	log.Println("✓ Payment Service started successfully")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Payment Service...")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

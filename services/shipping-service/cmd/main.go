package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"shipping-service/internal/idempotency"
	kafkaConsumer "shipping-service/internal/kafka"
	kafkaProducer "shipping-service/internal/kafka"
	"shipping-service/internal/service"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	ctx := context.Background()

	producer := kafkaProducer.NewProducer([]string{kafkaBrokers}, "shipping.events")
	defer producer.Close()

	idempotencyChecker := idempotency.NewRedisChecker(redisAddr)
	shippingService := service.NewShippingService(producer, idempotencyChecker)

	consumer := kafkaConsumer.NewConsumer(
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

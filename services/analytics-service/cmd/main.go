package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"analytics-service/internal/handlers"
	kafkaConsumer "analytics-service/internal/kafka"
	"analytics-service/internal/service"
)

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	port := getEnv("PORT", "8090")

	ctx := context.Background()

	// Initialize service
	analyticsService := service.NewAnalyticsService()

	// Start Kafka consumers for all topics
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
			"analytics-service-group",
			analyticsService.ProcessEvent,
			nil,
		)

		go func(t string) {
			log.Printf("🎧 Starting consumer for topic: %s\n", t)
			if err := consumer.Start(ctx); err != nil {
				log.Printf("Consumer error for %s: %v\n", t, err)
			}
		}(topic)
	}

	// Setup HTTP server for metrics API
	analyticsHandler := handlers.NewAnalyticsHandler(analyticsService)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	r.Get("/api/metrics", analyticsHandler.GetMetrics)
	r.Get("/api/metrics/products", analyticsHandler.GetProductMetrics)
	r.Get("/api/events/realtime", analyticsHandler.GetRealtimeEvents)

	go func() {
		log.Printf("🚀 Analytics API starting on port %s\n", port)
		if err := http.ListenAndServe(":"+port, r); err != nil {
			log.Fatalf("Server error: %v\n", err)
		}
	}()

	log.Println("✓ Analytics Service started successfully")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Analytics Service...")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

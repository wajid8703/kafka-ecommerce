package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/idempotency"
	kafkaConsumer "inventory-service/internal/kafka"
	kafkaProducer "inventory-service/internal/kafka"
	"inventory-service/internal/repository"
	"inventory-service/internal/service"
)

func main() {
	dbURL := getEnv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/inventory_db?sslmode=disable")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	redisAddr := getEnv("REDIS_URL", "localhost:6379")

	ctx := context.Background()

	// Connect to database
	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		log.Fatalf("Unable to ping database: %v\n", err)
	}
	log.Println("✓ Database connection established")

	// Run migrations
	if err := runMigrations(ctx, dbPool); err != nil {
		log.Fatalf("Failed to run migrations: %v\n", err)
	}

	// Seed test products
	if err := seedProducts(ctx, dbPool); err != nil {
		log.Fatalf("Failed to seed products: %v\n", err)
	}

	// Initialize Kafka producer
	producer := kafkaProducer.NewProducer([]string{kafkaBrokers}, "inventory.events")
	defer producer.Close()

	// Initialize Redis idempotency checker
	idempotencyChecker := idempotency.NewRedisChecker(redisAddr)

	// Initialize service
	inventoryRepo := repository.NewInventoryRepository(dbPool)
	inventoryService := service.NewInventoryService(inventoryRepo, producer, idempotencyChecker)

	// Start Kafka consumer
	consumer := kafkaConsumer.NewConsumer(
		[]string{kafkaBrokers},
		"order.events",
		"inventory-service-group",
		inventoryService.ProcessOrderCreated,
	)

	// Start consuming in background
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
	}()

	log.Println("✓ Inventory Service started successfully")

	// Wait for interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Inventory Service...")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            sku VARCHAR(100) UNIQUE NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0,
            reserved INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
		`CREATE TABLE IF NOT EXISTS reservations (
            id VARCHAR(100) PRIMARY KEY,
            order_id VARCHAR(36) NOT NULL,
            product_id VARCHAR(36) NOT NULL,
            quantity INTEGER NOT NULL,
            status VARCHAR(50) NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
		`CREATE INDEX IF NOT EXISTS idx_reservations_order_id ON reservations(order_id)`,
	}

	for _, query := range queries {
		if _, err := pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	log.Println("✓ Database migrations completed")
	return nil
}

func seedProducts(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if products already exist
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		log.Println("✓ Products already seeded")
		return nil
	}

	products := []struct {
		id       string
		name     string
		sku      string
		quantity int
	}{
		{"prod-001", "Laptop", "LAPTOP-001", 50},
		{"prod-002", "Mouse", "MOUSE-001", 200},
		{"prod-003", "Keyboard", "KEYBOARD-001", 150},
		{"prod-004", "Monitor", "MONITOR-001", 75},
		{"prod-005", "Headphones", "HEADPHONE-001", 100},
	}

	for _, p := range products {
		_, err := pool.Exec(ctx, `
            INSERT INTO products (id, name, sku, quantity, reserved)
            VALUES ($1, $2, $3, $4, 0)
        `, p.id, p.name, p.sku, p.quantity)

		if err != nil {
			return fmt.Errorf("failed to seed product %s: %w", p.id, err)
		}
	}

	log.Println("✓ Products seeded successfully")
	return nil
}

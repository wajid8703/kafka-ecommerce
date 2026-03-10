package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"order-service/internal/models"
)

var (
	ErrOrderNotFound = errors.New("order not found")
)

type OrderRepository struct {
	db *pgxpool.Pool
}

func NewOrderRepository(db *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{db: db}
}

// Create inserts a new order
func (r *OrderRepository) Create(ctx context.Context, order *models.Order) error {
	// Serialize items to JSON
	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	query := `
        INSERT INTO orders (id, customer_id, items, total_amount, currency, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING created_at, updated_at
    `

	err = r.db.QueryRow(
		ctx,
		query,
		order.ID,
		order.CustomerID,
		itemsJSON,
		order.TotalAmount,
		order.Currency,
		order.Status,
	).Scan(&order.CreatedAt, &order.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create order: %w", err)
	}

	return nil
}

// FindByID retrieves an order by ID
func (r *OrderRepository) FindByID(ctx context.Context, id string) (*models.Order, error) {
	query := `
        SELECT id, customer_id, items, total_amount, currency, status, created_at, updated_at
        FROM orders
        WHERE id = $1
    `

	var itemsJSON []byte
	order := &models.Order{}

	err := r.db.QueryRow(ctx, query, id).Scan(
		&order.ID,
		&order.CustomerID,
		&itemsJSON,
		&order.TotalAmount,
		&order.Currency,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt,
	)

	if err != nil {
		return nil, ErrOrderNotFound
	}

	// Deserialize items
	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items: %w", err)
	}

	return order, nil
}

// UpdateStatus updates order status
func (r *OrderRepository) UpdateStatus(ctx context.Context, id string, status models.OrderStatus) error {
	query := `
        UPDATE orders
        SET status = $1, updated_at = $2
        WHERE id = $3
    `

	result, err := r.db.Exec(ctx, query, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return ErrOrderNotFound
	}

	return nil
}

// CreateEvent stores an event (Event Sourcing pattern)
func (r *OrderRepository) CreateEvent(ctx context.Context, orderID, eventType string, eventData interface{}) error {
	dataJSON, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	query := `
        INSERT INTO order_events (order_id, event_type, event_data)
        VALUES ($1, $2, $3)
    `

	_, err = r.db.Exec(ctx, query, orderID, eventType, dataJSON)
	if err != nil {
		return fmt.Errorf("failed to create event: %w", err)
	}

	return nil
}

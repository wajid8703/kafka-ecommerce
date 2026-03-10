package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"inventory-service/internal/models"
)

var (
	ErrProductNotFound   = errors.New("product not found")
	ErrInsufficientStock = errors.New("insufficient stock")
)

type InventoryRepository struct {
	db *pgxpool.Pool
}

func NewInventoryRepository(db *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{db: db}
}

// ReserveStock reserves inventory for an order
func (r *InventoryRepository) ReserveStock(ctx context.Context, orderID, productID string, quantity int) (*models.Reservation, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Check available stock
	var available int
	err = tx.QueryRow(ctx, `
        SELECT quantity - reserved FROM products WHERE id = $1 FOR UPDATE
    `, productID).Scan(&available)

	if err != nil {
		return nil, ErrProductNotFound
	}

	if available < quantity {
		return nil, ErrInsufficientStock
	}

	// Update reserved quantity
	_, err = tx.Exec(ctx, `
        UPDATE products SET reserved = reserved + $1 WHERE id = $2
    `, quantity, productID)

	if err != nil {
		return nil, err
	}

	// Create reservation
	reservation := &models.Reservation{
		ID:        fmt.Sprintf("res-%s-%s", orderID, productID),
		OrderID:   orderID,
		ProductID: productID,
		Quantity:  quantity,
		Status:    "reserved",
		ExpiredAt: time.Now().Add(15 * time.Minute), // 15 min expiry
		CreatedAt: time.Now(),
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO reservations (id, order_id, product_id, quantity, status, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6)
    `, reservation.ID, reservation.OrderID, reservation.ProductID,
		reservation.Quantity, reservation.Status, reservation.ExpiredAt)

	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return reservation, nil
}

// GetProduct retrieves product by ID
func (r *InventoryRepository) GetProduct(ctx context.Context, id string) (*models.Product, error) {
	var product models.Product
	err := r.db.QueryRow(ctx, `
        SELECT id, name, sku, quantity, reserved, (quantity - reserved) as available, updated_at
        FROM products WHERE id = $1
    `, id).Scan(&product.ID, &product.Name, &product.SKU, &product.Quantity, &product.Reserved, &product.Available, &product.UpdatedAt)

	if err != nil {
		return nil, ErrProductNotFound
	}

	return &product, nil
}

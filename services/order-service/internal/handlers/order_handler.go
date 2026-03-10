package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"order-service/internal/models"
	"order-service/internal/repository"
	"order-service/internal/service"
)

type OrderHandler struct {
	service *service.OrderService
}

func NewOrderHandler(service *service.OrderService) *OrderHandler {
	return &OrderHandler{service: service}
}

// CreateOrder creates a new order
func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req models.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	order, err := h.service.CreateOrder(r.Context(), &req)
	if err != nil {
		log.Printf("ERROR CreateOrder: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Failed to create order")
		return
	}

	respondWithJSON(w, http.StatusCreated, order)
}

// GetOrder retrieves an order by ID
func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	orderID := chi.URLParam(r, "id")

	order, err := h.service.GetOrder(r.Context(), orderID)
	if err != nil {
		if errors.Is(err, repository.ErrOrderNotFound) {
			respondWithError(w, http.StatusNotFound, "Order not found")
			return
		}
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve order")
		return
	}

	respondWithJSON(w, http.StatusOK, order)
}

// Helper functions
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

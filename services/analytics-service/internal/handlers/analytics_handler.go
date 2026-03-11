package handlers

import (
	"encoding/json"
	"net/http"

	"analytics-service/internal/service"
)

type AnalyticsHandler struct {
	service *service.AnalyticsService
}

func NewAnalyticsHandler(service *service.AnalyticsService) *AnalyticsHandler {
	return &AnalyticsHandler{service: service}
}

// GetMetrics returns overall order metrics
func (h *AnalyticsHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.service.GetMetrics()
	respondWithJSON(w, http.StatusOK, metrics)
}

// GetProductMetrics returns per-product metrics
func (h *AnalyticsHandler) GetProductMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.service.GetProductMetrics()
	respondWithJSON(w, http.StatusOK, metrics)
}

// GetRealtimeEvents returns recent events
func (h *AnalyticsHandler) GetRealtimeEvents(w http.ResponseWriter, r *http.Request) {
	events := h.service.GetRealtimeEvents()
	respondWithJSON(w, http.StatusOK, events)
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // CORS for dashboard
	w.WriteHeader(code)
	w.Write(response)
}

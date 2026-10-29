package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Pinger interface defines connectivity check for database/dependencies.
type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	db Pinger
}

func NewHealthHandler(db Pinger) *HealthHandler {
	return &HealthHandler{db: db}
}

// LivenessHandler responds to /healthz (and /livez).
// Returns 200 OK as long as the process is running, not deadlocked, and threads are responsive.
// It deliberately does NOT fail when external services or databases have temporary glitches (Dependency Decoupling).
func (h *HealthHandler) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "alive",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// ReadinessHandler responds to /readyz.
// Returns 200 OK when the process is fully ready to accept customer traffic (Postgres pool connected).
// If the database is unreachable, it returns 503 Service Unavailable so orchestrators do not route traffic to it.
func (h *HealthHandler) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if h.db != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := h.db.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":    "unready",
				"reason":    "database unreachable",
				"error":     err.Error(),
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ready",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

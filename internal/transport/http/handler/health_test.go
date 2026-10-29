package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthHandler_Liveness(t *testing.T) {
	// Even if DB ping fails, Liveness MUST return 200 OK (INV-04 / Dependency Decoupling)
	h := NewHealthHandler(&mockPinger{err: errors.New("db connection refused")})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	h.LivenessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on liveness even when db is down, got %d", rec.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["status"] != "alive" {
		t.Fatalf("expected status=alive, got %v", res["status"])
	}
}

func TestHealthHandler_Readiness_Healthy(t *testing.T) {
	h := NewHealthHandler(&mockPinger{err: nil})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on readiness when db is reachable, got %d", rec.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["status"] != "ready" {
		t.Fatalf("expected status=ready, got %v", res["status"])
	}
}

func TestHealthHandler_Readiness_Unhealthy(t *testing.T) {
	h := NewHealthHandler(&mockPinger{err: errors.New("timeout connecting to postgres")})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	h.ReadinessHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on readiness when db is down, got %d", rec.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if res["status"] != "unready" {
		t.Fatalf("expected status=unready, got %v", res["status"])
	}
}

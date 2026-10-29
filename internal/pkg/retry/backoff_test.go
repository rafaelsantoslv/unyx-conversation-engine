package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/pkg/retry"
)

type customTimeoutError struct{}

func (e *customTimeoutError) Error() string   { return "i/o timeout" }
func (e *customTimeoutError) Timeout() bool   { return true }
func (e *customTimeoutError) Temporary() bool { return true }

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, true},
		{"network timeout error", &customTimeoutError{}, true},
		{"http 503 service unavailable", errors.New("HTTP 503: upstream service overloaded"), true},
		{"http 429 rate limit exceeded", errors.New("rate limit reached: 429 too many requests"), true},
		{"http 502 bad gateway", errors.New("502 Bad Gateway from cloudflare"), true},
		{"connection reset by peer", errors.New("read tcp: connection reset by peer"), true},
		{"permanent 400 bad request", errors.New("HTTP 400: invalid prompt parameters"), false},
		{"permanent 401 unauthorized", errors.New("HTTP 401: invalid api key provided"), false},
		{"permanent 404 not found", errors.New("resource not found 404"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retry.IsRetryable(tt.err)
			if got != tt.expected {
				t.Errorf("IsRetryable(%v) = %v; want %v", tt.err, got, tt.expected)
			}
		})
	}
}

func TestCalculateBackoff_Jitter(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second

	t.Run("calculates backoff within jitter bounds for attempts", func(t *testing.T) {
		for attempt := 1; attempt <= 5; attempt++ {
			backoff := retry.CalculateBackoff(attempt, base, max)

			// O teto para attempt=1 é 100ms, attempt=2 é 200ms, attempt=3 é 400ms, etc.
			shift := attempt - 1
			ceiling := base * (1 << shift)
			if ceiling > max {
				ceiling = max
			}

			if backoff < 0 {
				t.Errorf("attempt %d: negative backoff %v", attempt, backoff)
			}
			if backoff > ceiling {
				t.Errorf("attempt %d: backoff %v exceeded ceiling %v", attempt, backoff, ceiling)
			}
		}
	})

	t.Run("disperses values with random jitter (no constant values)", func(t *testing.T) {
		seen := make(map[time.Duration]bool)
		distinctCount := 0
		for i := 0; i < 20; i++ {
			b := retry.CalculateBackoff(3, base, max)
			if !seen[b] {
				seen[b] = true
				distinctCount++
			}
		}

		// Com 20 execuções de nano-precisão, o jitter deve produzir valores quase todos distintos
		if distinctCount < 15 {
			t.Errorf("expected high dispersion from jitter, got only %d distinct values out of 20", distinctCount)
		}
	})
}

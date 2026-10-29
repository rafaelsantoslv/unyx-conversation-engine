package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	runtimeHttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHttpAgentRuntime_IntegrationWithServer(t *testing.T) {
	t.Run("Successfully executes and parses agent response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/v1/agents/execute", r.URL.Path)
			assert.Equal(t, "tenant-xyz", r.Header.Get("X-Tenant-ID"))
			assert.Equal(t, "conv-123", r.Header.Get("X-Conversation-ID"))
			assert.Equal(t, "req-789", r.Header.Get("X-Correlation-ID"))

			var in domain.ExecutionInput
			err := json.NewDecoder(r.Body).Decode(&in)
			require.NoError(t, err)
			assert.Equal(t, "Olá agente", in.Content)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(domain.ExecutionOutput{
				ResponseContent: "Olá! Como posso ajudar você hoje?",
				TokensUsed:      35,
				ExecutedAt:      time.Now().UTC(),
			})
		}))
		defer server.Close()

		rt := runtimeHttp.NewHttpAgentRuntime(server.URL)
		input := domain.ExecutionInput{
			ExecutionRequestID: "req-789",
			TenantID:           "tenant-xyz",
			ConversationID:     "conv-123",
			MessageID:          "msg-456",
			Content:            "Olá agente",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		out, err := rt.Execute(ctx, input)
		require.NoError(t, err)
		require.NotNil(t, out)
		assert.Equal(t, "Olá! Como posso ajudar você hoje?", out.ResponseContent)
		assert.Equal(t, 35, out.TokensUsed)
	})

	t.Run("Returns transient error for 503 Service Unavailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error": "model overloaded"}`))
		}))
		defer server.Close()

		rt := runtimeHttp.NewHttpAgentRuntime(server.URL)
		_, err := rt.Execute(context.Background(), domain.ExecutionInput{TenantID: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "temporary failure: http status 503")
	})

	t.Run("Returns permanent error for 400 Bad Request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": "unsupported prompt format"}`))
		}))
		defer server.Close()

		rt := runtimeHttp.NewHttpAgentRuntime(server.URL)
		_, err := rt.Execute(context.Background(), domain.ExecutionInput{TenantID: "t"})
		require.Error(t, err)
		assert.True(t, errors.Is(err, runtimeHttp.ErrRuntimePermanent))
	})

	t.Run("Handles context timeout cleanly", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		rt := runtimeHttp.NewHttpAgentRuntime(server.URL)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err := rt.Execute(ctx, domain.ExecutionInput{TenantID: "t"})
		require.Error(t, err)
		assert.True(t, errors.Is(err, runtimeHttp.ErrRuntimeTimeout) || errors.Is(err, context.DeadlineExceeded) || stringsContains(err.Error(), "timeout") || stringsContains(err.Error(), "context deadline exceeded"))
	})
}

func stringsContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > 0 && len(substr) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

package mock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
)

func TestMockRuntime_Execute(t *testing.T) {
	input := domain.ExecutionInput{
		TenantID:       "tenant-1",
		ConversationID: "conv-1",
		MessageID:      "msg-1",
		Content:        "Qual o status do meu pedido?",
	}

	t.Run("successfully executes with custom reply and default latency", func(t *testing.T) {
		customReply := "Seu pedido está a caminho!"
		runtime := mock.NewMockRuntime(mock.WithSimulatedReply(customReply))

		out, err := runtime.Execute(context.Background(), input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out == nil {
			t.Fatalf("expected non-nil output")
		}
		if out.ResponseContent != customReply {
			t.Errorf("expected %q, got %q", customReply, out.ResponseContent)
		}
		if out.TokensUsed <= 0 {
			t.Errorf("expected positive tokens used, got %d", out.TokensUsed)
		}
	})

	t.Run("fails deterministically when ShouldFail is true", func(t *testing.T) {
		expectedErr := errors.New("upstream service unavailable 503")
		runtime := mock.NewMockRuntime(mock.WithFailure(expectedErr))

		out, err := runtime.Execute(context.Background(), input)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
		if out != nil {
			t.Errorf("expected nil output on failure, got %v", out)
		}
	})

	t.Run("aborts immediately when context deadline is exceeded during latency", func(t *testing.T) {
		// Mock configurado para 200ms de latência
		runtime := mock.NewMockRuntime(mock.WithLatency(200 * time.Millisecond))

		// Context com timeout curto de 30ms
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		start := time.Now()
		out, err := runtime.Execute(ctx, input)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatalf("expected context error, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
		if out != nil {
			t.Errorf("expected nil output on timeout, got %v", out)
		}
		if elapsed >= 180*time.Millisecond {
			t.Errorf("expected cooperative abort in ~30ms, but elapsed %v", elapsed)
		}
	})

	t.Run("aborts when context is already cancelled before execution", func(t *testing.T) {
		runtime := mock.NewMockRuntime()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancelado imediatamente

		out, err := runtime.Execute(ctx, input)
		if err == nil {
			t.Fatalf("expected context error, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
		if out != nil {
			t.Errorf("expected nil output on canceled context, got %v", out)
		}
	})
}

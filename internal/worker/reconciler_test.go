package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
)

type mockReconcilerCoordinator struct {
	mu         sync.Mutex
	reconciled []postgres.ReconciledExecution
	err        error
	calls      int
}

func (m *mockReconcilerCoordinator) ReconcileOrphanExecutions(ctx context.Context, batchSize int) ([]postgres.ReconciledExecution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	return m.reconciled, nil
}

type mockReconcilerQuarantine struct {
	mu          sync.Mutex
	quarantined []domain.QuarantinedEvent
}

func (m *mockReconcilerQuarantine) QuarantineEvent(ctx context.Context, q domain.QuarantinedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quarantined = append(m.quarantined, q)
	return nil
}

func TestExecutionReconciler_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully recovers orphan execution and increments attempt", func(t *testing.T) {
		coord := &mockReconcilerCoordinator{
			reconciled: []postgres.ReconciledExecution{
				{
					ID:             "exec-orphan-1",
					TenantID:       "tenant-1",
					ConversationID: "conv-1",
					OldWorkerID:    "worker-crashed",
					FencingToken:   2,
					AttemptCount:   2,
					MaxAttempts:    5,
					IsQuarantined:  false,
				},
			},
		}
		quar := &mockReconcilerQuarantine{}

		reconciler := worker.NewExecutionReconciler(coord, quar, worker.ExecutionReconcilerConfig{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    10,
		})

		count, err := reconciler.ReconcileOnce(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 execution reconciled, got %d", count)
		}
		if len(quar.quarantined) != 0 {
			t.Errorf("expected 0 quarantined events, got %d", len(quar.quarantined))
		}
	})

	t.Run("sends to quarantine when max attempts are exceeded", func(t *testing.T) {
		coord := &mockReconcilerCoordinator{
			reconciled: []postgres.ReconciledExecution{
				{
					ID:             "exec-orphan-dead",
					TenantID:       "tenant-dead",
					ConversationID: "conv-dead",
					OldWorkerID:    "worker-dead",
					FencingToken:   6,
					AttemptCount:   5,
					MaxAttempts:    5,
					IsQuarantined:  true,
				},
			},
		}
		quar := &mockReconcilerQuarantine{}

		reconciler := worker.NewExecutionReconciler(coord, quar, worker.ExecutionReconcilerConfig{})

		count, err := reconciler.ReconcileOnce(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 execution reconciled, got %d", count)
		}
		if len(quar.quarantined) != 1 {
			t.Fatalf("expected 1 quarantined event, got %d", len(quar.quarantined))
		}
		if quar.quarantined[0].TenantID != "tenant-dead" {
			t.Errorf("expected tenant-dead, got %s", quar.quarantined[0].TenantID)
		}
	})

	t.Run("handles coordinator query error gracefully", func(t *testing.T) {
		coord := &mockReconcilerCoordinator{
			err: errors.New("db connection failure"),
		}
		quar := &mockReconcilerQuarantine{}

		reconciler := worker.NewExecutionReconciler(coord, quar, worker.ExecutionReconcilerConfig{})

		_, err := reconciler.ReconcileOnce(ctx)
		if err == nil {
			t.Fatalf("expected error from ReconcileOnce")
		}
	})

	t.Run("background start and cancellation", func(t *testing.T) {
		coord := &mockReconcilerCoordinator{}
		quar := &mockReconcilerQuarantine{}

		reconciler := worker.NewExecutionReconciler(coord, quar, worker.ExecutionReconcilerConfig{
			PollInterval: 10 * time.Millisecond,
		})

		loopCtx, cancel := context.WithCancel(context.Background())
		doneCh := make(chan error, 1)

		go func() {
			doneCh <- reconciler.Start(loopCtx)
		}()

		time.Sleep(30 * time.Millisecond)
		cancel()

		select {
		case err := <-doneCh:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled, got %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("reconciler failed to shutdown within timeout")
		}
	})
}

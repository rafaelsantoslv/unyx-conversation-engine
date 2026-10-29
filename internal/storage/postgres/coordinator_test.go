package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

func TestExecutionCoordinator_Integration(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping test: database connection failed: %v", err)
		return
	}
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)

	tenantID := fmt.Sprintf("tenant-coord-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-coord-1")
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	t.Run("worker 1 acquires execution and worker 2 gets ErrConversationBusy", func(t *testing.T) {
		worker1 := "worker-instance-1"
		worker2 := "worker-instance-2"

		// Worker 1 adquire com lease de 10s
		exec1, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, worker1, 10*time.Second)
		if err != nil {
			t.Fatalf("worker 1 failed to acquire execution: %v", err)
		}
		if exec1 == nil || exec1.Status != domain.ExecutionStatusRunning {
			t.Fatalf("expected status RUNNING for worker 1, got %+v", exec1)
		}

		// Worker 2 tenta adquirir a mesma conversa e é rejeitado com ErrConversationBusy
		exec2, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, worker2, 10*time.Second)
		if err == nil {
			t.Fatalf("expected ErrConversationBusy for worker 2, got success: %+v", exec2)
		}
		if err != domain.ErrConversationBusy {
			t.Fatalf("expected ErrConversationBusy, got: %v", err)
		}

		// Worker 1 libera a execução
		err = coordinator.ReleaseExecution(ctx, exec1.ID, domain.ExecutionStatusSucceeded)
		if err != nil {
			t.Fatalf("failed to release execution: %v", err)
		}

		// Agora Worker 2 consegue adquirir
		exec2After, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, worker2, 10*time.Second)
		if err != nil {
			t.Fatalf("worker 2 should acquire after release, got: %v", err)
		}
		if exec2After.WorkerID != worker2 {
			t.Errorf("expected worker_id %s, got %s", worker2, exec2After.WorkerID)
		}

		_ = coordinator.ReleaseExecution(ctx, exec2After.ID, domain.ExecutionStatusSucceeded)
	})

	t.Run("parallel execution between different tenants and conversations (INV-08)", func(t *testing.T) {
		tenantA := fmt.Sprintf("tenant-A-%d", time.Now().UnixNano())
		tenantB := fmt.Sprintf("tenant-B-%d", time.Now().UnixNano())

		convA, _ := repo.GetOrCreateConversation(ctx, tenantA, "conv-A")
		convB, _ := repo.GetOrCreateConversation(ctx, tenantB, "conv-B")

		// Ambos adquirem simultaneamente sem contenção
		execA, errA := coordinator.AcquireExecution(ctx, tenantA, convA.ID, "worker-A", 10*time.Second)
		execB, errB := coordinator.AcquireExecution(ctx, tenantB, convB.ID, "worker-B", 10*time.Second)

		if errA != nil || errB != nil {
			t.Fatalf("both distinct conversations should acquire concurrently without blocking: errA=%v, errB=%v", errA, errB)
		}

		if execA == nil || execB == nil {
			t.Fatalf("both executions should be non-nil")
		}

		_ = coordinator.ReleaseExecution(ctx, execA.ID, domain.ExecutionStatusSucceeded)
		_ = coordinator.ReleaseExecution(ctx, execB.ID, domain.ExecutionStatusSucceeded)
	})

	t.Run("concurrency stress: 10 workers competing for same conversation", func(t *testing.T) {
		stressConv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-stress")
		numWorkers := 10
		var wg sync.WaitGroup
		acquiredCount := 0
		busyCount := 0
		var mu sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			wID := fmt.Sprintf("worker-%d", i)
			go func(workerName string) {
				defer wg.Done()
				exec, err := coordinator.AcquireExecution(ctx, tenantID, stressConv.ID, workerName, 10*time.Second)
				mu.Lock()
				defer mu.Unlock()
				if err == nil && exec != nil {
					acquiredCount++
				} else if err == domain.ErrConversationBusy {
					busyCount++
				}
			}(wID)
		}

		wg.Wait()

		if acquiredCount != 1 {
			t.Fatalf("expected exactly 1 worker to acquire execution, got %d", acquiredCount)
		}
		if busyCount != numWorkers-1 {
			t.Fatalf("expected %d workers to receive ErrConversationBusy, got %d", numWorkers-1, busyCount)
		}
	})
}

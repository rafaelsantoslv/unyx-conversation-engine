package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

func TestResultsRepository_Integration(t *testing.T) {
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

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping test: database ping failed: %v", err)
		return
	}

	repo := postgres.NewRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)

	tenantID := fmt.Sprintf("tenant-res-%d", time.Now().UnixNano())
	extConvID := fmt.Sprintf("conv-res-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, extConvID)
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	msgID := uuid.New().String()
	msg := &domain.Message{
		ID:                msgID,
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: fmt.Sprintf("ext-msg-%d", time.Now().UnixNano()),
		Role:              domain.RoleUser,
		Content:           "Teste de resultado",
	}
	_, err = repo.SaveMessage(ctx, msg)
	if err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}

	t.Run("records execution start, success and verifies idempotency check", func(t *testing.T) {
		// 1. Deve retornar nil antes de iniciar
		initial, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg.ID)
		if err != nil {
			t.Fatalf("unexpected error getting initial result: %v", err)
		}
		if initial != nil {
			t.Errorf("expected nil initial result, got %+v", initial)
		}

		// 2. Registra início como RUNNING via AcquireExecutionLock
		startRes, err := resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, msg.ID, 30*time.Second)
		if err != nil {
			t.Fatalf("failed to record execution start: %v", err)
		}
		if startRes == nil || startRes.Status != domain.ExecutionStatusRunning {
			t.Fatalf("expected status RUNNING, got %+v", startRes)
		}

		// 3. Registra sucesso
		output := map[string]interface{}{
			"reply":       "Resposta de teste",
			"tokens_used": 100,
		}
		execDuration := 250 * time.Millisecond
		err = resultsRepo.RecordExecutionSuccess(ctx, tenantID, conv.ID, msg.ID, output, execDuration)
		if err != nil {
			t.Fatalf("failed to record execution success: %v", err)
		}

		// 4. Busca resultado e valida campos
		finalRes, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg.ID)
		if err != nil {
			t.Fatalf("failed to get final result: %v", err)
		}
		if finalRes == nil {
			t.Fatalf("expected non-nil result")
		}
		if finalRes.Status != domain.ExecutionStatusSucceeded {
			t.Errorf("expected status SUCCEEDED, got %s", finalRes.Status)
		}
		if finalRes.ExecutionTimeMS != 250 {
			t.Errorf("expected execution time 250ms, got %d", finalRes.ExecutionTimeMS)
		}
		if finalRes.CompletedAt == nil {
			t.Errorf("expected completed_at to be non-nil")
		}

		// 5. Tentativa subsequente de lock para evento já SUCCEEDED deve retornar nil (idempotência)
		subsequentLock, err := resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, msg.ID, 30*time.Second)
		if err != nil {
			t.Fatalf("unexpected error on subsequent lock: %v", err)
		}
		if subsequentLock != nil {
			t.Errorf("expected nil lock for already SUCCEEDED message, got %+v", subsequentLock)
		}
	})

	t.Run("records execution failure properly", func(t *testing.T) {
		failMsgID := uuid.New().String()
		failMsg := &domain.Message{
			ID:                failMsgID,
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-fail-%d", time.Now().UnixNano()),
			Role:              domain.RoleUser,
			Content:           "Mensagem com falha",
		}
		_, err := repo.SaveMessage(ctx, failMsg)
		if err != nil {
			t.Fatalf("failed to insert fail message: %v", err)
		}

		// Registra início
		_, err = resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, failMsg.ID, 30*time.Second)
		if err != nil {
			t.Fatalf("failed to record execution start: %v", err)
		}

		// Registra falha
		testErr := errors.New("upstream timeout from AI")
		err = resultsRepo.RecordExecutionFailure(ctx, tenantID, conv.ID, failMsg.ID, testErr, 1200*time.Millisecond)
		if err != nil {
			t.Fatalf("failed to record execution failure: %v", err)
		}

		failRes, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, failMsg.ID)
		if err != nil {
			t.Fatalf("failed to get fail result: %v", err)
		}
		if failRes.Status != domain.ExecutionStatusFailed {
			t.Errorf("expected status FAILED, got %s", failRes.Status)
		}
		if failRes.ErrorMessage == nil || *failRes.ErrorMessage != testErr.Error() {
			t.Errorf("expected error message %q, got %v", testErr.Error(), failRes.ErrorMessage)
		}
		if failRes.ExecutionTimeMS != 1200 {
			t.Errorf("expected execution time 1200ms, got %d", failRes.ExecutionTimeMS)
		}
	})

	t.Run("concurrency test: multiple goroutines competing for execution lock", func(t *testing.T) {
		concurMsgID := uuid.New().String()
		concurMsg := &domain.Message{
			ID:                concurMsgID,
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-concur-%d", time.Now().UnixNano()),
			Role:              domain.RoleUser,
			Content:           "Mensagem concorrente",
		}
		_, err := repo.SaveMessage(ctx, concurMsg)
		if err != nil {
			t.Fatalf("failed to insert concur message: %v", err)
		}

		workersCount := 10
		var wg sync.WaitGroup
		acquiredCount := 0
		var mu sync.Mutex

		for i := 0; i < workersCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, lockErr := resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, concurMsgID, 10*time.Second)
				if lockErr == nil && res != nil {
					mu.Lock()
					acquiredCount++
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		// Exatamente 1 worker deve ter conseguido o lock inicial
		if acquiredCount != 1 {
			t.Fatalf("expected exactly 1 worker to acquire lock, got %d", acquiredCount)
		}
	})
}

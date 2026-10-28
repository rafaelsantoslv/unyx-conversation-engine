package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping postgres integration test: failed to connect to %s: %v", dbURL, err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres integration test: database ping failed: %v", err)
	}

	migrationsPath := filepath.Join("..", "..", "..", "migrations")
	if err := postgres.RunMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	return pool
}

func TestRepository_GetOrCreateConversation(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	ctx := context.Background()

	tenantID := fmt.Sprintf("tenant-%d", time.Now().UnixNano())
	externalID := "ext-conv-1"

	t.Run("creates new conversation if not exists", func(t *testing.T) {
		conv, err := repo.GetOrCreateConversation(ctx, tenantID, externalID)
		if err != nil {
			t.Fatalf("unexpected error creating conversation: %v", err)
		}
		if conv.ID == "" {
			t.Errorf("expected non-empty conversation ID")
		}
		if conv.TenantID != tenantID || conv.ExternalConversationID != externalID {
			t.Errorf("unexpected conv tenant or external id: %+v", conv)
		}
		if conv.State != domain.StateAIActive {
			t.Errorf("expected status AI_ACTIVE, got %s", conv.State)
		}
		if conv.Version != 1 {
			t.Errorf("expected version 1, got %d", conv.Version)
		}
	})

	t.Run("returns existing conversation on repeated calls", func(t *testing.T) {
		conv1, err := repo.GetOrCreateConversation(ctx, tenantID, externalID)
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}

		conv2, err := repo.GetOrCreateConversation(ctx, tenantID, externalID)
		if err != nil {
			t.Fatalf("second call failed: %v", err)
		}

		if conv1.ID != conv2.ID {
			t.Errorf("expected same ID, got %s and %s", conv1.ID, conv2.ID)
		}
	})

	t.Run("concurrency test: multiple goroutines getting or creating same conversation", func(t *testing.T) {
		concurrencyTenant := fmt.Sprintf("tenant-concurrent-%d", time.Now().UnixNano())
		targetExternalID := "concurrent-ext-id"
		goroutines := 10

		var wg sync.WaitGroup
		results := make([]*domain.Conversation, goroutines)
		errorsList := make([]error, goroutines)

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				c, err := repo.GetOrCreateConversation(ctx, concurrencyTenant, targetExternalID)
				results[idx] = c
				errorsList[idx] = err
			}(i)
		}

		wg.Wait()

		for i, err := range errorsList {
			if err != nil {
				t.Fatalf("goroutine %d failed with error: %v", i, err)
			}
		}

		firstID := results[0].ID
		for i := 1; i < goroutines; i++ {
			if results[i].ID != firstID {
				t.Errorf("goroutine %d got different conversation ID: %s vs %s", i, results[i].ID, firstID)
			}
		}
	})
}

func TestRepository_SaveMessage_And_Deduplication(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	ctx := context.Background()

	tenantID := fmt.Sprintf("tenant-msg-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-ext-100")
	if err != nil {
		t.Fatalf("failed to prepare conversation: %v", err)
	}

	t.Run("saves first message with sequence_number 1", func(t *testing.T) {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-ext-1",
			Role:              domain.RoleUser,
			Content:           "Primeira mensagem",
		}

		saved, err := repo.SaveMessage(ctx, msg)
		if err != nil {
			t.Fatalf("failed to save message: %v", err)
		}

		if saved.SequenceNumber != 1 {
			t.Errorf("expected sequence_number 1, got %d", saved.SequenceNumber)
		}
		if saved.ID == "" {
			t.Errorf("expected non-empty message ID")
		}
	})

	t.Run("saves second message with sequence_number 2", func(t *testing.T) {
		msg2 := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-ext-2",
			Role:              domain.RoleAgent,
			Content:           "Segunda mensagem",
		}

		saved, err := repo.SaveMessage(ctx, msg2)
		if err != nil {
			t.Fatalf("failed to save second message: %v", err)
		}

		if saved.SequenceNumber != 2 {
			t.Errorf("expected sequence_number 2, got %d", saved.SequenceNumber)
		}
	})

	t.Run("idempotency: inserting duplicate message returns ErrDuplicateMessage without panic", func(t *testing.T) {
		dupMsg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-ext-1",
			Role:              domain.RoleUser,
			Content:           "Tentativa de duplicata",
		}

		existing, err := repo.SaveMessage(ctx, dupMsg)
		if !errors.Is(err, postgres.ErrDuplicateMessage) {
			t.Fatalf("expected ErrDuplicateMessage, got %v", err)
		}

		if existing == nil {
			t.Fatalf("expected existing message returned on duplicate")
		}
		if existing.ExternalMessageID != "msg-ext-1" {
			t.Errorf("expected existing external message id 'msg-ext-1', got %s", existing.ExternalMessageID)
		}
		if existing.Content != "Primeira mensagem" {
			t.Errorf("expected original content preserved, got %s", existing.Content)
		}
	})
}

func TestRepository_TransactionalOutbox(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	ctx := context.Background()

	tenantID := fmt.Sprintf("tenant-outbox-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-ext-outbox-1")
	if err != nil {
		t.Fatalf("failed to prepare conversation: %v", err)
	}

	t.Run("commits message and outbox event atomically", func(t *testing.T) {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-atomic-1",
			Role:              domain.RoleUser,
			Content:           "Mensagem atomica com outbox",
		}

		payloadBytes, _ := json.Marshal(map[string]string{"message": msg.Content})
		evt := &domain.OutboxEvent{
			EventType:    domain.EventTypeMessageReceived,
			Payload:      payloadBytes,
			PartitionKey: conv.ID,
			Status:       domain.OutboxStatusPending,
		}

		savedMsg, err := repo.SaveMessageWithOutbox(ctx, msg, evt)
		if err != nil {
			t.Fatalf("failed to save message with outbox: %v", err)
		}

		if savedMsg.ID == "" || evt.ID == "" {
			t.Errorf("expected non-empty IDs for msg and outbox event")
		}

		// Valida busca com FOR UPDATE SKIP LOCKED
		events, err := repo.FetchPendingOutboxEvents(ctx, 10)
		if err != nil {
			t.Fatalf("failed to fetch pending outbox events: %v", err)
		}

		var found bool
		for _, e := range events {
			if e.ID == evt.ID {
				found = true
				if e.Status != domain.OutboxStatusPending {
					t.Errorf("expected status PENDING, got %s", e.Status)
				}
				if e.PartitionKey != conv.ID {
					t.Errorf("expected partition key %s, got %s", conv.ID, e.PartitionKey)
				}
				break
			}
		}

		if !found {
			t.Errorf("expected outbox event %s to be in pending list", evt.ID)
		}

		// Marca como publicado e valida que não vem mais como pendente
		if err := repo.MarkOutboxEventPublished(ctx, evt.ID); err != nil {
			t.Fatalf("failed to mark outbox event published: %v", err)
		}

		pendingAfter, err := repo.FetchPendingOutboxEvents(ctx, 10)
		if err != nil {
			t.Fatalf("failed to fetch pending after publish: %v", err)
		}
		for _, e := range pendingAfter {
			if e.ID == evt.ID {
				t.Errorf("outbox event %s should not appear as pending after being marked published", evt.ID)
			}
		}
	})

	t.Run("rollbacks both message and outbox event on failure", func(t *testing.T) {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-rollback-test",
			Role:              domain.RoleUser,
			Content:           "Mensagem de teste de rollback",
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to start manual transaction: %v", err)
		}
		defer func() {
			_ = tx.Rollback(ctx)
		}()

		if err := repo.InsertMessage(ctx, tx, msg); err != nil {
			t.Fatalf("failed to insert message in tx: %v", err)
		}

		// Força erro de banco no outbox (ID duplicado ou violação proposital)
		invalidEvt := &domain.OutboxEvent{
			ID:             "invalid-not-uuid", // falhará em UUID no postgres se for formato inválido
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Payload:        []byte(`{}`),
			PartitionKey:   conv.ID,
		}

		err = repo.InsertOutboxEvent(ctx, tx, invalidEvt)
		if err == nil {
			t.Fatalf("expected error inserting outbox with invalid UUID")
		}

		// Desfaz transação
		_ = tx.Rollback(ctx)

		// Verifica que a mensagem NÃO foi gravada no banco
		_, checkErr := repo.GetMessageByExternalID(ctx, tenantID, msg.ExternalMessageID)
		if !errors.Is(checkErr, pgx.ErrNoRows) {
			t.Fatalf("expected pgx.ErrNoRows for message after rollback, got %v", checkErr)
		}
	})
}

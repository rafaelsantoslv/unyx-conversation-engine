package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
)

func setupDebounceTestPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	return pool
}

func TestDebounceRepository_Integration(t *testing.T) {
	pool := setupDebounceTestPostgres(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repo := postgres.NewRepository(pool)
	debounceRepo := postgres.NewDebounceRepository(pool)

	tenantID := fmt.Sprintf("tenant-deb-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-deb-test")
	if err != nil {
		t.Fatalf("failed to create conv: %v", err)
	}

	cfg := domain.DebounceConfig{
		InactivityWindow: 200 * time.Millisecond,
		MaxWaitTimeout:   500 * time.Millisecond,
	}

	// 1. Mensagem 1 -> Cria janela com message_count = 1
	w1, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, cfg)
	if err != nil {
		t.Fatalf("first message failed to create window: %v", err)
	}
	if w1.MessageCount != 1 {
		t.Errorf("expected message_count 1, got %d", w1.MessageCount)
	}
	if w1.Status != domain.DebounceStatusOpen {
		t.Errorf("expected status OPEN, got %s", w1.Status)
	}

	time.Sleep(50 * time.Millisecond)

	// 2. Mensagem 2 (chega dentro da janela) -> Estende debounce_until e incrementa message_count = 2
	w2, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, cfg)
	if err != nil {
		t.Fatalf("second message failed to extend window: %v", err)
	}
	if w2.MessageCount != 2 {
		t.Errorf("expected message_count 2, got %d", w2.MessageCount)
	}
	if !w2.DebounceUntil.After(w1.DebounceUntil) {
		t.Errorf("expected extended debounce_until (%v) > w1 (%v)", w2.DebounceUntil, w1.DebounceUntil)
	}

	// 3. Marca janela como processada
	err = debounceRepo.MarkWindowProcessed(ctx, conv.ID)
	if err != nil {
		t.Fatalf("failed to mark window as processed: %v", err)
	}

	wProcessed, err := debounceRepo.GetActiveWindow(ctx, conv.ID)
	if err != nil {
		t.Fatalf("failed to get window: %v", err)
	}
	if wProcessed.Status != domain.DebounceStatusProcessed {
		t.Errorf("expected PROCESSED status, got %s", wProcessed.Status)
	}

	// 4. Nova Mensagem após processada -> Deve abrir uma NOVA janela limpa (message_count = 1)
	w3, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, cfg)
	if err != nil {
		t.Fatalf("new message should create fresh window after processed: %v", err)
	}
	if w3.MessageCount != 1 {
		t.Errorf("expected fresh message_count 1, got %d", w3.MessageCount)
	}
	if w3.Status != domain.DebounceStatusOpen {
		t.Errorf("expected status OPEN, got %s", w3.Status)
	}
}

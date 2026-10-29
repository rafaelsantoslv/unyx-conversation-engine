package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

// DebounceRepository gerencia janelas duráveis de debounce no PostgreSQL (INV-01, INV-02, INV-10).
type DebounceRepository struct {
	pool *pgxpool.Pool
}

func NewDebounceRepository(pool *pgxpool.Pool) *DebounceRepository {
	return &DebounceRepository{pool: pool}
}

// RegisterOrExtendWindow registra uma nova mensagem na janela de debounce da conversa.
// Se não houver janela ativa ('OPEN'), cria uma nova com window_started_at, debounce_until e max_wait_until.
// Se já houver janela 'OPEN', estende atomicamente o debounce_until = LEAST(NOW() + inactivity_window, max_wait_until)
// e incrementa o message_count (INV-02, INV-10).
func (r *DebounceRepository) RegisterOrExtendWindow(ctx context.Context, tenantID, conversationID string, cfg domain.DebounceConfig) (*domain.DebounceWindow, error) {
	now := time.Now().UTC()
	windowID := uuid.New().String()

	inactivity := cfg.InactivityWindow
	if inactivity < 0 {
		inactivity = 0
	}
	maxWait := cfg.MaxWaitTimeout
	if maxWait < inactivity {
		maxWait = inactivity
	}

	initialDebounceUntil := now.Add(inactivity)
	initialMaxWaitUntil := now.Add(maxWait)

	// Inserção atômica se não houver janela ativa
	insertQuery := `
	INSERT INTO conversation_debounce_windows (
		id, tenant_id, conversation_id, window_started_at, debounce_until, max_wait_until, message_count, status, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, 1, 'OPEN', $4, $4
	)
	ON CONFLICT (conversation_id) DO NOTHING
	RETURNING id, tenant_id, conversation_id, window_started_at, debounce_until, max_wait_until, message_count, status, processed_at, created_at, updated_at;
	`

	var w domain.DebounceWindow
	var statusStr string
	err := r.pool.QueryRow(ctx, insertQuery, windowID, tenantID, conversationID, now, initialDebounceUntil, initialMaxWaitUntil).Scan(
		&w.ID,
		&w.TenantID,
		&w.ConversationID,
		&w.WindowStartedAt,
		&w.DebounceUntil,
		&w.MaxWaitUntil,
		&w.MessageCount,
		&statusStr,
		&w.ProcessedAt,
		&w.CreatedAt,
		&w.UpdatedAt,
	)

	if err == nil {
		w.Status = domain.DebounceWindowStatus(statusStr)
		return &w, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// Janela já existe: estende se status for OPEN ou recria se for PROCESSED
		// Se status = 'OPEN':
		//   debounce_until = LEAST($4 + inactivity, max_wait_until)
		//   message_count = message_count + 1
		// Se status = 'PROCESSED':
		//   abre uma nova janela com novos limites
		updateQuery := `
		UPDATE conversation_debounce_windows
		SET status = 'OPEN',
		    message_count = CASE 
		        WHEN status = 'OPEN' THEN message_count + 1 
		        ELSE 1 
		    END,
		    window_started_at = CASE 
		        WHEN status = 'OPEN' THEN window_started_at 
		        ELSE $2 
		    END,
		    max_wait_until = CASE 
		        WHEN status = 'OPEN' THEN max_wait_until 
		        ELSE $4 
		    END,
		    debounce_until = CASE 
		        WHEN status = 'OPEN' THEN LEAST($3, max_wait_until) 
		        ELSE $3 
		    END,
		    processed_at = NULL,
		    updated_at = $2
		WHERE conversation_id = $1
		RETURNING id, tenant_id, conversation_id, window_started_at, debounce_until, max_wait_until, message_count, status, processed_at, created_at, updated_at;
		`

		err = r.pool.QueryRow(ctx, updateQuery, conversationID, now, initialDebounceUntil, initialMaxWaitUntil).Scan(
			&w.ID,
			&w.TenantID,
			&w.ConversationID,
			&w.WindowStartedAt,
			&w.DebounceUntil,
			&w.MaxWaitUntil,
			&w.MessageCount,
			&statusStr,
			&w.ProcessedAt,
			&w.CreatedAt,
			&w.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update or extend debounce window: %w", err)
		}

		w.Status = domain.DebounceWindowStatus(statusStr)
		return &w, nil
	}

	return nil, fmt.Errorf("failed to register debounce window: %w", err)
}

// GetActiveWindow recupera a janela de debounce atual da conversa.
func (r *DebounceRepository) GetActiveWindow(ctx context.Context, conversationID string) (*domain.DebounceWindow, error) {
	query := `
	SELECT id, tenant_id, conversation_id, window_started_at, debounce_until, max_wait_until, message_count, status, processed_at, created_at, updated_at
	FROM conversation_debounce_windows
	WHERE conversation_id = $1;
	`
	var w domain.DebounceWindow
	var statusStr string
	err := r.pool.QueryRow(ctx, query, conversationID).Scan(
		&w.ID,
		&w.TenantID,
		&w.ConversationID,
		&w.WindowStartedAt,
		&w.DebounceUntil,
		&w.MaxWaitUntil,
		&w.MessageCount,
		&statusStr,
		&w.ProcessedAt,
		&w.CreatedAt,
		&w.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query debounce window: %w", err)
	}

	w.Status = domain.DebounceWindowStatus(statusStr)
	return &w, nil
}

// MarkWindowProcessed marca atomicamente a janela de debounce como PROCESSED para abrir caminho para novos lotes.
func (r *DebounceRepository) MarkWindowProcessed(ctx context.Context, conversationID string) error {
	now := time.Now().UTC()
	query := `
	UPDATE conversation_debounce_windows
	SET status = 'PROCESSED', processed_at = $1, updated_at = $1
	WHERE conversation_id = $2 AND status = 'OPEN';
	`
	_, err := r.pool.Exec(ctx, query, now, conversationID)
	if err != nil {
		return fmt.Errorf("failed to mark debounce window as processed: %w", err)
	}
	return nil
}

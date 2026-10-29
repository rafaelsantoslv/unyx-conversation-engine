package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

// ExecutionCoordinator gerencia concessões distribuídas de conversas no PostgreSQL (INV-02, INV-04, INV-08).
type ExecutionCoordinator struct {
	pool *pgxpool.Pool
}

func NewExecutionCoordinator(pool *pgxpool.Pool) *ExecutionCoordinator {
	return &ExecutionCoordinator{pool: pool}
}

// AcquireExecution obtém de forma atômica (Compare-And-Swap / Insert ON CONFLICT) o direito exclusivo de executar uma conversa.
// Implementa geração monotônica de fencing_token (INV-04) que é incrementado estritamente a cada nova aquisição.
func (c *ExecutionCoordinator) AcquireExecution(ctx context.Context, tenantID, conversationID, workerID string, leaseDuration time.Duration) (*domain.ConversationExecution, error) {
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	execID := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(leaseDuration)

	// 1. Tenta inserir nova concessão se a conversa não tiver registro (fencing_token inicia em 1)
	insertQuery := `
	INSERT INTO conversation_executions (id, tenant_id, conversation_id, worker_id, status, acquired_at, expires_at, fencing_token)
	VALUES ($1, $2, $3, $4, 'RUNNING', $5, $6, 1)
	ON CONFLICT (conversation_id) DO NOTHING
	RETURNING id, tenant_id, conversation_id, worker_id, status, acquired_at, expires_at, fencing_token;
	`

	var exec domain.ConversationExecution
	var statusStr string
	err := c.pool.QueryRow(ctx, insertQuery, execID, tenantID, conversationID, workerID, now, expiresAt).Scan(
		&exec.ID,
		&exec.TenantID,
		&exec.ConversationID,
		&exec.WorkerID,
		&statusStr,
		&exec.AcquiredAt,
		&exec.ExpiresAt,
		&exec.FencingToken,
	)

	if err == nil {
		exec.Status = domain.ExecutionStatus(statusStr)
		return &exec, nil
	}

	// 2. Se deu conflito de unicidade (registro já existe), verifica se a conversa está livre ou se o lease expirou
	if errors.Is(err, pgx.ErrNoRows) {
		// UPDATE condicional atômico (CAS): só adquire se status terminal ou lease expirado.
		// Incrementa estritamente o fencing_token: fencing_token = conversation_executions.fencing_token + 1 (INV-04)
		updateQuery := `
		UPDATE conversation_executions
		SET id = $1, tenant_id = $2, worker_id = $3, status = 'RUNNING', acquired_at = $4, expires_at = $5, released_at = NULL, fencing_token = conversation_executions.fencing_token + 1
		WHERE conversation_id = $6
		  AND (status IN ('SUCCEEDED', 'FAILED', 'PENDING') OR expires_at < $4)
		RETURNING id, tenant_id, conversation_id, worker_id, status, acquired_at, expires_at, fencing_token;
		`

		err = c.pool.QueryRow(ctx, updateQuery, execID, tenantID, workerID, now, expiresAt, conversationID).Scan(
			&exec.ID,
			&exec.TenantID,
			&exec.ConversationID,
			&exec.WorkerID,
			&statusStr,
			&exec.AcquiredAt,
			&exec.ExpiresAt,
			&exec.FencingToken,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Outro worker está rodando ativamente com lease não expirado
				return nil, domain.ErrConversationBusy
			}
			return nil, fmt.Errorf("failed to acquire conversation execution: %w", err)
		}

		exec.Status = domain.ExecutionStatus(statusStr)
		return &exec, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return nil, domain.ErrConversationBusy
	}

	return nil, fmt.Errorf("failed to acquire conversation execution lock: %w", err)
}

// RenewLease renova o lease da execução ativa estendendo seu expires_at condicionado ao fencing_token (INV-04 Heartbeat).
func (c *ExecutionCoordinator) RenewLease(ctx context.Context, executionID string, fencingToken int64, extension time.Duration) error {
	if extension <= 0 {
		extension = 30 * time.Second
	}
	now := time.Now().UTC()
	newExpiresAt := now.Add(extension)

	query := `
	UPDATE conversation_executions
	SET expires_at = $1
	WHERE id = $2 AND fencing_token = $3 AND status = 'RUNNING';
	`

	cmdTag, err := c.pool.Exec(ctx, query, newExpiresAt, executionID, fencingToken)
	if err != nil {
		return fmt.Errorf("failed to renew execution lease: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return domain.ErrFencingTokenStale
	}

	return nil
}

// ReleaseExecution libera a concessão da conversa de forma atômica para permitir a próxima mensagem,
// validando se o fencing_token ainda pertence a esta execução (INV-04).
func (c *ExecutionCoordinator) ReleaseExecution(ctx context.Context, executionID string, finalStatus domain.ExecutionStatus) error {
	now := time.Now().UTC()
	query := `
	UPDATE conversation_executions
	SET status = $1, released_at = $2
	WHERE id = $3;
	`
	_, err := c.pool.Exec(ctx, query, string(finalStatus), now, executionID)
	if err != nil {
		return fmt.Errorf("failed to release conversation execution: %w", err)
	}
	return nil
}

// ReleaseExecutionWithFencing libera a concessão validando estritamente o fencing_token.
func (c *ExecutionCoordinator) ReleaseExecutionWithFencing(ctx context.Context, executionID string, fencingToken int64, finalStatus domain.ExecutionStatus) error {
	now := time.Now().UTC()
	query := `
	UPDATE conversation_executions
	SET status = $1, released_at = $2
	WHERE id = $3 AND fencing_token = $4;
	`
	cmdTag, err := c.pool.Exec(ctx, query, string(finalStatus), now, executionID, fencingToken)
	if err != nil {
		return fmt.Errorf("failed to release conversation execution with fencing: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return domain.ErrFencingTokenStale
	}
	return nil
}

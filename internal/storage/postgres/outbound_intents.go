package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

type OutboundIntentsRepository struct {
	pool *pgxpool.Pool
}

func NewOutboundIntentsRepository(pool *pgxpool.Pool) *OutboundIntentsRepository {
	return &OutboundIntentsRepository{pool: pool}
}

// CreateIntent insere atomicamente uma intenção de envio com status PENDING (INV-04).
func (r *OutboundIntentsRepository) CreateIntent(ctx context.Context, intent domain.OutboundIntent) (*domain.OutboundIntent, error) {
	if intent.ID == "" {
		intent.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	if intent.MaxAttempts <= 0 {
		intent.MaxAttempts = 5
	}
	if intent.Status == "" {
		intent.Status = domain.OutboundStatusPending
	}

	payloadJSON, err := json.Marshal(intent.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal outbound intent payload: %w", err)
	}

	query := `
	INSERT INTO outbound_intents (
		id, tenant_id, conversation_id, execution_id, payload, status, attempts, max_attempts, next_retry_at, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10
	)
	ON CONFLICT (execution_id) DO NOTHING
	RETURNING id, tenant_id, conversation_id, execution_id, payload, status, attempts, max_attempts, next_retry_at, external_message_id, error_message, created_at, updated_at;
	`

	var res domain.OutboundIntent
	var statusStr string
	var rawPayload []byte

	err = r.pool.QueryRow(ctx, query,
		intent.ID, intent.TenantID, intent.ConversationID, intent.ExecutionID, payloadJSON,
		intent.Status, intent.Attempts, intent.MaxAttempts, intent.NextRetryAt, now,
	).Scan(
		&res.ID,
		&res.TenantID,
		&res.ConversationID,
		&res.ExecutionID,
		&rawPayload,
		&statusStr,
		&res.Attempts,
		&res.MaxAttempts,
		&res.NextRetryAt,
		&res.ExternalMessageID,
		&res.ErrorMessage,
		&res.CreatedAt,
		&res.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Já existia para esse execution_id
			return r.GetIntentByExecutionID(ctx, intent.TenantID, intent.ExecutionID)
		}
		return nil, fmt.Errorf("failed to insert outbound intent: %w", err)
	}

	res.Status = domain.OutboundIntentStatus(statusStr)
	_ = json.Unmarshal(rawPayload, &res.Payload)
	return &res, nil
}

// GetIntentByExecutionID busca uma intenção associada a uma execução.
func (r *OutboundIntentsRepository) GetIntentByExecutionID(ctx context.Context, tenantID, executionID string) (*domain.OutboundIntent, error) {
	query := `
	SELECT id, tenant_id, conversation_id, execution_id, payload, status, attempts, max_attempts, next_retry_at, external_message_id, error_message, created_at, updated_at
	FROM outbound_intents
	WHERE tenant_id = $1 AND execution_id = $2;
	`
	var res domain.OutboundIntent
	var statusStr string
	var rawPayload []byte

	err := r.pool.QueryRow(ctx, query, tenantID, executionID).Scan(
		&res.ID,
		&res.TenantID,
		&res.ConversationID,
		&res.ExecutionID,
		&rawPayload,
		&statusStr,
		&res.Attempts,
		&res.MaxAttempts,
		&res.NextRetryAt,
		&res.ExternalMessageID,
		&res.ErrorMessage,
		&res.CreatedAt,
		&res.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	res.Status = domain.OutboundIntentStatus(statusStr)
	_ = json.Unmarshal(rawPayload, &res.Payload)
	return &res, nil
}

// ClaimPendingIntents adquire atomicamente um lote de intenções PENDING para envio via FOR UPDATE SKIP LOCKED (INV-07).
func (r *OutboundIntentsRepository) ClaimPendingIntents(ctx context.Context, limit int, now time.Time) ([]domain.OutboundIntent, error) {
	if limit <= 0 {
		limit = 10
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx for claiming intents: %w", err)
	}
	defer tx.Rollback(ctx)

	selectQuery := `
	SELECT id, tenant_id, conversation_id, execution_id, payload, status, attempts, max_attempts, next_retry_at, external_message_id, error_message, created_at, updated_at
	FROM outbound_intents
	WHERE (status = 'PENDING' OR (status = 'FAILED' AND attempts < max_attempts))
	  AND (next_retry_at IS NULL OR next_retry_at <= $1)
	ORDER BY created_at ASC
	LIMIT $2
	FOR UPDATE SKIP LOCKED;
	`
	rows, err := tx.Query(ctx, selectQuery, now, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to select pending intents: %w", err)
	}
	defer rows.Close()

	var claimed []domain.OutboundIntent
	var idsToUpdate []string

	for rows.Next() {
		var item domain.OutboundIntent
		var statusStr string
		var rawPayload []byte
		if err := rows.Scan(
			&item.ID,
			&item.TenantID,
			&item.ConversationID,
			&item.ExecutionID,
			&rawPayload,
			&statusStr,
			&item.Attempts,
			&item.MaxAttempts,
			&item.NextRetryAt,
			&item.ExternalMessageID,
			&item.ErrorMessage,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan intent: %w", err)
		}
		item.Status = domain.OutboundIntentStatus(statusStr)
		_ = json.Unmarshal(rawPayload, &item.Payload)
		claimed = append(claimed, item)
		idsToUpdate = append(idsToUpdate, item.ID)
	}

	if len(idsToUpdate) > 0 {
		updateQuery := `
		UPDATE outbound_intents
		SET status = 'SENDING', attempts = attempts + 1, updated_at = $1
		WHERE id = ANY($2);
		`
		if _, err := tx.Exec(ctx, updateQuery, now, idsToUpdate); err != nil {
			return nil, fmt.Errorf("failed to update status to SENDING: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claim intents: %w", err)
	}

	return claimed, nil
}

// MarkSent marca a intenção como SENT registrando o external_message_id retornado pelo canal.
func (r *OutboundIntentsRepository) MarkSent(ctx context.Context, intentID, externalMessageID string) error {
	now := time.Now().UTC()
	query := `
	UPDATE outbound_intents
	SET status = 'SENT', external_message_id = $1, error_message = NULL, updated_at = $2
	WHERE id = $3;
	`
	_, err := r.pool.Exec(ctx, query, externalMessageID, now, intentID)
	return err
}

// MarkFailed registra a falha e agenda o próximo retry se attempts < max_attempts.
func (r *OutboundIntentsRepository) MarkFailed(ctx context.Context, intentID string, errReason string, retryAfter time.Duration) error {
	now := time.Now().UTC()
	nextRetry := now.Add(retryAfter)
	query := `
	UPDATE outbound_intents
	SET status = 'FAILED', error_message = $1, next_retry_at = $2, updated_at = $3
	WHERE id = $4;
	`
	_, err := r.pool.Exec(ctx, query, errReason, nextRetry, now, intentID)
	return err
}

// MarkCancelled marca o envio como cancelado (ex: cliente mudou para HUMAN_ACTIVE antes do despacho).
func (r *OutboundIntentsRepository) MarkCancelled(ctx context.Context, intentID string, reason string) error {
	now := time.Now().UTC()
	query := `
	UPDATE outbound_intents
	SET status = 'CANCELLED', error_message = $1, updated_at = $2
	WHERE id = $3;
	`
	_, err := r.pool.Exec(ctx, query, reason, now, intentID)
	return err
}

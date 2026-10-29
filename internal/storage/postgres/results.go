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

// ResultsRepository gerencia a persistência durável dos resultados de processamento.
type ResultsRepository struct {
	pool *pgxpool.Pool
}

func NewResultsRepository(pool *pgxpool.Pool) *ResultsRepository {
	return &ResultsRepository{pool: pool}
}

// GetResult busca o resultado existente de uma mensagem para checagem de idempotência.
func (r *ResultsRepository) GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	query := `
	SELECT id, tenant_id, conversation_id, message_id, status, output_payload, error_message, execution_time_ms, created_at, completed_at
	FROM processing_results
	WHERE tenant_id = $1 AND conversation_id = $2 AND message_id = $3;
	`

	var res domain.ProcessingResult
	var statusStr string
	err := r.pool.QueryRow(ctx, query, tenantID, conversationID, messageID).Scan(
		&res.ID,
		&res.TenantID,
		&res.ConversationID,
		&res.MessageID,
		&statusStr,
		&res.OutputPayload,
		&res.ErrorMessage,
		&res.ExecutionTimeMS,
		&res.CreatedAt,
		&res.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get processing result: %w", err)
	}

	res.Status = domain.ExecutionStatus(statusStr)
	return &res, nil
}

// RecordExecutionStart registra o início da execução com status RUNNING (ou atualiza se já existir).
func (r *ResultsRepository) RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	resultID := uuid.New().String()
	now := time.Now().UTC()

	query := `
	INSERT INTO processing_results (id, tenant_id, conversation_id, message_id, status, created_at)
	VALUES ($1, $2, $3, $4, 'RUNNING', $5)
	ON CONFLICT (tenant_id, conversation_id, message_id)
	DO UPDATE SET status = 'RUNNING', completed_at = NULL, error_message = NULL
	RETURNING id, tenant_id, conversation_id, message_id, status, created_at;
	`

	var res domain.ProcessingResult
	var statusStr string
	err := r.pool.QueryRow(ctx, query, resultID, tenantID, conversationID, messageID, now).Scan(
		&res.ID,
		&res.TenantID,
		&res.ConversationID,
		&res.MessageID,
		&statusStr,
		&res.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to record execution start: %w", err)
	}

	res.Status = domain.ExecutionStatus(statusStr)
	return &res, nil
}

// RecordExecutionSuccess atualiza o resultado para SUCCEEDED salvando o payload e o tempo de execução.
func (r *ResultsRepository) RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration) error {
	now := time.Now().UTC()
	payloadBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output payload: %w", err)
	}

	query := `
	UPDATE processing_results
	SET status = 'SUCCEEDED', output_payload = $1, execution_time_ms = $2, completed_at = $3, error_message = NULL
	WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6;
	`

	cmdTag, err := r.pool.Exec(ctx, query, payloadBytes, executionTime.Milliseconds(), now, tenantID, conversationID, messageID)
	if err != nil {
		return fmt.Errorf("failed to record execution success: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("processing result record not found for update: tenant=%s, conv=%s, msg=%s", tenantID, conversationID, messageID)
	}

	return nil
}

// RecordExecutionFailure atualiza o resultado para FAILED salvando a mensagem de erro e o tempo de execução.
func (r *ResultsRepository) RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error {
	now := time.Now().UTC()
	var errText string
	if failureErr != nil {
		errText = failureErr.Error()
	}

	query := `
	UPDATE processing_results
	SET status = 'FAILED', error_message = $1, execution_time_ms = $2, completed_at = $3
	WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6;
	`

	cmdTag, err := r.pool.Exec(ctx, query, errText, executionTime.Milliseconds(), now, tenantID, conversationID, messageID)
	if err != nil {
		return fmt.Errorf("failed to record execution failure: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("processing result record not found for update: tenant=%s, conv=%s, msg=%s", tenantID, conversationID, messageID)
	}

	return nil
}

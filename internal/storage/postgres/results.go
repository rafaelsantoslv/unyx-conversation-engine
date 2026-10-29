package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var (
	// ErrExecutionLocked indica que a execução já está em andamento (RUNNING com lease ativo)
	// por outro worker e não deve ser iniciada concorrentemente.
	ErrExecutionLocked = errors.New("execution is currently running with active lease")
)

// ResultsRepository gerencia a persistência durável dos resultados de processamento e controle de idempotência.
type ResultsRepository struct {
	pool *pgxpool.Pool
}

func NewResultsRepository(pool *pgxpool.Pool) *ResultsRepository {
	return &ResultsRepository{pool: pool}
}

// GetResult busca o resultado existente de uma mensagem para checagem de idempotência.
func (r *ResultsRepository) GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	query := `
	SELECT id, tenant_id, conversation_id, message_id, sequence_number, status, output_payload, error_message, execution_time_ms, created_at, completed_at, updated_at, lease_expires_at, fencing_token
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
		&res.SequenceNumber,
		&statusStr,
		&res.OutputPayload,
		&res.ErrorMessage,
		&res.ExecutionTimeMS,
		&res.CreatedAt,
		&res.CompletedAt,
		&res.UpdatedAt,
		&res.LeaseExpiresAt,
		&res.FencingToken,
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

// IsPrecedingSequenceCompleted verifica se a mensagem N-1 de uma conversa já foi concluída (SUCCEEDED).
// Se targetSeq <= 1, retorna true imediatamente (primeira mensagem da conversa).
func (r *ResultsRepository) IsPrecedingSequenceCompleted(ctx context.Context, conversationID string, targetSeq int64) (bool, error) {
	if targetSeq <= 1 {
		return true, nil
	}

	precedingSeq := targetSeq - 1
	query := `
	SELECT status
	FROM processing_results
	WHERE conversation_id = $1 AND sequence_number = $2;
	`

	var statusStr string
	err := r.pool.QueryRow(ctx, query, conversationID, precedingSeq).Scan(&statusStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// A mensagem anterior ainda não foi nem concluída nem gravada
			return false, nil
		}
		return false, fmt.Errorf("failed to check preceding sequence completion: %w", err)
	}

	return domain.ExecutionStatus(statusStr) == domain.ExecutionStatusSucceeded, nil
}

// AcquireExecutionLock tenta obter o lock idempotente de execução (com leaseDuration e sequenceNumber).
func (r *ResultsRepository) AcquireExecutionLockWithSeq(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration) (*domain.ProcessingResult, error) {
	return r.AcquireExecutionLockWithSeqAndFencing(ctx, tenantID, conversationID, messageID, sequenceNumber, leaseDuration, nil)
}

// AcquireExecutionLockWithSeqAndFencing associa opcionalmente o fencing_token gerado pela coordenação à execução da mensagem.
func (r *ResultsRepository) AcquireExecutionLockWithSeqAndFencing(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration, fencingToken *int64) (*domain.ProcessingResult, error) {
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	if sequenceNumber <= 0 {
		sequenceNumber = 1
	}

	resultID := uuid.New().String()
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseDuration)

	// Inserção atômica para nova execução
	insertQuery := `
	INSERT INTO processing_results (id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token)
	VALUES ($1, $2, $3, $4, $5, 'RUNNING', $6, $6, $7, $8)
	ON CONFLICT (tenant_id, conversation_id, message_id) DO NOTHING
	RETURNING id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token;
	`

	var res domain.ProcessingResult
	var statusStr string
	err := r.pool.QueryRow(ctx, insertQuery, resultID, tenantID, conversationID, messageID, sequenceNumber, now, leaseExpiresAt, fencingToken).Scan(
		&res.ID,
		&res.TenantID,
		&res.ConversationID,
		&res.MessageID,
		&res.SequenceNumber,
		&statusStr,
		&res.CreatedAt,
		&res.UpdatedAt,
		&res.LeaseExpiresAt,
		&res.FencingToken,
	)

	if err == nil {
		res.Status = domain.ExecutionStatus(statusStr)
		return &res, nil
	}

	// Se deu conflito de unicidade (DO NOTHING -> ErrNoRows)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := r.GetResult(ctx, tenantID, conversationID, messageID)
		if getErr != nil {
			return nil, getErr
		}
		if existing == nil {
			return nil, fmt.Errorf("concurrent insert conflict for msg %s", messageID)
		}

		if existing.Status == domain.ExecutionStatusSucceeded {
			return nil, nil
		}

		if existing.Status == domain.ExecutionStatusRunning && existing.LeaseExpiresAt != nil && existing.LeaseExpiresAt.After(now) {
			return nil, ErrExecutionLocked
		}

		updateQuery := `
		UPDATE processing_results
		SET status = 'RUNNING', sequence_number = $1, updated_at = $2, lease_expires_at = $3, fencing_token = $4, error_message = NULL
		WHERE tenant_id = $5 AND conversation_id = $6 AND message_id = $7
		  AND (status = 'FAILED' OR (status = 'RUNNING' AND (lease_expires_at IS NULL OR lease_expires_at < $2)))
		RETURNING id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token;
		`

		err = r.pool.QueryRow(ctx, updateQuery, sequenceNumber, now, leaseExpiresAt, fencingToken, tenantID, conversationID, messageID).Scan(
			&res.ID,
			&res.TenantID,
			&res.ConversationID,
			&res.MessageID,
			&res.SequenceNumber,
			&statusStr,
			&res.CreatedAt,
			&res.UpdatedAt,
			&res.LeaseExpiresAt,
			&res.FencingToken,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrExecutionLocked
			}
			return nil, fmt.Errorf("failed to take over expired/failed lease: %w", err)
		}

		res.Status = domain.ExecutionStatus(statusStr)
		return &res, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return nil, ErrExecutionLocked
	}

	return nil, fmt.Errorf("failed to acquire execution lock: %w", err)
}

func (r *ResultsRepository) AcquireExecutionLock(ctx context.Context, tenantID, conversationID, messageID string, leaseDuration time.Duration) (*domain.ProcessingResult, error) {
	return r.AcquireExecutionLockWithSeq(ctx, tenantID, conversationID, messageID, 1, leaseDuration)
}

func (r *ResultsRepository) RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	return r.AcquireExecutionLock(ctx, tenantID, conversationID, messageID, 30*time.Second)
}

// RecordExecutionSuccess atualiza o resultado para SUCCEEDED salvando o payload e o tempo de execução.
func (r *ResultsRepository) RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration) error {
	return r.RecordExecutionSuccessWithFencing(ctx, tenantID, conversationID, messageID, output, executionTime, 0)
}

// RecordExecutionSuccessWithFencing persiste o sucesso validando atomicamente o fencing_token (INV-04).
// Se fencingToken > 0, garante que o fencing_token não foi superado por outro worker.
func (r *ResultsRepository) RecordExecutionSuccessWithFencing(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration, fencingToken int64) error {
	now := time.Now().UTC()
	payloadBytes, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output payload: %w", err)
	}

	var query string
	var args []interface{}

	if fencingToken > 0 {
		query = `
		UPDATE processing_results
		SET status = 'SUCCEEDED', output_payload = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, error_message = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6
		  AND (fencing_token IS NULL OR fencing_token <= $7);
		`
		args = []interface{}{payloadBytes, executionTime.Milliseconds(), now, tenantID, conversationID, messageID, fencingToken}
	} else {
		query = `
		UPDATE processing_results
		SET status = 'SUCCEEDED', output_payload = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, error_message = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6;
		`
		args = []interface{}{payloadBytes, executionTime.Milliseconds(), now, tenantID, conversationID, messageID}
	}

	cmdTag, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to record execution success: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		if fencingToken > 0 {
			return domain.ErrFencingTokenStale
		}
		return fmt.Errorf("processing result record not found for update: tenant=%s, conv=%s, msg=%s", tenantID, conversationID, messageID)
	}

	return nil
}

// RecordExecutionFailure atualiza o resultado para FAILED salvando a mensagem de erro e liberando o lease.
func (r *ResultsRepository) RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error {
	return r.RecordExecutionFailureWithFencing(ctx, tenantID, conversationID, messageID, failureErr, executionTime, 0)
}

// RecordExecutionFailureWithFencing registra a falha condicionado ao fencing_token.
func (r *ResultsRepository) RecordExecutionFailureWithFencing(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration, fencingToken int64) error {
	now := time.Now().UTC()
	var errText string
	if failureErr != nil {
		errText = failureErr.Error()
	}

	var query string
	var args []interface{}

	if fencingToken > 0 {
		query = `
		UPDATE processing_results
		SET status = 'FAILED', error_message = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6
		  AND (fencing_token IS NULL OR fencing_token <= $7);
		`
		args = []interface{}{errText, executionTime.Milliseconds(), now, tenantID, conversationID, messageID, fencingToken}
	} else {
		query = `
		UPDATE processing_results
		SET status = 'FAILED', error_message = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6;
		`
		args = []interface{}{errText, executionTime.Milliseconds(), now, tenantID, conversationID, messageID}
	}

	cmdTag, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to record execution failure: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		if fencingToken > 0 {
			return domain.ErrFencingTokenStale
		}
		return fmt.Errorf("processing result record not found for update: tenant=%s, conv=%s, msg=%s", tenantID, conversationID, messageID)
	}

	return nil
}

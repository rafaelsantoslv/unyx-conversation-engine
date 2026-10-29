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
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/pkg/retry"
)

var (
	// ErrExecutionLocked indica que a execução já está em andamento (RUNNING com lease ativo)
	// por outro worker e não deve ser iniciada concorrentemente.
	ErrExecutionLocked = errors.New("execution is currently running with active lease")
)

// ResultsRepository gerencia a persistência durável dos resultados de processamento,
// idempotência com fencing token e agendamento durável de retries com backoff.
type ResultsRepository struct {
	pool *pgxpool.Pool
}

func NewResultsRepository(pool *pgxpool.Pool) *ResultsRepository {
	return &ResultsRepository{pool: pool}
}

// GetResult busca o resultado existente de uma mensagem para checagem de idempotência e retries.
func (r *ResultsRepository) GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	query := `
	SELECT id, tenant_id, conversation_id, message_id, sequence_number, status, output_payload, error_message, execution_time_ms, created_at, completed_at, updated_at, lease_expires_at, fencing_token, attempt_count, max_attempts, next_retry_at, last_error, error_type
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
		&res.AttemptCount,
		&res.MaxAttempts,
		&res.NextRetryAt,
		&res.LastError,
		&res.ErrorType,
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
// Se a mensagem N-1 foi enviada durante período humano (sequence_number <= resumed_at_sequence),
// considera-se completada (bypassed/read) para permitir a execução da nova mensagem da IA (INV-04, INV-06).
func (r *ResultsRepository) IsPrecedingSequenceCompleted(ctx context.Context, conversationID string, targetSeq int64) (bool, error) {
	if targetSeq <= 1 {
		return true, nil
	}

	precedingSeq := targetSeq - 1

	// Checa se a sequência precedente faz parte do período coberto pela marca de retomada (resumed_at_sequence)
	var resumedAtSeq int64
	err := r.pool.QueryRow(ctx, "SELECT resumed_at_sequence FROM conversations WHERE id = $1", conversationID).Scan(&resumedAtSeq)
	if err == nil && precedingSeq <= resumedAtSeq {
		return true, nil
	}

	query := `
	SELECT status
	FROM processing_results
	WHERE conversation_id = $1 AND sequence_number = $2;
	`

	var statusStr string
	err = r.pool.QueryRow(ctx, query, conversationID, precedingSeq).Scan(&statusStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
// Respeita estritamente o next_retry_at: se now < next_retry_at, a execução ainda não é elegível.
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
	INSERT INTO processing_results (id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token, attempt_count, max_attempts)
	VALUES ($1, $2, $3, $4, $5, 'RUNNING', $6, $6, $7, $8, 0, 5)
	ON CONFLICT (tenant_id, conversation_id, message_id) DO NOTHING
	RETURNING id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token, attempt_count, max_attempts, next_retry_at, last_error, error_type;
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
		&res.AttemptCount,
		&res.MaxAttempts,
		&res.NextRetryAt,
		&res.LastError,
		&res.ErrorType,
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

		// Se está em PENDING mas next_retry_at ainda é no futuro, não adquire o lock ainda
		if existing.NextRetryAt != nil && existing.NextRetryAt.After(now) {
			return nil, ErrExecutionLocked
		}

		if existing.Status == domain.ExecutionStatusRunning && existing.LeaseExpiresAt != nil && existing.LeaseExpiresAt.After(now) {
			return nil, ErrExecutionLocked
		}

		// UPDATE condicional para retomar retry ou lease expirado
		updateQuery := `
		UPDATE processing_results
		SET status = 'RUNNING', sequence_number = $1, updated_at = $2, lease_expires_at = $3, fencing_token = $4, error_message = NULL
		WHERE tenant_id = $5 AND conversation_id = $6 AND message_id = $7
		  AND (
		      status = 'FAILED' 
		      OR (status = 'PENDING' AND (next_retry_at IS NULL OR next_retry_at <= $2))
		      OR (status = 'RUNNING' AND (lease_expires_at IS NULL OR lease_expires_at < $2))
		  )
		RETURNING id, tenant_id, conversation_id, message_id, sequence_number, status, created_at, updated_at, lease_expires_at, fencing_token, attempt_count, max_attempts, next_retry_at, last_error, error_type;
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
			&res.AttemptCount,
			&res.MaxAttempts,
			&res.NextRetryAt,
			&res.LastError,
			&res.ErrorType,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrExecutionLocked
			}
			return nil, fmt.Errorf("failed to take over retry/expired lease: %w", err)
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
func (r *ResultsRepository) RecordExecutionSuccessWithFencing(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, executionTime time.Duration, fencingToken int64) error {
	_, err := r.RecordExecutionSuccessWithApplicability(ctx, tenantID, conversationID, messageID, output, executionTime, fencingToken, 0)
	return err
}

// RecordExecutionSuccessWithApplicability valida atômica e simultaneamente o fencing token, o estado da conversa ('AI_ACTIVE')
// e a versão da conversa antes de registrar o resultado como SUCCEEDED e emitir a intenção de envio (Outbound Intent) na Outbox (INV-06, INV-07).
// Se o estado migrou para 'HUMAN_ACTIVE' ou 'CLOSED', ou se a versão divergiu, o resultado é gravado como 'DISCARDED_STALE' e
// NENHUM evento de envio é inserido na Outbox, protegendo operadores humanos de atropelos (INV-07).
func (r *ResultsRepository) RecordExecutionSuccessWithApplicability(
	ctx context.Context,
	tenantID, conversationID, messageID string,
	output interface{},
	executionTime time.Duration,
	fencingToken int64,
	expectedVersion int64,
) (*domain.ApplicabilityCheckResult, error) {
	now := time.Now().UTC()
	payloadBytes, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output payload: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Checa atômica e diretamente no PostgreSQL o estado e a versão atual da conversa
	var currentStateStr string
	var currentVersion int64
	checkConvQuery := `
	SELECT status, version
	FROM conversations
	WHERE tenant_id = $1 AND id = $2;
	`
	err = tx.QueryRow(ctx, checkConvQuery, tenantID, conversationID).Scan(&currentStateStr, &currentVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to verify conversation state: %w", err)
	}
	currentState := domain.ConversationState(currentStateStr)

	// Validação de aplicabilidade: conversa precisa estar em AI_ACTIVE
	// e, se expectedVersion > 0, a versão não pode ter divergido
	isApplicable := true
	discardReason := ""

	if currentState != domain.StateAIActive {
		isApplicable = false
		discardReason = fmt.Sprintf("conversation state changed to %s during computation", currentState)
	} else if expectedVersion > 0 && currentVersion != expectedVersion {
		isApplicable = false
		discardReason = fmt.Sprintf("conversation version mismatch (expected %d, got %d)", expectedVersion, currentVersion)
	}

	if !isApplicable {
		// Grava como DISCARDED_STALE no processing_results
		discardQuery := `
		UPDATE processing_results
		SET status = 'DISCARDED_STALE',
		    output_payload = $1,
		    execution_time_ms = $2,
		    completed_at = $3,
		    updated_at = $3,
		    error_message = $4,
		    last_error = $4,
		    error_type = 'STALE_SUPERSEDED',
		    lease_expires_at = NULL,
		    next_retry_at = NULL
		WHERE tenant_id = $5 AND conversation_id = $6 AND message_id = $7
		  AND (fencing_token IS NULL OR $8 = 0 OR fencing_token <= $8);
		`
		cmdTag, err := tx.Exec(ctx, discardQuery, payloadBytes, executionTime.Milliseconds(), now, discardReason, tenantID, conversationID, messageID, fencingToken)
		if err != nil {
			return nil, fmt.Errorf("failed to record stale discarded result: %w", err)
		}
		if cmdTag.RowsAffected() == 0 && fencingToken > 0 {
			return nil, domain.ErrFencingTokenStale
		}

		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit discarded transaction: %w", err)
		}

		return &domain.ApplicabilityCheckResult{
			IsApplicable:   false,
			FinalStatus:    domain.ExecutionStatusDiscardedStale,
			Reason:         discardReason,
			CurrentState:   currentState,
			CurrentVersion: currentVersion,
		}, nil
	}

	// 2. Se for aplicável, atualiza o resultado para SUCCEEDED
	var updateQuery string
	var args []interface{}
	if fencingToken > 0 {
		updateQuery = `
		UPDATE processing_results
		SET status = 'SUCCEEDED', output_payload = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, next_retry_at = NULL, error_message = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6
		  AND (fencing_token IS NULL OR fencing_token <= $7);
		`
		args = []interface{}{payloadBytes, executionTime.Milliseconds(), now, tenantID, conversationID, messageID, fencingToken}
	} else {
		updateQuery = `
		UPDATE processing_results
		SET status = 'SUCCEEDED', output_payload = $1, execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, next_retry_at = NULL, error_message = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6;
		`
		args = []interface{}{payloadBytes, executionTime.Milliseconds(), now, tenantID, conversationID, messageID}
	}

	cmdTag, err := tx.Exec(ctx, updateQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to record execution success: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		if fencingToken > 0 {
			return nil, domain.ErrFencingTokenStale
		}
		return nil, fmt.Errorf("processing result record not found for update: tenant=%s, conv=%s, msg=%s", tenantID, conversationID, messageID)
	}

	// 3. Emite o evento de intenção de envio (Outbound Intent) na mesma transação atômica
	var outContent string
	var tokensUsed int
	if outObj, ok := output.(*domain.ExecutionOutput); ok && outObj != nil {
		outContent = outObj.ResponseContent
		tokensUsed = outObj.TokensUsed
	} else if outObj, ok := output.(domain.ExecutionOutput); ok {
		outContent = outObj.ResponseContent
		tokensUsed = outObj.TokensUsed
	}

	outboundPayload := domain.Event[domain.OutboundIntentPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantID,
		ConversationID: conversationID,
		EventType:      domain.EventTypeOutboundIntent,
		Version:        domain.EventVersionV1,
		OccurredAt:     now,
		Payload: domain.OutboundIntentPayload{
			MessageID:      messageID,
			ConversationID: conversationID,
			TenantID:       tenantID,
			Content:        outContent,
			GeneratedAt:    now,
			TokensUsed:     tokensUsed,
		},
	}
	outboundBytes, err := json.Marshal(outboundPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal outbound event payload: %w", err)
	}

	insertOutboxQuery := `
	INSERT INTO outbox_events (
		id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, 'PENDING', 0, $7
	);
	`
	outboxID := uuid.New().String()
	_, err = tx.Exec(ctx, insertOutboxQuery,
		outboxID,
		tenantID,
		conversationID,
		domain.EventTypeOutboundIntent,
		outboundBytes,
		conversationID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert outbound intent to outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit execution success and outbound intent: %w", err)
	}

	return &domain.ApplicabilityCheckResult{
		IsApplicable:   true,
		FinalStatus:    domain.ExecutionStatusSucceeded,
		CurrentState:   currentState,
		CurrentVersion: currentVersion,
	}, nil
}

// RecordExecutionFailure atualiza o resultado para FAILED salvando a mensagem de erro e liberando o lease.
func (r *ResultsRepository) RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, executionTime time.Duration) error {
	return r.RecordExecutionFailureWithFencing(ctx, tenantID, conversationID, messageID, failureErr, executionTime, 0)
}

// RecordExecutionFailureWithFencing registra falha permanente.
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
		SET status = 'FAILED', error_message = $1, last_error = $1, error_type = 'PERMANENT', execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, next_retry_at = NULL
		WHERE tenant_id = $4 AND conversation_id = $5 AND message_id = $6
		  AND (fencing_token IS NULL OR fencing_token <= $7);
		`
		args = []interface{}{errText, executionTime.Milliseconds(), now, tenantID, conversationID, messageID, fencingToken}
	} else {
		query = `
		UPDATE processing_results
		SET status = 'FAILED', error_message = $1, last_error = $1, error_type = 'PERMANENT', execution_time_ms = $2, completed_at = $3, updated_at = $3, lease_expires_at = NULL, next_retry_at = NULL
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

// ScheduleRetry avalia se a falha é elegível para retry e agenda no banco (INV-05, INV-06).
// Se for transitório e attempt < max_attempts: atualiza status para 'PENDING', calcula next_retry_at com backoff+jitter.
// Se exceder max_attempts ou for permanente: marca status = 'FAILED'.
// Retorna (scheduled=true, nextRetry, nil) se foi agendado; (scheduled=false, _, nil) se esgotou ou for permanente.
func (r *ResultsRepository) ScheduleRetry(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, baseBackoff, maxBackoff time.Duration) (bool, time.Time, error) {
	now := time.Now().UTC()
	var errText string
	if failureErr != nil {
		errText = failureErr.Error()
	}

	errType := retry.ClassifyError(failureErr)
	isRetryable := errType == retry.ErrorTypeTransient

	// 1. Busca registro atual para verificar attempt_count e max_attempts
	existing, err := r.GetResult(ctx, tenantID, conversationID, messageID)
	if err != nil {
		return false, time.Time{}, err
	}
	if existing == nil {
		return false, time.Time{}, fmt.Errorf("result not found for scheduling retry: msg %s", messageID)
	}

	nextAttempt := existing.AttemptCount + 1
	if isRetryable && nextAttempt <= existing.MaxAttempts {
		// Calcula backoff exponencial com Full Jitter
		backoffDuration := retry.CalculateBackoff(nextAttempt, baseBackoff, maxBackoff)
		nextRetryAt := now.Add(backoffDuration)

		query := `
		UPDATE processing_results
		SET status = 'PENDING',
		    attempt_count = $1,
		    next_retry_at = $2,
		    last_error = $3,
		    error_type = $4,
		    updated_at = $5,
		    lease_expires_at = NULL
		WHERE tenant_id = $6 AND conversation_id = $7 AND message_id = $8;
		`
		_, err := r.pool.Exec(ctx, query, nextAttempt, nextRetryAt, errText, string(errType), now, tenantID, conversationID, messageID)
		if err != nil {
			return false, time.Time{}, fmt.Errorf("failed to schedule retry in db: %w", err)
		}

		return true, nextRetryAt, nil
	}

	// Tentativas esgotadas ou erro permanente: marca como FAILED terminal
	query := `
	UPDATE processing_results
	SET status = 'FAILED',
	    attempt_count = $1,
	    last_error = $2,
	    error_message = $2,
	    error_type = $3,
	    completed_at = $4,
	    updated_at = $4,
	    lease_expires_at = NULL,
	    next_retry_at = NULL
	WHERE tenant_id = $5 AND conversation_id = $6 AND message_id = $7;
	`
	_, err = r.pool.Exec(ctx, query, nextAttempt, errText, string(errType), now, tenantID, conversationID, messageID)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("failed to mark terminal failure in db: %w", err)
	}

	return false, time.Time{}, nil
}

// QuarantineEvent isola duravelmente um evento na tabela quarantine_events (Dead Letter),
// preservando o payload sanitizado e a causa raiz do erro (INV-06, INV-08).
func (r *ResultsRepository) QuarantineEvent(ctx context.Context, qEvent domain.QuarantinedEvent) error {
	if qEvent.ID == "" {
		qEvent.ID = uuid.New().String()
	}
	if qEvent.QuarantinedAt.IsZero() {
		qEvent.QuarantinedAt = time.Now().UTC()
	}
	if len(qEvent.Payload) == 0 {
		qEvent.Payload = json.RawMessage(`{}`)
	} else if !json.Valid(qEvent.Payload) {
		// Se o payload for texto corrompido que não é JSON válido, encapsula com segurança como string JSON
		escaped, _ := json.Marshal(string(qEvent.Payload))
		qEvent.Payload = json.RawMessage(fmt.Sprintf(`{"raw_malformed_payload": %s}`, escaped))
	}

	query := `
	INSERT INTO quarantine_events (
		id, tenant_id, conversation_id, event_id, payload, error_reason, stack_trace, attempt_count, quarantined_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`

	_, err := r.pool.Exec(
		ctx,
		query,
		qEvent.ID,
		qEvent.TenantID,
		qEvent.ConversationID,
		qEvent.EventID,
		qEvent.Payload,
		qEvent.ErrorReason,
		qEvent.StackTrace,
		qEvent.AttemptCount,
		qEvent.QuarantinedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert quarantine event: %w", err)
	}

	return nil
}

// GetQuarantinedEvent recupera um evento da quarentena pelo event_id e tenant_id.
func (r *ResultsRepository) GetQuarantinedEvent(ctx context.Context, tenantID, eventID string) (*domain.QuarantinedEvent, error) {
	query := `
	SELECT id, tenant_id, conversation_id, event_id, payload, error_reason, stack_trace, attempt_count, quarantined_at, resolved_at
	FROM quarantine_events
	WHERE tenant_id = $1 AND event_id = $2;
	`

	var q domain.QuarantinedEvent
	var stackTrace *string
	err := r.pool.QueryRow(ctx, query, tenantID, eventID).Scan(
		&q.ID,
		&q.TenantID,
		&q.ConversationID,
		&q.EventID,
		&q.Payload,
		&q.ErrorReason,
		&stackTrace,
		&q.AttemptCount,
		&q.QuarantinedAt,
		&q.ResolvedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get quarantined event: %w", err)
	}

	if stackTrace != nil {
		q.StackTrace = *stackTrace
	}

	return &q, nil
}

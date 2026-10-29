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

// ScheduleRepository gerencia o ciclo de vida dos agendamentos persistidos no PostgreSQL (INV-03, INV-05, INV-08).
type ScheduleRepository struct {
	pool *pgxpool.Pool
}

func NewScheduleRepository(pool *pgxpool.Pool) *ScheduleRepository {
	return &ScheduleRepository{pool: pool}
}

// ScheduleExecution cria ou atualiza um agendamento durável para uma conversa.
// Se já existir um agendamento para a conversa, atualiza o scheduled_for e redefine o status para 'PENDING'.
func (r *ScheduleRepository) ScheduleExecution(ctx context.Context, tenantID, conversationID string, scheduledFor time.Time) (*domain.ConversationSchedule, error) {
	if tenantID == "" || conversationID == "" {
		return nil, errors.New("tenantID and conversationID must not be empty")
	}
	if scheduledFor.IsZero() {
		return nil, domain.ErrZeroScheduledFor
	}

	scheduleID := uuid.New().String()
	now := time.Now().UTC()

	query := `
	INSERT INTO conversation_schedules (
		id, tenant_id, conversation_id, scheduled_for, status, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, 'PENDING', $5, $5
	)
	ON CONFLICT (conversation_id)
	DO UPDATE SET
		scheduled_for = EXCLUDED.scheduled_for,
		status = 'PENDING',
		claimed_at = NULL,
		updated_at = EXCLUDED.updated_at
	RETURNING id, tenant_id, conversation_id, scheduled_for, status, claimed_at, created_at, updated_at;
	`

	var s domain.ConversationSchedule
	var statusStr string
	err := r.pool.QueryRow(ctx, query, scheduleID, tenantID, conversationID, scheduledFor.UTC(), now).Scan(
		&s.ID,
		&s.TenantID,
		&s.ConversationID,
		&s.ScheduledFor,
		&statusStr,
		&s.ClaimedAt,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to schedule conversation execution: %w", err)
	}
	s.Status = domain.ScheduleStatus(statusStr)
	return &s, nil
}

// ClaimDueSchedulesWithOutbox reivindica de forma atômica agendamentos com status 'PENDING' e scheduled_for <= now,
// usando SELECT ... FOR UPDATE SKIP LOCKED para prevenir disputa concorrente entre schedulers,
// e insere na mesma transação o evento 'conversation.execution.ready' na tabela outbox_events (INV-03, INV-05).
func (r *ScheduleRepository) ClaimDueSchedulesWithOutbox(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error) {
	if batchSize <= 0 {
		batchSize = 50
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Reivindica atomicamente as tarefas prontas via FOR UPDATE SKIP LOCKED
	claimQuery := `
	WITH due AS (
		SELECT id
		FROM conversation_schedules
		WHERE status = 'PENDING' AND scheduled_for <= $1
		ORDER BY scheduled_for ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	UPDATE conversation_schedules cs
	SET status = 'CLAIMED',
	    claimed_at = $1,
	    updated_at = $1
	FROM due
	WHERE cs.id = due.id
	RETURNING cs.id, cs.tenant_id, cs.conversation_id, cs.scheduled_for, cs.status, cs.claimed_at, cs.created_at, cs.updated_at;
	`

	rows, err := tx.Query(ctx, claimQuery, now, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to claim due schedules: %w", err)
	}
	defer rows.Close()

	var claimed []domain.ConversationSchedule
	for rows.Next() {
		var s domain.ConversationSchedule
		var statusStr string
		if err := rows.Scan(
			&s.ID,
			&s.TenantID,
			&s.ConversationID,
			&s.ScheduledFor,
			&statusStr,
			&s.ClaimedAt,
			&s.CreatedAt,
			&s.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan claimed schedule: %w", err)
		}
		s.Status = domain.ScheduleStatus(statusStr)
		claimed = append(claimed, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading claimed schedule rows: %w", err)
	}

	if len(claimed) == 0 {
		return nil, nil
	}

	// 2. Insere eventos de Outbox atomicamente na mesma transação para cada agendamento reivindicado
	outboxInsertQuery := `
	INSERT INTO outbox_events (
		id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, 'PENDING', 0, $7
	);
	`

	for _, s := range claimed {
		// 2.1 Verifica o estado atual e a marca de retomada (resumed_at_sequence) da conversa (INV-04, INV-06)
		var convState string
		var resumedAtSeq int64
		_ = tx.QueryRow(ctx, "SELECT status, resumed_at_sequence FROM conversations WHERE tenant_id = $1 AND id = $2", s.TenantID, s.ConversationID).Scan(&convState, &resumedAtSeq)

		// Se a conversa não está em AI_ACTIVE, não emite nem cria ExecutionRequest (INV-06)
		if convState != string(domain.StateAIActive) {
			continue
		}

		// 2.2 Cria ExecutionRequest com snapshot delimitado pelo watermark e IDs das mensagens após resumedAtSeq (INV-01, INV-04)
		var messageIDs []string
		var maxSeq int64 = resumedAtSeq

		msgQuery := `
		SELECT id, sequence_number
		FROM messages
		WHERE tenant_id = $1 AND conversation_id = $2 AND sequence_number > $3
		ORDER BY sequence_number ASC;
		`
		mRows, mErr := tx.Query(ctx, msgQuery, s.TenantID, s.ConversationID, resumedAtSeq)
		if mErr == nil {
			for mRows.Next() {
				var mid string
				var mseq int64
				if scanErr := mRows.Scan(&mid, &mseq); scanErr == nil {
					messageIDs = append(messageIDs, mid)
					if mseq > maxSeq {
						maxSeq = mseq
					}
				}
			}
			mRows.Close()
		}

		// Se não há mensagens gravadas ainda, mas o agendamento foi disparado, define maxSeq padrão 0
		var execReqID string
		if len(messageIDs) > 0 {
			execReqID = uuid.New().String()
			msgIDsJSON, _ := json.Marshal(messageIDs)

			insertReqQuery := `
			INSERT INTO execution_requests (
				id, tenant_id, conversation_id, generation_id, watermark_sequence, message_ids, status, created_at, updated_at
			) VALUES (
				$1, $2, $3, 1, $4, $5, 'PENDING', $6, $6
			);
			`
			_, _ = tx.Exec(ctx, insertReqQuery, execReqID, s.TenantID, s.ConversationID, maxSeq, msgIDsJSON, now)
		}

		payloadObj := domain.Event[domain.ExecutionReadyPayload]{
			EventID:        uuid.New().String(),
			TenantID:       s.TenantID,
			ConversationID: s.ConversationID,
			EventType:      domain.EventTypeExecutionReady,
			Version:        domain.EventVersionV1,
			OccurredAt:     now,
			Payload: domain.ExecutionReadyPayload{
				ExecutionRequestID: execReqID,
				ConversationID:     s.ConversationID,
				TenantID:           s.TenantID,
				WatermarkSequence:  maxSeq,
				MessageIDs:         messageIDs,
				TriggeredAt:        now,
				Reason:             "debounce_or_scheduled_execution",
			},
		}

		payloadBytes, err := json.Marshal(payloadObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal outbox event payload: %w", err)
		}

		outboxID := uuid.New().String()
		_, err = tx.Exec(ctx, outboxInsertQuery,
			outboxID,
			s.TenantID,
			s.ConversationID,
			domain.EventTypeExecutionReady,
			payloadBytes,
			s.ConversationID, // partition_key garante ordem estrita por conversa no Kafka
			now,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to insert outbox event for schedule %s: %w", s.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit claim and outbox transaction: %w", err)
	}

	return claimed, nil
}

// MarkScheduleExecuted marca o agendamento como executado com sucesso ('EXECUTED').
func (r *ScheduleRepository) MarkScheduleExecuted(ctx context.Context, conversationID string) error {
	query := `
	UPDATE conversation_schedules
	SET status = 'EXECUTED', updated_at = NOW()
	WHERE conversation_id = $1 AND status = 'CLAIMED';
	`
	_, err := r.pool.Exec(ctx, query, conversationID)
	return err
}

// CancelSchedule cancela um agendamento pendente ('CANCELLED').
func (r *ScheduleRepository) CancelSchedule(ctx context.Context, conversationID string) error {
	query := `
	UPDATE conversation_schedules
	SET status = 'CANCELLED', updated_at = NOW()
	WHERE conversation_id = $1 AND status = 'PENDING';
	`
	_, err := r.pool.Exec(ctx, query, conversationID)
	return err
}

// GetSchedule busca o agendamento atual de uma conversa.
func (r *ScheduleRepository) GetSchedule(ctx context.Context, conversationID string) (*domain.ConversationSchedule, error) {
	query := `
	SELECT id, tenant_id, conversation_id, scheduled_for, status, claimed_at, created_at, updated_at
	FROM conversation_schedules
	WHERE conversation_id = $1;
	`
	var s domain.ConversationSchedule
	var statusStr string
	err := r.pool.QueryRow(ctx, query, conversationID).Scan(
		&s.ID,
		&s.TenantID,
		&s.ConversationID,
		&s.ScheduledFor,
		&statusStr,
		&s.ClaimedAt,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	s.Status = domain.ScheduleStatus(statusStr)
	return &s, nil
}

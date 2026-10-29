package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var ErrDuplicateMessage = errors.New("duplicate message: external_id already exists for tenant")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) GetOrCreateConversation(ctx context.Context, tenantID, externalID string) (*domain.Conversation, error) {
	if tenantID == "" || externalID == "" {
		return nil, errors.New("tenantID and externalID must not be empty")
	}

	newID := uuid.New().String()
	now := time.Now().UTC()

	query := `
	INSERT INTO conversations (id, tenant_id, external_id, channel, status, version, metadata, created_at, updated_at)
	VALUES ($1, $2, $3, 'default', 'AI_ACTIVE', 1, '{}'::jsonb, $4, $4)
	ON CONFLICT (tenant_id, external_id)
	DO UPDATE SET updated_at = conversations.updated_at
	RETURNING id, tenant_id, COALESCE(user_id, ''), external_id, channel, status, version, resumed_at_sequence, metadata, created_at, updated_at;
	`

	var conv domain.Conversation
	var statusStr string
	var metaBytes []byte

	err := r.pool.QueryRow(ctx, query, newID, tenantID, externalID, now).Scan(
		&conv.ID,
		&conv.TenantID,
		&conv.UserID,
		&conv.ExternalConversationID,
		&conv.Channel,
		&statusStr,
		&conv.Version,
		&conv.ResumedAtSequence,
		&metaBytes,
		&conv.CreatedAt,
		&conv.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create conversation: %w", err)
	}

	conv.State = domain.ConversationState(statusStr)
	if len(metaBytes) > 0 {
		_ = json.Unmarshal(metaBytes, &conv.Metadata)
	}

	return &conv, nil
}

func (r *Repository) InsertMessage(ctx context.Context, tx pgx.Tx, msg *domain.Message) error {
	if msg == nil {
		return errors.New("message cannot be nil")
	}
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}

	var convExists bool
	err := tx.QueryRow(ctx, "SELECT true FROM conversations WHERE id = $1 FOR UPDATE", msg.ConversationID).Scan(&convExists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("conversation %s not found: %w", msg.ConversationID, err)
		}
		return fmt.Errorf("failed to lock conversation: %w", err)
	}

	var nextSeq int64
	seqQuery := `SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM messages WHERE conversation_id = $1`
	if err := tx.QueryRow(ctx, seqQuery, msg.ConversationID).Scan(&nextSeq); err != nil {
		return fmt.Errorf("failed to calculate next sequence number: %w", err)
	}
	msg.SequenceNumber = nextSeq

	insertQuery := `
	INSERT INTO messages (id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at;
	`

	var roleStr string
	err = tx.QueryRow(ctx, insertQuery,
		msg.ID,
		msg.TenantID,
		msg.ConversationID,
		msg.ExternalMessageID,
		string(msg.Role),
		msg.Content,
		msg.SequenceNumber,
		msg.CreatedAt,
	).Scan(
		&msg.ID,
		&msg.TenantID,
		&msg.ConversationID,
		&msg.ExternalMessageID,
		&roleStr,
		&msg.Content,
		&msg.SequenceNumber,
		&msg.CreatedAt,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicateMessage
		}
		return fmt.Errorf("failed to insert message: %w", err)
	}

	msg.Role = domain.MessageRole(roleStr)
	return nil
}

func (r *Repository) InsertOutboxEvent(ctx context.Context, tx pgx.Tx, evt *domain.OutboxEvent) error {
	if evt == nil {
		return errors.New("outbox event cannot be nil")
	}
	if evt.ID == "" {
		evt.ID = uuid.New().String()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	if evt.Status == "" {
		evt.Status = domain.OutboxStatusPending
	}

	query := `
	INSERT INTO outbox_events (id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at, published_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at, published_at;
	`

	var statusStr string
	err := tx.QueryRow(ctx, query,
		evt.ID,
		evt.TenantID,
		evt.ConversationID,
		evt.EventType,
		evt.Payload,
		evt.PartitionKey,
		string(evt.Status),
		evt.RetryCount,
		evt.CreatedAt,
		evt.PublishedAt,
	).Scan(
		&evt.ID,
		&evt.TenantID,
		&evt.ConversationID,
		&evt.EventType,
		&evt.Payload,
		&evt.PartitionKey,
		&statusStr,
		&evt.RetryCount,
		&evt.CreatedAt,
		&evt.PublishedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	evt.Status = domain.OutboxStatus(statusStr)
	return nil
}

func (r *Repository) SaveMessageWithOutbox(ctx context.Context, msg *domain.Message, evt *domain.OutboxEvent) (*domain.Message, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if msg != nil {
		if err := r.InsertMessage(ctx, tx, msg); err != nil {
			if errors.Is(err, ErrDuplicateMessage) {
				_ = tx.Rollback(ctx)
				existing, fetchErr := r.GetMessageByExternalID(ctx, msg.TenantID, msg.ExternalMessageID)
				if fetchErr == nil {
					return existing, ErrDuplicateMessage
				}
				return nil, ErrDuplicateMessage
			}
			return nil, err
		}
	}

	if evt != nil {
		if msg != nil {
			evt.TenantID = msg.TenantID
			evt.ConversationID = msg.ConversationID
			if evt.PartitionKey == "" {
				evt.PartitionKey = msg.ConversationID
			}

			// Se o payload do evento for do tipo MessageReceived, atualiza o sequence_number real atribuído
			var envelope map[string]interface{}
			if err := json.Unmarshal(evt.Payload, &envelope); err == nil {
				if payloadMap, ok := envelope["payload"].(map[string]interface{}); ok {
					payloadMap["sequence_number"] = msg.SequenceNumber
					if updatedBytes, err := json.Marshal(envelope); err == nil {
						evt.Payload = updatedBytes
					}
				}
			}
		}
		if err := r.InsertOutboxEvent(ctx, tx, evt); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return msg, nil
}

func (r *Repository) SaveMessage(ctx context.Context, msg *domain.Message) (*domain.Message, error) {
	return r.SaveMessageWithOutbox(ctx, msg, nil)
}

func (r *Repository) FetchPendingOutboxEvents(ctx context.Context, limit int) ([]*domain.OutboxEvent, error) {
	if limit <= 0 {
		limit = 50
	}

	query := `
	SELECT id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at, published_at
	FROM outbox_events
	WHERE status = 'PENDING'
	ORDER BY created_at ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED;
	`

	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		var evt domain.OutboxEvent
		var statusStr string
		err := rows.Scan(
			&evt.ID,
			&evt.TenantID,
			&evt.ConversationID,
			&evt.EventType,
			&evt.Payload,
			&evt.PartitionKey,
			&statusStr,
			&evt.RetryCount,
			&evt.CreatedAt,
			&evt.PublishedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		evt.Status = domain.OutboxStatus(statusStr)
		events = append(events, &evt)
	}

	return events, rows.Err()
}

func (r *Repository) MarkOutboxEventPublished(ctx context.Context, id string) error {
	now := time.Now().UTC()
	query := `
	UPDATE outbox_events
	SET status = 'PUBLISHED', published_at = $1
	WHERE id = $2;
	`
	_, err := r.pool.Exec(ctx, query, now, id)
	return err
}

func (r *Repository) GetMessageByExternalID(ctx context.Context, tenantID, externalID string) (*domain.Message, error) {
	query := `
	SELECT id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at
	FROM messages
	WHERE tenant_id = $1 AND external_id = $2;
	`
	var msg domain.Message
	var roleStr string
	err := r.pool.QueryRow(ctx, query, tenantID, externalID).Scan(
		&msg.ID,
		&msg.TenantID,
		&msg.ConversationID,
		&msg.ExternalMessageID,
		&roleStr,
		&msg.Content,
		&msg.SequenceNumber,
		&msg.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	msg.Role = domain.MessageRole(roleStr)
	return &msg, nil
}

// TransitionConversationState executa uma transição formal na FSM da conversa com:
// 1. Validação de guardas de domínio (CanTransition).
// 2. Concorrência otimista estrita via versionamento (expectedVersion).
// 3. Registro auditável imutável na tabela conversation_state_history.
// 4. Emissão atômica do evento conversation.state.changed na outbox_events.
func (r *Repository) TransitionConversationState(
	ctx context.Context,
	tenantID, conversationID string,
	toState domain.ConversationState,
	expectedVersion int64,
	reason string,
	changedBy string,
) (*domain.Conversation, error) {
	if !toState.IsValid() {
		return nil, domain.ErrInvalidConversationState
	}
	if strings.TrimSpace(changedBy) == "" {
		changedBy = "system"
	}
	if strings.TrimSpace(reason) == "" {
		reason = "state transition requested"
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Busca estado atual com lock FOR UPDATE para concorrência segura
	var conv domain.Conversation
	var statusStr string
	var metaBytes []byte
	selectQuery := `
	SELECT id, tenant_id, COALESCE(user_id, ''), external_id, channel, status, version, resumed_at_sequence, metadata, created_at, updated_at
	FROM conversations
	WHERE tenant_id = $1 AND id = $2
	FOR UPDATE;
	`
	err = tx.QueryRow(ctx, selectQuery, tenantID, conversationID).Scan(
		&conv.ID,
		&conv.TenantID,
		&conv.UserID,
		&conv.ExternalConversationID,
		&conv.Channel,
		&statusStr,
		&conv.Version,
		&conv.ResumedAtSequence,
		&metaBytes,
		&conv.CreatedAt,
		&conv.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("conversation %s not found: %w", conversationID, err)
		}
		return nil, fmt.Errorf("failed to lock conversation for transition: %w", err)
	}

	fromState := domain.ConversationState(statusStr)
	fromVersion := conv.Version

	// 2. Validação da guarda da FSM
	if !domain.CanTransition(fromState, toState) {
		return nil, fmt.Errorf("%w: cannot transition from %s to %s", domain.ErrInvalidStateTransition, fromState, toState)
	}

	// 3. Validação de Concorrência Otimista (State Versioning)
	if expectedVersion > 0 && conv.Version != expectedVersion {
		return nil, fmt.Errorf("%w: expected version %d, but current version is %d", domain.ErrStateVersionMismatch, expectedVersion, conv.Version)
	}

	// Se transição for idempotente (fromState == toState), não precisa alterar nada
	if fromState == toState {
		conv.State = fromState
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &conv.Metadata)
		}
		return &conv, nil
	}

	now := time.Now().UTC()
	nextVersion := conv.Version + 1

	// Se estiver transicionando para HUMAN_ACTIVE ou CLOSED, cancela quaisquer agendamentos pendentes de execução
	if toState == domain.StateHumanActive || toState == domain.StateClosed {
		cancelScheduleQuery := `
		UPDATE conversation_schedules
		SET status = 'CANCELLED', updated_at = $1
		WHERE tenant_id = $2 AND conversation_id = $3 AND status = 'PENDING';
		`
		_, _ = tx.Exec(ctx, cancelScheduleQuery, now, tenantID, conversationID)
	}

	// Se estiver retomando para AI_ACTIVE a partir de HUMAN_ACTIVE ou CLOSED (Fresh Start / Resumption Watermark):
	// define resumed_at_sequence = MAX(sequence_number) para não disparar IA em mensagens passadas (INV-04, INV-06)
	nextResumedSeq := conv.ResumedAtSequence
	if toState == domain.StateAIActive && (fromState == domain.StateHumanActive || fromState == domain.StateClosed) {
		var maxSeq int64
		_ = tx.QueryRow(ctx, "SELECT COALESCE(MAX(sequence_number), 0) FROM messages WHERE tenant_id = $1 AND conversation_id = $2", tenantID, conversationID).Scan(&maxSeq)
		nextResumedSeq = maxSeq
	}

	// 4. Atualiza o estado da conversa, resumed_at_sequence e incrementa versão
	updateQuery := `
	UPDATE conversations
	SET status = $1, version = $2, resumed_at_sequence = $3, updated_at = $4
	WHERE tenant_id = $5 AND id = $6 AND version = $7;
	`
	cmdTag, err := tx.Exec(ctx, updateQuery, string(toState), nextVersion, nextResumedSeq, now, tenantID, conversationID, fromVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to update conversation state: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return nil, domain.ErrStateVersionMismatch
	}

	conv.State = toState
	conv.Version = nextVersion
	conv.ResumedAtSequence = nextResumedSeq
	conv.UpdatedAt = now
	if len(metaBytes) > 0 {
		_ = json.Unmarshal(metaBytes, &conv.Metadata)
	}

	// 5. Registra no histórico de auditoria (conversation_state_history)
	historyID := uuid.New().String()
	historyQuery := `
	INSERT INTO conversation_state_history (
		id, tenant_id, conversation_id, from_state, to_state, reason, changed_by, from_version, to_version, created_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
	);
	`
	_, err = tx.Exec(ctx, historyQuery,
		historyID,
		tenantID,
		conversationID,
		string(fromState),
		string(toState),
		reason,
		changedBy,
		fromVersion,
		nextVersion,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert conversation state history: %w", err)
	}

	// 6. Insere evento de domínio conversation.state.changed na outbox_events
	lifecycleEvt := domain.Event[domain.LifecycleEvent]{
		EventID:        uuid.New().String(),
		TenantID:       tenantID,
		ConversationID: conversationID,
		EventType:      domain.EventTypeConversationStateChanged,
		Version:        domain.EventVersionV1,
		OccurredAt:     now,
		Payload: domain.LifecycleEvent{
			EventID:        uuid.New().String(),
			TenantID:       tenantID,
			ConversationID: conversationID,
			EventType:      domain.LifecycleEventType(domain.EventTypeConversationStateChanged),
			PreviousState:  fromState,
			CurrentState:   toState,
			Version:        nextVersion,
			Reason:         reason,
			OccurredAt:     now,
		},
	}
	payloadBytes, err := json.Marshal(lifecycleEvt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state changed event: %w", err)
	}

	outboxID := uuid.New().String()
	insertOutbox := `
	INSERT INTO outbox_events (
		id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, 'PENDING', 0, $7
	);
	`
	_, err = tx.Exec(ctx, insertOutbox,
		outboxID,
		tenantID,
		conversationID,
		domain.EventTypeConversationStateChanged,
		payloadBytes,
		conversationID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert state changed event into outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transition transaction: %w", err)
	}

	return &conv, nil
}

// GetConversationStateHistory recupera a trilha auditável de transições de uma conversa.
func (r *Repository) GetConversationStateHistory(ctx context.Context, tenantID, conversationID string) ([]domain.ConversationStateHistory, error) {
	query := `
	SELECT id, tenant_id, conversation_id, from_state, to_state, reason, changed_by, from_version, to_version, created_at
	FROM conversation_state_history
	WHERE tenant_id = $1 AND conversation_id = $2
	ORDER BY created_at ASC, to_version ASC;
	`
	rows, err := r.pool.Query(ctx, query, tenantID, conversationID)
	if err != nil {
		return nil, fmt.Errorf("failed to query state history: %w", err)
	}
	defer rows.Close()

	var history []domain.ConversationStateHistory
	for rows.Next() {
		var h domain.ConversationStateHistory
		var fromStr, toStr string
		err := rows.Scan(
			&h.ID,
			&h.TenantID,
			&h.ConversationID,
			&fromStr,
			&toStr,
			&h.Reason,
			&h.ChangedBy,
			&h.FromVersion,
			&h.ToVersion,
			&h.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan state history row: %w", err)
		}
		h.FromState = domain.ConversationState(fromStr)
		h.ToState = domain.ConversationState(toStr)
		history = append(history, h)
	}
	return history, rows.Err()
}

func (r *Repository) GetConversationByID(ctx context.Context, id string) (*domain.Conversation, error) {
	if id == "" {
		return nil, errors.New("conversation id must not be empty")
	}

	query := `
	SELECT id, tenant_id, COALESCE(user_id, ''), external_id, channel, status, version, resumed_at_sequence, metadata, created_at, updated_at
	FROM conversations
	WHERE id = $1;
	`

	var conv domain.Conversation
	var statusStr string
	var metaBytes []byte

	err := r.pool.QueryRow(ctx, query, id).Scan(
		&conv.ID,
		&conv.TenantID,
		&conv.UserID,
		&conv.ExternalConversationID,
		&conv.Channel,
		&statusStr,
		&conv.Version,
		&conv.ResumedAtSequence,
		&metaBytes,
		&conv.CreatedAt,
		&conv.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("conversation %s not found: %w", id, err)
		}
		return nil, fmt.Errorf("failed to query conversation by id: %w", err)
	}

	conv.State = domain.ConversationState(statusStr)
	if len(metaBytes) > 0 {
		_ = json.Unmarshal(metaBytes, &conv.Metadata)
	}

	return &conv, nil
}


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
	RETURNING id, tenant_id, COALESCE(user_id, ''), external_id, channel, status, version, metadata, created_at, updated_at;
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

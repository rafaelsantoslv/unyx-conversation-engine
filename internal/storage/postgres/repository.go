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

func (r *Repository) SaveMessage(ctx context.Context, msg *domain.Message) (*domain.Message, error) {
	if msg == nil {
		return nil, errors.New("message cannot be nil")
	}

	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var convExists bool
	err = tx.QueryRow(ctx, "SELECT true FROM conversations WHERE id = $1 FOR UPDATE", msg.ConversationID).Scan(&convExists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("conversation %s not found: %w", msg.ConversationID, err)
		}
		return nil, fmt.Errorf("failed to lock conversation: %w", err)
	}

	var nextSeq int64
	seqQuery := `SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM messages WHERE conversation_id = $1`
	if err := tx.QueryRow(ctx, seqQuery, msg.ConversationID).Scan(&nextSeq); err != nil {
		return nil, fmt.Errorf("failed to calculate next sequence number: %w", err)
	}
	msg.SequenceNumber = nextSeq

	insertQuery := `
	INSERT INTO messages (id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at;
	`

	var saved domain.Message
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
		&saved.ID,
		&saved.TenantID,
		&saved.ConversationID,
		&saved.ExternalMessageID,
		&roleStr,
		&saved.Content,
		&saved.SequenceNumber,
		&saved.CreatedAt,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = tx.Rollback(ctx)
			existing, fetchErr := r.GetMessageByExternalID(ctx, msg.TenantID, msg.ExternalMessageID)
			if fetchErr == nil {
				return existing, ErrDuplicateMessage
			}
			return nil, ErrDuplicateMessage
		}
		return nil, fmt.Errorf("failed to insert message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	saved.Role = domain.MessageRole(roleStr)
	*msg = saved
	return &saved, nil
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

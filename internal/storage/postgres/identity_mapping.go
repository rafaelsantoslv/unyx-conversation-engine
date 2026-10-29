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

type ConversationChannelMapping struct {
	ID                     string    `json:"id"`
	TenantID               string    `json:"tenant_id"`
	Provider               string    `json:"provider"`
	ConversationID         string    `json:"conversation_id"`
	ExternalAccountID      string    `json:"external_account_id"`
	ExternalInboxID        string    `json:"external_inbox_id"`
	ExternalConversationID string    `json:"external_conversation_id"`
	ExternalContactID      string    `json:"external_contact_id,omitempty"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

type IdentityMappingRepository struct {
	pool *pgxpool.Pool
}

func NewIdentityMappingRepository(pool *pgxpool.Pool) *IdentityMappingRepository {
	return &IdentityMappingRepository{pool: pool}
}

// ResolveOrCreateConversationMapping busca a conversa interna associada aos IDs externos ou cria atomicamente (INV-01, INV-09).
func (r *IdentityMappingRepository) ResolveOrCreateConversationMapping(
	ctx context.Context,
	tenantID, provider string,
	extAccountID, extInboxID, extConvID, extContactID string,
	channel string,
) (*domain.Conversation, error) {
	// 1. Busca mapeamento existente
	selectQuery := `
	SELECT m.conversation_id, c.tenant_id, c.external_id, c.channel, c.status, c.version, c.resumed_at_sequence, c.created_at, c.updated_at
	FROM conversation_channel_mappings m
	INNER JOIN conversations c ON c.id = m.conversation_id
	WHERE m.tenant_id = $1 AND m.provider = $2 AND m.external_conversation_id = $3;
	`
	var conv domain.Conversation
	var statusStr string
	err := r.pool.QueryRow(ctx, selectQuery, tenantID, provider, extConvID).Scan(
		&conv.ID,
		&conv.TenantID,
		&conv.ExternalConversationID,
		&conv.Channel,
		&statusStr,
		&conv.Version,
		&conv.ResumedAtSequence,
		&conv.CreatedAt,
		&conv.UpdatedAt,
	)
	if err == nil {
		conv.State = domain.ConversationState(statusStr)
		return &conv, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("failed to lookup conversation mapping: %w", err)
	}

	// 2. Não existe: Inicia transação atômica para criar Conversation e Mapping
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	newConvID := uuid.New().String()
	now := time.Now().UTC()

	insertConv := `
	INSERT INTO conversations (id, tenant_id, external_id, channel, status, version, created_at, updated_at)
	VALUES ($1, $2, $3, $4, 'AI_ACTIVE', 1, $5, $5)
	ON CONFLICT (tenant_id, external_id) DO UPDATE
	SET updated_at = EXCLUDED.updated_at
	RETURNING id, tenant_id, external_id, channel, status, version, resumed_at_sequence, created_at, updated_at;
	`
	err = tx.QueryRow(ctx, insertConv, newConvID, tenantID, extConvID, channel, now).Scan(
		&conv.ID,
		&conv.TenantID,
		&conv.ExternalConversationID,
		&conv.Channel,
		&statusStr,
		&conv.Version,
		&conv.ResumedAtSequence,
		&conv.CreatedAt,
		&conv.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert/get conversation: %w", err)
	}
	conv.State = domain.ConversationState(statusStr)

	// Cria o mapeamento na tabela dedicada
	mappingID := uuid.New().String()
	insertMapping := `
	INSERT INTO conversation_channel_mappings (
		id, tenant_id, provider, conversation_id, external_account_id, external_inbox_id, external_conversation_id, external_contact_id, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
	ON CONFLICT (tenant_id, provider, external_conversation_id) DO UPDATE
	SET external_contact_id = COALESCE(EXCLUDED.external_contact_id, conversation_channel_mappings.external_contact_id),
	    updated_at = EXCLUDED.updated_at
	RETURNING id;
	`
	var mID string
	err = tx.QueryRow(ctx, insertMapping, mappingID, tenantID, provider, conv.ID, extAccountID, extInboxID, extConvID, extContactID, now).Scan(&mID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert conversation channel mapping: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit conversation mapping tx: %w", err)
	}

	return &conv, nil
}

// GetMappingByConversationID retorna o mapeamento externo de uma conversa interna para despacho de saída.
func (r *IdentityMappingRepository) GetMappingByConversationID(ctx context.Context, tenantID, conversationID string) (*ConversationChannelMapping, error) {
	query := `
	SELECT id, tenant_id, provider, conversation_id, external_account_id, external_inbox_id, external_conversation_id, external_contact_id, created_at, updated_at
	FROM conversation_channel_mappings
	WHERE tenant_id = $1 AND conversation_id = $2
	`
	var m ConversationChannelMapping
	var contactID *string
	err := r.pool.QueryRow(ctx, query, tenantID, conversationID).Scan(
		&m.ID,
		&m.TenantID,
		&m.Provider,
		&m.ConversationID,
		&m.ExternalAccountID,
		&m.ExternalInboxID,
		&m.ExternalConversationID,
		&contactID,
		&m.CreatedAt,
		&m.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if contactID != nil {
		m.ExternalContactID = *contactID
	}
	return &m, nil
}

// SaveMessageMapping associa um message_id interno ao ID externo do canal.
func (r *IdentityMappingRepository) SaveMessageMapping(ctx context.Context, tenantID, provider, messageID, extMessageID string) error {
	mappingID := uuid.New().String()
	now := time.Now().UTC()
	query := `
	INSERT INTO message_channel_mappings (id, tenant_id, provider, message_id, external_message_id, created_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (tenant_id, provider, external_message_id) DO NOTHING;
	`
	_, err := r.pool.Exec(ctx, query, mappingID, tenantID, provider, messageID, extMessageID, now)
	return err
}

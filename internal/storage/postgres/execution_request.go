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

// ExecutionRequestRepository gerencia a persistência e snapshots de execution requests no PostgreSQL (INV-01, INV-04).
type ExecutionRequestRepository struct {
	pool *pgxpool.Pool
}

func NewExecutionRequestRepository(pool *pgxpool.Pool) *ExecutionRequestRepository {
	return &ExecutionRequestRepository{pool: pool}
}

// CreateSnapshotRequest cria uma ExecutionRequest com o snapshot lógico congelado de todas as mensagens
// da conversa até o sequence_number máximo existente no momento (INV-04).
func (r *ExecutionRequestRepository) CreateSnapshotRequest(ctx context.Context, tenantID, conversationID string) (*domain.ExecutionRequest, error) {
	if tenantID == "" || conversationID == "" {
		return nil, errors.New("tenantID and conversationID cannot be empty")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// 1. Busca todas as mensagens da conversa ordenadas por sequence_number
	queryMsgs := `
	SELECT id, sequence_number
	FROM messages
	WHERE tenant_id = $1 AND conversation_id = $2
	ORDER BY sequence_number ASC;
	`

	rows, err := tx.Query(ctx, queryMsgs, tenantID, conversationID)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages for snapshot: %w", err)
	}
	defer rows.Close()

	var messageIDs []string
	var maxSeq int64
	for rows.Next() {
		var id string
		var seq int64
		if err := rows.Scan(&id, &seq); err != nil {
			return nil, fmt.Errorf("failed to scan message for snapshot: %w", err)
		}
		messageIDs = append(messageIDs, id)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading messages for snapshot: %w", err)
	}

	if len(messageIDs) == 0 {
		return nil, fmt.Errorf("cannot create execution request: conversation %s has no messages", conversationID)
	}

	// 2. Determina generation_id (incrementando da última execution_request da conversa, ou 1)
	var nextGeneration int64 = 1
	var lastGen int64
	genQuery := `
	SELECT COALESCE(MAX(generation_id), 0) + 1
	FROM execution_requests
	WHERE conversation_id = $1;
	`
	if err := tx.QueryRow(ctx, genQuery, conversationID).Scan(&lastGen); err == nil && lastGen > 0 {
		nextGeneration = lastGen
	}

	requestID := uuid.New().String()
	now := time.Now().UTC()

	msgIDsJSON, err := json.Marshal(messageIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message ids: %w", err)
	}

	insertQuery := `
	INSERT INTO execution_requests (
		id, tenant_id, conversation_id, generation_id, watermark_sequence, message_ids, status, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, 'PENDING', $7, $7
	)
	RETURNING id, tenant_id, conversation_id, generation_id, watermark_sequence, status, created_at, updated_at;
	`

	var req domain.ExecutionRequest
	var statusStr string
	err = tx.QueryRow(ctx, insertQuery,
		requestID,
		tenantID,
		conversationID,
		nextGeneration,
		maxSeq,
		msgIDsJSON,
		now,
	).Scan(
		&req.ID,
		&req.TenantID,
		&req.ConversationID,
		&req.GenerationID,
		&req.WatermarkSequence,
		&statusStr,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert execution request: %w", err)
	}

	req.Status = domain.ExecutionRequestStatus(statusStr)
	req.MessageIDs = messageIDs

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit execution request transaction: %w", err)
	}

	return &req, nil
}

// GetExecutionRequest busca uma ExecutionRequest pelo ID.
func (r *ExecutionRequestRepository) GetExecutionRequest(ctx context.Context, id string) (*domain.ExecutionRequest, error) {
	query := `
	SELECT id, tenant_id, conversation_id, generation_id, watermark_sequence, message_ids, status, created_at, updated_at
	FROM execution_requests
	WHERE id = $1;
	`

	var req domain.ExecutionRequest
	var statusStr string
	var msgIDsBytes []byte
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&req.ID,
		&req.TenantID,
		&req.ConversationID,
		&req.GenerationID,
		&req.WatermarkSequence,
		&msgIDsBytes,
		&statusStr,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get execution request: %w", err)
	}

	req.Status = domain.ExecutionRequestStatus(statusStr)
	if len(msgIDsBytes) > 0 {
		_ = json.Unmarshal(msgIDsBytes, &req.MessageIDs)
	}
	return &req, nil
}

// FetchMessagesForRequest busca estritamente as mensagens que pertencem ao snapshot da ExecutionRequest,
// ignorando quaisquer mensagens com sequence_number > watermark_sequence (INV-04).
func (r *ExecutionRequestRepository) FetchMessagesForRequest(ctx context.Context, req *domain.ExecutionRequest) ([]*domain.Message, error) {
	if req == nil {
		return nil, errors.New("execution request cannot be nil")
	}

	query := `
	SELECT id, tenant_id, conversation_id, external_id, role, content, sequence_number, created_at
	FROM messages
	WHERE tenant_id = $1 
	  AND conversation_id = $2 
	  AND sequence_number <= $3
	ORDER BY sequence_number ASC;
	`

	rows, err := r.pool.Query(ctx, query, req.TenantID, req.ConversationID, req.WatermarkSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch snapshot messages: %w", err)
	}
	defer rows.Close()

	var messages []*domain.Message
	for rows.Next() {
		var msg domain.Message
		var roleStr string
		err := rows.Scan(
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
			return nil, fmt.Errorf("failed to scan snapshot message: %w", err)
		}
		msg.Role = domain.MessageRole(roleStr)

		// Valida se o ID está contido explicitamente no snapshot
		if req.IncludesMessage(msg.SequenceNumber, msg.ID) {
			messages = append(messages, &msg)
		}
	}

	return messages, rows.Err()
}

// UpdateStatus atualiza o status de uma ExecutionRequest (PENDING -> RUNNING -> COMPLETED/FAILED).
func (r *ExecutionRequestRepository) UpdateStatus(ctx context.Context, id string, status domain.ExecutionRequestStatus) error {
	query := `
	UPDATE execution_requests
	SET status = $1, updated_at = NOW()
	WHERE id = $2;
	`
	_, err := r.pool.Exec(ctx, query, string(status), id)
	return err
}

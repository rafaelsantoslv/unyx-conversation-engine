package domain

import (
	"errors"
	"time"
)

var (
	// ErrConversationBusy indica que outra instância/worker já possui a concessão ativa desta conversa.
	ErrConversationBusy = errors.New("conversation is currently locked by another worker")

	// ErrFencingTokenStale indica que a tentativa de persistência ou renovação foi rejeitada
	// porque outro worker assumiu a execução com uma geração / fencing token maior (INV-04).
	ErrFencingTokenStale = errors.New("fencing token is stale: execution was taken over by another worker")
)

// ConversationExecution representa a concessão distribuída ativa de uma conversa.
type ConversationExecution struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	ConversationID string          `json:"conversation_id"`
	WorkerID       string          `json:"worker_id"`
	Status         ExecutionStatus `json:"status"`
	AcquiredAt     time.Time       `json:"acquired_at"`
	ReleasedAt     *time.Time      `json:"released_at,omitempty"`
	ExpiresAt      time.Time       `json:"expires_at"`
	FencingToken   int64           `json:"fencing_token"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// HasPendingMessagesResult indica se há mensagens pendentes pós-watermark e se uma nova execução foi agendada.
type HasPendingMessagesResult struct {
	HasPending        bool  `json:"has_pending"`
	PendingCount      int   `json:"pending_count"`
	MaxSequenceNumber int64 `json:"max_sequence_number"`
}

package domain

import (
	"errors"
	"time"
)

var (
	// ErrConversationBusy indica que outra instância/worker já possui a concessão ativa desta conversa.
	ErrConversationBusy = errors.New("conversation is currently locked by another worker")
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
}

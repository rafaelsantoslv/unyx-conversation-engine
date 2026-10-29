package domain

import (
	"encoding/json"
	"time"
)

type ExecutionStatus string

const (
	ExecutionStatusPending         ExecutionStatus = "PENDING"
	ExecutionStatusRunning         ExecutionStatus = "RUNNING"
	ExecutionStatusSucceeded       ExecutionStatus = "SUCCEEDED"
	ExecutionStatusFailed          ExecutionStatus = "FAILED"
	ExecutionStatusCancelled       ExecutionStatus = "CANCELLED"
	ExecutionStatusDiscardedStale  ExecutionStatus = "DISCARDED_STALE"
)

func (s ExecutionStatus) IsValid() bool {
	switch s {
	case ExecutionStatusPending, ExecutionStatusRunning, ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusCancelled, ExecutionStatusDiscardedStale:
		return true
	default:
		return false
	}
}

// ProcessingResult representa o registro durável do ciclo de vida de processamento de uma mensagem.
type ProcessingResult struct {
	ID              string          `json:"id"`
	TenantID        string          `json:"tenant_id"`
	ConversationID  string          `json:"conversation_id"`
	MessageID       string          `json:"message_id"`
	SequenceNumber  int64           `json:"sequence_number"`
	Status          ExecutionStatus `json:"status"`
	OutputPayload   json.RawMessage `json:"output_payload,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	ExecutionTimeMS int64           `json:"execution_time_ms"`
	CreatedAt       time.Time       `json:"created_at"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	UpdatedAt       time.Time       `json:"updated_at"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	FencingToken    *int64          `json:"fencing_token,omitempty"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	NextRetryAt     *time.Time      `json:"next_retry_at,omitempty"`
	LastError       *string         `json:"last_error,omitempty"`
	ErrorType       *string         `json:"error_type,omitempty"`
}

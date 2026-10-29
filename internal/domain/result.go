package domain

import (
	"encoding/json"
	"time"
)

type ExecutionStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "RUNNING"
	ExecutionStatusSucceeded ExecutionStatus = "SUCCEEDED"
	ExecutionStatusFailed    ExecutionStatus = "FAILED"
)

func (s ExecutionStatus) IsValid() bool {
	switch s {
	case ExecutionStatusRunning, ExecutionStatusSucceeded, ExecutionStatusFailed:
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
	Status          ExecutionStatus `json:"status"`
	OutputPayload   json.RawMessage `json:"output_payload,omitempty"`
	ErrorMessage    *string         `json:"error_message,omitempty"`
	ExecutionTimeMS int64           `json:"execution_time_ms"`
	CreatedAt       time.Time       `json:"created_at"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
}

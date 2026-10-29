package domain

import (
	"errors"
	"strings"
	"time"
)

type ExecutionRequestStatus string

const (
	ExecutionRequestStatusPending   ExecutionRequestStatus = "PENDING"
	ExecutionRequestStatusRunning   ExecutionRequestStatus = "RUNNING"
	ExecutionRequestStatusCompleted ExecutionRequestStatus = "COMPLETED"
	ExecutionRequestStatusFailed    ExecutionRequestStatus = "FAILED"
)

func (s ExecutionRequestStatus) IsValid() bool {
	switch s {
	case ExecutionRequestStatusPending, ExecutionRequestStatusRunning, ExecutionRequestStatusCompleted, ExecutionRequestStatusFailed:
		return true
	default:
		return false
	}
}

// ExecutionRequest representa um snapshot lógico imutável de entrada para o Agent Runtime (INV-01, INV-04).
type ExecutionRequest struct {
	ID                string                 `json:"id"`
	TenantID          string                 `json:"tenant_id"`
	ConversationID    string                 `json:"conversation_id"`
	GenerationID      int64                  `json:"generation_id"`
	WatermarkSequence int64                  `json:"watermark_sequence"`
	MessageIDs        []string               `json:"message_ids"`
	Status            ExecutionRequestStatus `json:"status"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

var (
	ErrEmptyExecutionRequestID   = errors.New("execution request id cannot be empty")
	ErrInvalidExecutionReqStatus = errors.New("invalid execution request status")
	ErrZeroWatermarkSequence     = errors.New("watermark_sequence must be greater than zero")
	ErrEmptyMessageIDs           = errors.New("message_ids list cannot be empty")
)

func (r *ExecutionRequest) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return ErrEmptyExecutionRequestID
	}
	if strings.TrimSpace(r.TenantID) == "" {
		return ErrEmptyTenantID
	}
	if strings.TrimSpace(r.ConversationID) == "" {
		return ErrEmptyConversationRef
	}
	if !r.Status.IsValid() {
		return ErrInvalidExecutionReqStatus
	}
	if r.WatermarkSequence <= 0 {
		return ErrZeroWatermarkSequence
	}
	if len(r.MessageIDs) == 0 {
		return ErrEmptyMessageIDs
	}
	return nil
}

// IncludesMessage verifica se uma mensagem pertence estritamente ao snapshot lógico
func (r *ExecutionRequest) IncludesMessage(seq int64, msgID string) bool {
	if seq > r.WatermarkSequence {
		return false
	}
	for _, id := range r.MessageIDs {
		if id == msgID {
			return true
		}
	}
	return false
}

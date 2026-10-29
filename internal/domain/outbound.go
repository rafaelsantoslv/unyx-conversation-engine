package domain

import (
	"encoding/json"
	"time"
)

type OutboundIntentStatus string

const (
	OutboundStatusPending   OutboundIntentStatus = "PENDING"
	OutboundStatusSending   OutboundIntentStatus = "SENDING"
	OutboundStatusSent      OutboundIntentStatus = "SENT"
	OutboundStatusFailed    OutboundIntentStatus = "FAILED"
	OutboundStatusCancelled OutboundIntentStatus = "CANCELLED"
	OutboundStatusUnknown   OutboundIntentStatus = "UNKNOWN"
)

type OutboundPayload struct {
	Content        string            `json:"content"`
	RecipientID    string            `json:"recipient_id,omitempty"`
	Channel        string            `json:"channel,omitempty"`
	TokensUsed     int               `json:"tokens_used,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type OutboundIntent struct {
	ID                string               `json:"id"`
	TenantID          string               `json:"tenant_id"`
	ConversationID    string               `json:"conversation_id"`
	ExecutionID       string               `json:"execution_id"`
	Payload           OutboundPayload      `json:"payload"`
	RawPayload        json.RawMessage      `json:"raw_payload,omitempty"`
	Status            OutboundIntentStatus `json:"status"`
	Attempts          int                  `json:"attempts"`
	MaxAttempts       int                  `json:"max_attempts"`
	NextRetryAt       *time.Time           `json:"next_retry_at,omitempty"`
	ExternalMessageID *string              `json:"external_message_id,omitempty"`
	ErrorMessage      *string              `json:"error_message,omitempty"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
}

package domain

import (
	"encoding/json"
	"time"
)

// QuarantinedEvent representa um evento isolado na tabela de quarentena (Dead Letter)
// após erro irrecuperável (poison pill) ou esgotamento de retentativas (INV-06, INV-08).
type QuarantinedEvent struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	ConversationID string          `json:"conversation_id"`
	EventID        string          `json:"event_id"`
	Payload        json.RawMessage `json:"payload"`
	ErrorReason    string          `json:"error_reason"`
	StackTrace     string          `json:"stack_trace,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	QuarantinedAt  time.Time       `json:"quarantined_at"`
	ResolvedAt     *time.Time      `json:"resolved_at,omitempty"`
}

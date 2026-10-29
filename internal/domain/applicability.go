package domain

import (
	"errors"
	"time"
)

var (
	// ErrResultSuperseded indica que a computação de IA foi descartada porque a conversa não está mais em AI_ACTIVE
	// ou a versão da conversa divergiu (ex: operador humano assumiu ou conversa fechou).
	ErrResultSuperseded = errors.New("result superseded: conversation state or version changed during computation")
)

// ApplicabilityCheckResult resume o resultado da validação atômica de atualidade.
type ApplicabilityCheckResult struct {
	IsApplicable bool              `json:"is_applicable"`
	FinalStatus  ExecutionStatus   `json:"final_status"`
	Reason       string            `json:"reason,omitempty"`
	CurrentState ConversationState `json:"current_state"`
	CurrentVersion int64           `json:"current_version"`
}

// OutboundIntentPayload representa o payload do evento gerado para envio ao canal externo (ex: WhatsApp).
type OutboundIntentPayload struct {
	MessageID      string    `json:"message_id"`
	ConversationID string    `json:"conversation_id"`
	TenantID       string    `json:"tenant_id"`
	Content        string    `json:"content"`
	GeneratedAt    time.Time `json:"generated_at"`
	TokensUsed     int       `json:"tokens_used"`
}

const (
	EventTypeOutboundIntent = "message.outbound.intent"
)

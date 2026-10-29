package domain

import (
	"context"
	"time"
)

// ExecutionInput contém os dados necessários para execução do agente.
type ExecutionInput struct {
	ExecutionRequestID string            `json:"execution_request_id,omitempty"`
	TenantID           string            `json:"tenant_id"`
	ConversationID     string            `json:"conversation_id"`
	MessageID          string            `json:"message_id"`
	WatermarkSequence  int64             `json:"watermark_sequence,omitempty"`
	SnapshotMessages   []*Message        `json:"snapshot_messages,omitempty"`
	Content            string            `json:"content"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// ExecutionOutput contém a resposta gerada pelo runtime de execução do agente.
type ExecutionOutput struct {
	ResponseContent string            `json:"response_content"`
	TokensUsed      int               `json:"tokens_used,omitempty"`
	ExecutedAt      time.Time         `json:"executed_at"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// AgentRuntime define o contrato para execução de lógica/raciocínio de agentes de inteligência artificial.
type AgentRuntime interface {
	Execute(ctx context.Context, input ExecutionInput) (*ExecutionOutput, error)
}

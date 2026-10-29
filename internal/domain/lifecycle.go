package domain

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidStateTransition indica que a transição de estado da conversa solicitada viola a máquina de estados.
	ErrInvalidStateTransition = errors.New("invalid conversation state transition")

	// ErrStateVersionMismatch indica conflito de concorrência otimista ao tentar alterar o estado.
	ErrStateVersionMismatch = errors.New("conversation state version mismatch")

	// ErrAIProcessingNotAllowed indica que a conversa não está em estado elegível para processamento de IA.
	ErrAIProcessingNotAllowed = errors.New("ai processing is not allowed in current conversation state")
)

// AllowedTransitions mapeia a máquina de estados formal e determinística de uma conversa (INV-06).
// AI_ACTIVE:
//   -> HUMAN_ACTIVE (transbordo / handoff)
//   -> CLOSED (encerramento do atendimento)
// HUMAN_ACTIVE:
//   -> AI_ACTIVE (retorno para automação / devolução)
//   -> CLOSED (resolução pelo operador humano)
// CLOSED:
//   -> AI_ACTIVE (reabertura pelo cliente via nova mensagem ou comando explícito)
//   -> HUMAN_ACTIVE (reabertura direta para operador)
var AllowedTransitions = map[ConversationState][]ConversationState{
	StateAIActive: {
		StateHumanActive,
		StateClosed,
	},
	StateHumanActive: {
		StateAIActive,
		StateClosed,
	},
	StateClosed: {
		StateAIActive,
		StateHumanActive,
	},
}

// CanTransition verifica se uma transição de fromState para toState é permitida pelas regras de domínio.
func CanTransition(fromState, toState ConversationState) bool {
	if fromState == toState {
		return true // No-op idempotente
	}
	allowed, exists := AllowedTransitions[fromState]
	if !exists {
		return false
	}
	for _, target := range allowed {
		if target == toState {
			return true
		}
	}
	return false
}

// TransitionTo avalia e aplica a transição de estado na conversa de forma pura e auditada.
func (c *Conversation) TransitionTo(newState ConversationState) error {
	if !CanTransition(c.State, newState) {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, c.State, newState)
	}
	if c.State != newState {
		c.State = newState
		c.Version++
		c.UpdatedAt = time.Now().UTC()
	}
	return nil
}

// AllowsAIProcessing determina se o estado atual da conversa autoriza a invocação do Agent Runtime.
// Apenas conversas em AI_ACTIVE podem ser atendidas por IA.
func (c *Conversation) AllowsAIProcessing() bool {
	return c.State == StateAIActive
}

// LifecycleCommandType tipa os comandos de intenção de ciclo de vida.
type LifecycleCommandType string

const (
	CommandRequestHandoff   LifecycleCommandType = "REQUEST_HANDOFF"
	CommandReturnToAI       LifecycleCommandType = "RETURN_TO_AI"
	CommandCloseConversation LifecycleCommandType = "CLOSE_CONVERSATION"
	CommandReopenConversation LifecycleCommandType = "REOPEN_CONVERSATION"
)

// LifecycleCommand expressa a intenção explícita de transição de estado.
type LifecycleCommand struct {
	CommandID        string               `json:"command_id"`
	TenantID         string               `json:"tenant_id"`
	ConversationID   string               `json:"conversation_id"`
	Type             LifecycleCommandType `json:"type"`
	TargetState      ConversationState    `json:"target_state"`
	ExpectedVersion  int64                `json:"expected_version"`
	Reason           string               `json:"reason"`
	Initiator        string               `json:"initiator"` // user, agent, operator, system
	Metadata         map[string]string    `json:"metadata,omitempty"`
	RequestedAt      time.Time            `json:"requested_at"`
}

// LifecycleEventType tipa os eventos de ciclo de vida já consumados.
type LifecycleEventType string

// ConversationStateHistory representa o registro auditável e imutável de transição de estado da conversa.
type ConversationStateHistory struct {
	ID             string            `json:"id"`
	TenantID       string            `json:"tenant_id"`
	ConversationID string            `json:"conversation_id"`
	FromState      ConversationState `json:"from_state"`
	ToState        ConversationState `json:"to_state"`
	Reason         string            `json:"reason"`
	ChangedBy      string            `json:"changed_by"`
	FromVersion    int64             `json:"from_version"`
	ToVersion      int64             `json:"to_version"`
	CreatedAt      time.Time         `json:"created_at"`
}

const (
	EventTypeConversationStateChanged = "conversation.state.changed"
)

// LifecycleEvent registra a transição de estado consumada no motor.
type LifecycleEvent struct {
	EventID        string             `json:"event_id"`
	TenantID       string             `json:"tenant_id"`
	ConversationID string             `json:"conversation_id"`
	EventType      LifecycleEventType `json:"event_type"`
	PreviousState  ConversationState  `json:"previous_state"`
	CurrentState   ConversationState  `json:"current_state"`
	Version        int64              `json:"version"`
	Reason         string             `json:"reason"`
	OccurredAt     time.Time          `json:"occurred_at"`
}

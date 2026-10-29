package service

import (
	"context"
	"errors"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var (
	ErrConversationNotFound = errors.New("conversation not found")
)

// HandoffStateRepository define os métodos necessários para transição de estado da FSM e cancelamento de agendamentos.
type HandoffStateRepository interface {
	TransitionConversationState(
		ctx context.Context,
		tenantID, conversationID string,
		toState domain.ConversationState,
		expectedVersion int64,
		reason string,
		changedBy string,
	) (*domain.Conversation, error)
	GetConversationStateHistory(ctx context.Context, tenantID, conversationID string) ([]domain.ConversationStateHistory, error)
}

// HandoffService gerencia transbordo para operadores humanos e retomada para IA (INV-04, INV-06, INV-07).
type HandoffService struct {
	repo HandoffStateRepository
}

func NewHandoffService(repo HandoffStateRepository) *HandoffService {
	return &HandoffService{repo: repo}
}

// RequestHandoffToHuman transfere o atendimento para um operador humano (HUMAN_ACTIVE),
// cancela agendamentos pendentes da IA e impede execuções concorrentes automáticas (INV-06).
func (s *HandoffService) RequestHandoffToHuman(ctx context.Context, tenantID, conversationID string, expectedVersion int64, reason, operatorID string) (*domain.Conversation, error) {
	if reason == "" {
		reason = "Handoff to human operator requested"
	}
	if operatorID == "" {
		operatorID = "operator"
	}

	return s.repo.TransitionConversationState(
		ctx,
		tenantID,
		conversationID,
		domain.StateHumanActive,
		expectedVersion,
		reason,
		operatorID,
	)
}

// ResumeAIAttendance devolve o atendimento para a IA (AI_ACTIVE).
// Registra o watermark de retomada (resumed_at_sequence = MAX(sequence_number)),
// garantindo que mensagens trocadas durante o período humano fiquem apenas como histórico/contexto passivo
// e NÃO disparem execuções retroativas da IA (INV-04, INV-06).
func (s *HandoffService) ResumeAIAttendance(ctx context.Context, tenantID, conversationID string, expectedVersion int64, reason, operatorID string) (*domain.Conversation, error) {
	if reason == "" {
		reason = "AI attendance resumed by operator"
	}
	if operatorID == "" {
		operatorID = "operator"
	}

	return s.repo.TransitionConversationState(
		ctx,
		tenantID,
		conversationID,
		domain.StateAIActive,
		expectedVersion,
		reason,
		operatorID,
	)
}

// CloseConversation encerra o atendimento (CLOSED).
func (s *HandoffService) CloseConversation(ctx context.Context, tenantID, conversationID string, expectedVersion int64, reason, initiator string) (*domain.Conversation, error) {
	if reason == "" {
		reason = "Conversation closed"
	}
	if initiator == "" {
		initiator = "system"
	}

	return s.repo.TransitionConversationState(
		ctx,
		tenantID,
		conversationID,
		domain.StateClosed,
		expectedVersion,
		reason,
		initiator,
	)
}

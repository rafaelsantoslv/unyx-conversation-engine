package chatwoot

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
)

type StateSyncRepository interface {
	GetOrCreateConversation(ctx context.Context, tenantID, externalID string) (*domain.Conversation, error)
	TransitionConversationState(
		ctx context.Context,
		tenantID, conversationID string,
		toState domain.ConversationState,
		expectedVersion int64,
		reason string,
		changedBy string,
	) (*domain.Conversation, error)
}

type IdentityResolver interface {
	ResolveOrCreateConversationMapping(
		ctx context.Context,
		tenantID, provider string,
		extAccountID, extInboxID, extConvID, extContactID string,
		channel string,
	) (*domain.Conversation, error)
}

// StateSyncService sincroniza eventos de ciclo de vida do Chatwoot com a FSM do Engine (INV-05, INV-06, INV-07).
type StateSyncService struct {
	stateRepo        StateSyncRepository
	identityResolver IdentityResolver
}

func NewStateSyncService(stateRepo StateSyncRepository, identityResolver IdentityResolver) *StateSyncService {
	return &StateSyncService{
		stateRepo:        stateRepo,
		identityResolver: identityResolver,
	}
}

// SyncChatwootEvent avalia eventos como conversation_status_changed ou conversation_updated e transiciona a conversa.
func (s *StateSyncService) SyncChatwootEvent(ctx context.Context, tenantID string, payload *cwDto.ChatwootWebhookPayload) (*domain.Conversation, error) {
	if payload == nil || payload.Conversation == nil {
		return nil, nil
	}

	extConvID := fmt.Sprintf("%d", payload.Conversation.ID)
	var extAccountID string
	if payload.Account != nil {
		extAccountID = fmt.Sprintf("%d", payload.Account.ID)
	}
	var extInboxID string
	if payload.Inbox != nil {
		extInboxID = fmt.Sprintf("%d", payload.Inbox.ID)
	}
	var extContactID string
	if payload.Contact != nil {
		extContactID = fmt.Sprintf("%d", payload.Contact.ID)
	}

	// 1. Resolve conversa interna
	var conv *domain.Conversation
	var err error
	if s.identityResolver != nil {
		conv, err = s.identityResolver.ResolveOrCreateConversationMapping(
			ctx, tenantID, "chatwoot", extAccountID, extInboxID, extConvID, extContactID, "whatsapp",
		)
	} else if s.stateRepo != nil {
		conv, err = s.stateRepo.GetOrCreateConversation(ctx, tenantID, extConvID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to resolve conversation for state sync: %w", err)
	}
	if conv == nil {
		return nil, nil
	}

	status := strings.ToLower(strings.TrimSpace(payload.Conversation.Status))
	assigneeID := payload.Conversation.AssigneeID

	targetState := conv.State
	reason := ""
	operator := "chatwoot-system"
	if payload.Sender != nil && payload.Sender.Name != "" {
		operator = payload.Sender.Name
	}

	// 2. Mapeamento de Regras:
	// A. Se atribuído a um atendente humano (assignee != 0) ou status "open" com intervenção humana:
	if assigneeID > 0 {
		targetState = domain.StateHumanActive
		reason = fmt.Sprintf("Assigned to human operator (id=%d) in Chatwoot", assigneeID)
	} else if status == "resolved" {
		// B. Conversa resolvida/fechada no Chatwoot:
		targetState = domain.StateClosed
		reason = "Conversation resolved in Chatwoot"
	} else if status == "open" && conv.State == domain.StateClosed {
		// C. Conversa reaberta
		targetState = domain.StateAIActive
		reason = "Conversation reopened in Chatwoot"
	}

	// 3. Aplica transição se o estado for diferente do atual
	if targetState != conv.State && s.stateRepo != nil {
		updatedConv, err := s.stateRepo.TransitionConversationState(
			ctx, tenantID, conv.ID, targetState, conv.Version, reason, operator,
		)
		if err != nil {
			log.Printf("[state-sync] warning: failed to transition conversation %s to %s: %v", conv.ID, targetState, err)
			return nil, err
		}
		log.Printf("[state-sync] conversation %s transitioned to %s via Chatwoot event (reason: %s)", conv.ID, targetState, reason)
		return updatedConv, nil
	}

	return conv, nil
}

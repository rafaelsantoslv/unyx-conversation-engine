package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transporthttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
)

type ConversationRepository interface {
	GetOrCreateConversation(ctx context.Context, tenantID, externalID string) (*domain.Conversation, error)
	SaveMessageWithOutbox(ctx context.Context, msg *domain.Message, evt *domain.OutboxEvent) (*domain.Message, error)
}

type IngestionService struct {
	repo ConversationRepository
}

func NewIngestionService(repo ConversationRepository) *IngestionService {
	return &IngestionService{repo: repo}
}

func (s *IngestionService) IngestMessage(ctx context.Context, req dto.IngestMessageRequest, messageID string) error {
	conv, err := s.repo.GetOrCreateConversation(ctx, req.TenantID, req.ConversationID)
	if err != nil {
		return fmt.Errorf("falha ao obter ou criar conversa: %w", err)
	}

	msg := &domain.Message{
		ID:                messageID,
		TenantID:          req.TenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: req.ExternalMessageID,
		Role:              domain.MessageRole(req.Role),
		Content:           req.Content,
		CreatedAt:         time.Now().UTC(),
	}

	eventPayload := domain.Event[domain.MessageReceivedPayload]{
		EventID:        transporthttp.RequestIDFromContext(ctx),
		TenantID:       req.TenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     msg.CreatedAt,
		Payload: domain.MessageReceivedPayload{
			MessageID:         msg.ID,
			ExternalMessageID: msg.ExternalMessageID,
			Channel:           req.Channel,
			Role:              msg.Role,
			Content:           msg.Content,
		},
	}

	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("falha ao serializar payload do outbox: %w", err)
	}

	outboxEvt := &domain.OutboxEvent{
		TenantID:       req.TenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Payload:        payloadBytes,
		PartitionKey:   fmt.Sprintf("%s:%s", req.TenantID, conv.ID),
		Status:         domain.OutboxStatusPending,
		CreatedAt:      msg.CreatedAt,
	}

	_, err = s.repo.SaveMessageWithOutbox(ctx, msg, outboxEvt)
	if err != nil {
		if errors.Is(err, postgres.ErrDuplicateMessage) {
			return nil
		}
		return err
	}

	return nil
}

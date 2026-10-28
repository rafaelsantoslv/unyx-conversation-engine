package service_test

import (
	"context"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
)

type mockRepo struct {
	conv    *domain.Conversation
	saved   *domain.Message
	outbox  *domain.OutboxEvent
	convErr error
	saveErr error
}

func (m *mockRepo) GetOrCreateConversation(ctx context.Context, tenantID, externalID string) (*domain.Conversation, error) {
	if m.convErr != nil {
		return nil, m.convErr
	}
	return m.conv, nil
}

func (m *mockRepo) SaveMessageWithOutbox(ctx context.Context, msg *domain.Message, evt *domain.OutboxEvent) (*domain.Message, error) {
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	m.saved = msg
	m.outbox = evt
	return msg, nil
}

func TestIngestionService_IngestMessage(t *testing.T) {
	conv := &domain.Conversation{
		ID:                     "conv-uuid-1",
		TenantID:               "tenant-1",
		ExternalConversationID: "ext-conv-1",
		State:                  domain.StateAIActive,
	}

	req := dto.IngestMessageRequest{
		TenantID:          "tenant-1",
		ConversationID:    "ext-conv-1",
		ExternalMessageID: "ext-msg-1",
		Channel:           "whatsapp",
		Role:              "user",
		Content:           "Olá, teste!",
	}

	t.Run("successfully persists message and outbox", func(t *testing.T) {
		repo := &mockRepo{conv: conv}
		svc := service.NewIngestionService(repo)

		err := svc.IngestMessage(context.Background(), req, "msg-uuid-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if repo.saved == nil || repo.outbox == nil {
			t.Fatalf("expected both message and outbox to be passed to repo")
		}
		if repo.saved.ConversationID != conv.ID {
			t.Errorf("expected conversation_id %s, got %s", conv.ID, repo.saved.ConversationID)
		}
		if repo.outbox.PartitionKey != conv.ID {
			t.Errorf("expected partition key %s, got %s", conv.ID, repo.outbox.PartitionKey)
		}
		if repo.outbox.Status != domain.OutboxStatusPending {
			t.Errorf("expected status PENDING, got %s", repo.outbox.Status)
		}
	})

	t.Run("handles duplicate message idempotently without error", func(t *testing.T) {
		repo := &mockRepo{
			conv:    conv,
			saveErr: postgres.ErrDuplicateMessage,
		}
		svc := service.NewIngestionService(repo)

		err := svc.IngestMessage(context.Background(), req, "msg-uuid-1")
		if err != nil {
			t.Fatalf("expected duplicate message to be treated idempotently without error, got %v", err)
		}
	})
}

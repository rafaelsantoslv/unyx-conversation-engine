package service_test

import (
	"context"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHandoffRepo struct {
	conv    *domain.Conversation
	history []domain.ConversationStateHistory
	err     error
}

func (m *mockHandoffRepo) TransitionConversationState(
	ctx context.Context,
	tenantID, conversationID string,
	toState domain.ConversationState,
	expectedVersion int64,
	reason string,
	changedBy string,
) (*domain.Conversation, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.conv.State = toState
	m.conv.Version++
	m.history = append(m.history, domain.ConversationStateHistory{
		TenantID:       tenantID,
		ConversationID: conversationID,
		FromState:      m.conv.State,
		ToState:        toState,
		Reason:         reason,
		ChangedBy:      changedBy,
		ToVersion:      m.conv.Version,
	})
	return m.conv, nil
}

func (m *mockHandoffRepo) GetConversationStateHistory(ctx context.Context, tenantID, conversationID string) ([]domain.ConversationStateHistory, error) {
	return m.history, nil
}

func TestHandoffService(t *testing.T) {
	ctx := context.Background()

	mockRepo := &mockHandoffRepo{
		conv: &domain.Conversation{
			ID:       "conv-123",
			TenantID: "tenant-1",
			State:    domain.StateAIActive,
			Version:  1,
		},
	}

	svc := service.NewHandoffService(mockRepo)

	t.Run("handoff to human", func(t *testing.T) {
		res, err := svc.RequestHandoffToHuman(ctx, "tenant-1", "conv-123", 1, "Client requested human", "operator-1")
		require.NoError(t, err)
		assert.Equal(t, domain.StateHumanActive, res.State)
	})

	t.Run("resume ai attendance", func(t *testing.T) {
		res, err := svc.ResumeAIAttendance(ctx, "tenant-1", "conv-123", 2, "Returned to bot", "operator-1")
		require.NoError(t, err)
		assert.Equal(t, domain.StateAIActive, res.State)
	})

	t.Run("close conversation", func(t *testing.T) {
		res, err := svc.CloseConversation(ctx, "tenant-1", "conv-123", 3, "Issue resolved", "operator-1")
		require.NoError(t, err)
		assert.Equal(t, domain.StateClosed, res.State)
	})
}

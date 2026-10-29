package handler_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockWebhookAuthValidator struct {
	validToken string
}

func (m *mockWebhookAuthValidator) ValidateWebhookToken(ctx context.Context, tenantID, channel, token string) (bool, error) {
	return token == m.validToken, nil
}

type mockWebhookIngestionService struct {
	ingested []dto.IngestMessageRequest
}

func (m *mockWebhookIngestionService) IngestMessage(ctx context.Context, req dto.IngestMessageRequest, messageID string) error {
	m.ingested = append(m.ingested, req)
	return nil
}

func TestChatwootWebhookHandler_AuthenticationAndIngestion(t *testing.T) {
	mockAuth := &mockWebhookAuthValidator{validToken: "secret-token-123"}
	mockIngestion := &mockWebhookIngestionService{}
	webhookHandler := handler.NewChatwootWebhookHandler(mockIngestion, mockAuth)

	payloadJSON := `{
		"event": "message_created",
		"id": 98765,
		"content": "Olá via Chatwoot Webhook!",
		"message_type": "incoming",
		"content_type": "text",
		"sender": {
			"id": 100,
			"name": "Cliente Teste",
			"type": "contact"
		},
		"conversation": {
			"id": 4321,
			"inbox_id": 5,
			"status": "open"
		},
		"inbox": {
			"id": 5,
			"name": "WhatsApp",
			"channel_type": "Channel::Whatsapp"
		}
	}`

	t.Run("Rejects request with invalid or missing token (401)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/integrations/chatwoot/webhooks/tenant-a", bytes.NewBufferString(payloadJSON))
		w := httptest.NewRecorder()

		webhookHandler.HandleWebhook(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("Accepts request with valid token via header and ingests message (200)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/integrations/chatwoot/webhooks/tenant-a", bytes.NewBufferString(payloadJSON))
		req.Header.Set("X-Chatwoot-Webhook-Token", "secret-token-123")
		w := httptest.NewRecorder()

		webhookHandler.HandleWebhook(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		require.Len(t, mockIngestion.ingested, 1)
		ing := mockIngestion.ingested[0]
		assert.Equal(t, "tenant-a", ing.TenantID)
		assert.Equal(t, "4321", ing.ConversationID)
		assert.Equal(t, "98765", ing.ExternalMessageID)
		assert.Equal(t, "Olá via Chatwoot Webhook!", ing.Content)
		assert.Equal(t, "whatsapp", ing.Channel)
	})

	t.Run("Accepts valid token via query parameter (200)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/integrations/chatwoot/webhooks/tenant-a?token=secret-token-123", bytes.NewBufferString(payloadJSON))
		w := httptest.NewRecorder()

		webhookHandler.HandleWebhook(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Ignores echoes/outgoing messages without error (200)", func(t *testing.T) {
		echoJSON := `{
			"event": "message_created",
			"id": 98766,
			"content": "Resposta do bot",
			"message_type": "outgoing",
			"sender": {
				"id": 1,
				"name": "Bot",
				"type": "agent_bot"
			},
			"conversation": {
				"id": 4321
			}
		}`
		countBefore := len(mockIngestion.ingested)

		req := httptest.NewRequest(http.MethodPost, "/v1/integrations/chatwoot/webhooks/tenant-a", bytes.NewBufferString(echoJSON))
		req.Header.Set("X-Chatwoot-Webhook-Token", "secret-token-123")
		w := httptest.NewRecorder()

		webhookHandler.HandleWebhook(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "echo_loop_prevented")
		assert.Equal(t, countBefore, len(mockIngestion.ingested), "Echoes must NOT be ingested into engine pipeline")
	})

	t.Run("Ignores non-actionable administrative events (200)", func(t *testing.T) {
		adminJSON := `{
			"event": "conversation_assigned",
			"id": 4321,
			"conversation": {
				"id": 4321
			}
		}`
		countBefore := len(mockIngestion.ingested)

		req := httptest.NewRequest(http.MethodPost, "/v1/integrations/chatwoot/webhooks/tenant-a", bytes.NewBufferString(adminJSON))
		req.Header.Set("X-Chatwoot-Webhook-Token", "secret-token-123")
		w := httptest.NewRecorder()

		webhookHandler.HandleWebhook(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "non_actionable_event")
		assert.Equal(t, countBefore, len(mockIngestion.ingested))
	})
}

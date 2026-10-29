package chatwoot_test

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/stretchr/testify/assert"
)

func TestEventClassifier_LoopPreventionMatrix(t *testing.T) {
	ctx := context.Background()
	tracker := chatwoot.NewInMemoryEchoTracker(5 * time.Minute)
	classifier := chatwoot.NewEventClassifier(tracker)
	tenantID := "tenant-test"

	t.Run("Classifies incoming contact message as CLIENT_MESSAGE (eligible for AI)", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			ID:          1001,
			Content:     "Quero agendar uma reunião",
			MessageType: "incoming",
			Sender: &cwDto.ChatwootSender{
				ID:   50,
				Name: "Cliente João",
				Type: "contact",
			},
		}

		class, reason := classifier.ClassifyMessage(ctx, tenantID, payload)
		assert.Equal(t, chatwoot.ClassificationClientMessage, class)
		assert.Equal(t, "client_inbound_message", reason)
	})

	t.Run("Classifies outgoing bot message as BOT_ECHO (prevents feedback loop)", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			ID:          1002,
			Content:     "Olá João, temos horários disponíveis!",
			MessageType: "outgoing",
			Sender: &cwDto.ChatwootSender{
				ID:   1,
				Name: "Bot IA",
				Type: "agent_bot",
			},
		}

		class, reason := classifier.ClassifyMessage(ctx, tenantID, payload)
		assert.Equal(t, chatwoot.ClassificationBotEcho, class)
		assert.Equal(t, "outgoing_bot_echo", reason)
	})

	t.Run("Classifies outgoing human agent message as HUMAN_MESSAGE", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			ID:          1003,
			Content:     "Oi João, sou o atendente humano Pedro",
			MessageType: "outgoing",
			Sender: &cwDto.ChatwootSender{
				ID:   200,
				Name: "Pedro Atendente",
				Type: "user",
			},
		}

		class, reason := classifier.ClassifyMessage(ctx, tenantID, payload)
		assert.Equal(t, chatwoot.ClassificationHumanMessage, class)
		assert.Equal(t, "human_operator_reply", reason)
	})

	t.Run("Classifies message matching registered sent intent as BOT_ECHO even if metadata is ambiguous", func(t *testing.T) {
		extMsgID := "1004"
		tracker.RegisterSentMessage(ctx, tenantID, extMsgID, "corr-xyz", 1*time.Minute)

		payload := &cwDto.ChatwootWebhookPayload{
			ID:          1004,
			Content:     "Resposta enviada pelo engine",
			MessageType: "incoming", // supondo que Chatwoot enviou incoming por erro de canal
			Sender: &cwDto.ChatwootSender{
				ID:   50,
				Type: "contact",
			},
		}

		class, reason := classifier.ClassifyMessage(ctx, tenantID, payload)
		assert.Equal(t, chatwoot.ClassificationBotEcho, class)
		assert.Equal(t, "matched_sent_intent_cache", reason)
	})

	t.Run("Ignores private team internal notes", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			ID:          1005,
			Content:     "Nota interna: cliente é VIP",
			Private:     true,
			MessageType: "outgoing",
		}

		class, reason := classifier.ClassifyMessage(ctx, tenantID, payload)
		assert.Equal(t, chatwoot.ClassificationIgnored, class)
		assert.Equal(t, "private_note", reason)
	})
}

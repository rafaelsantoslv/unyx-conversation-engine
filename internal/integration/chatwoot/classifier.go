package chatwoot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
)

// MessageClassification categoriza a mensagem recebida para prevenir loops e decidir ações (INV-03, INV-05).
type MessageClassification string

const (
	ClassificationClientMessage MessageClassification = "CLIENT_MESSAGE" // Mensagem vinda do cliente final -> elegível para IA
	ClassificationHumanMessage  MessageClassification = "HUMAN_MESSAGE"  // Mensagem enviada pelo operador humano -> registra no histórico, não chama IA
	ClassificationBotEcho       MessageClassification = "BOT_ECHO"       // Mensagem originada pelo próprio bot/Engine -> descarta para evitar loop
	ClassificationIgnored       MessageClassification = "IGNORED"        // Mensagem privada, evento irrelevante ou não suportado
)

// OutboundIntentTracker rastreia correlation_ids ou external_message_ids recentemente enviados pelo Engine.
type OutboundIntentTracker interface {
	IsOutboundEcho(ctx context.Context, tenantID, externalMessageID, correlationID string) (bool, error)
	RegisterSentMessage(ctx context.Context, tenantID, externalMessageID, correlationID string, ttl time.Duration)
}

// InMemoryEchoTracker implementa OutboundIntentTracker com cache LRU / TTL em memória seguro para concorrência.
type InMemoryEchoTracker struct {
	mu      sync.RWMutex
	entries map[string]time.Time
	ttl     time.Duration
}

func NewInMemoryEchoTracker(ttl time.Duration) *InMemoryEchoTracker {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	t := &InMemoryEchoTracker{
		entries: make(map[string]time.Time),
		ttl:     ttl,
	}
	go t.cleanupLoop()
	return t
}

func (t *InMemoryEchoTracker) RegisterSentMessage(ctx context.Context, tenantID, externalMessageID, correlationID string, ttl time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	expiry := time.Now().Add(ttl)
	if externalMessageID != "" {
		t.entries[fmt.Sprintf("%s:msg:%s", tenantID, externalMessageID)] = expiry
	}
	if correlationID != "" {
		t.entries[fmt.Sprintf("%s:corr:%s", tenantID, correlationID)] = expiry
	}
}

func (t *InMemoryEchoTracker) IsOutboundEcho(ctx context.Context, tenantID, externalMessageID, correlationID string) (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := time.Now()

	if externalMessageID != "" {
		if exp, exists := t.entries[fmt.Sprintf("%s:msg:%s", tenantID, externalMessageID)]; exists && exp.After(now) {
			return true, nil
		}
	}
	if correlationID != "" {
		if exp, exists := t.entries[fmt.Sprintf("%s:corr:%s", tenantID, correlationID)]; exists && exp.After(now) {
			return true, nil
		}
	}
	return false, nil
}

func (t *InMemoryEchoTracker) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		t.mu.Lock()
		now := time.Now()
		for k, v := range t.entries {
			if !v.After(now) {
				delete(t.entries, k)
			}
		}
		t.mu.Unlock()
	}
}

// EventClassifier classifica eventos inbound do Chatwoot com prevenção matemática de loops (INV-03).
type EventClassifier struct {
	echoTracker OutboundIntentTracker
}

func NewEventClassifier(tracker OutboundIntentTracker) *EventClassifier {
	if tracker == nil {
		tracker = NewInMemoryEchoTracker(5 * time.Minute)
	}
	return &EventClassifier{echoTracker: tracker}
}

// ClassifyMessage avalia message_type, sender.type, private e histórico de outbound para prevenir loops.
func (c *EventClassifier) ClassifyMessage(ctx context.Context, tenantID string, payload *cwDto.ChatwootWebhookPayload) (MessageClassification, string) {
	if payload == nil {
		return ClassificationIgnored, "empty_payload"
	}

	// 1. Mensagens privadas (notas internas entre operadores humanos no painel do Chatwoot)
	if payload.Private {
		return ClassificationIgnored, "private_note"
	}

	extMsgID := fmt.Sprintf("%d", payload.ID)
	var senderType string
	if payload.Sender != nil {
		senderType = strings.ToLower(strings.TrimSpace(payload.Sender.Type))
	}

	// 2. Verificação de Echo no tracker de saída (outbound intents)
	if c.echoTracker != nil {
		isEcho, _ := c.echoTracker.IsOutboundEcho(ctx, tenantID, extMsgID, payload.SourceID)
		if isEcho {
			return ClassificationBotEcho, "matched_sent_intent_cache"
		}
	}

	// 3. Avaliação de autoria por message_type e sender_type do Chatwoot
	msgType := strings.ToLower(strings.TrimSpace(payload.MessageType))

	// Se for outgoing ou sender for bot:
	if msgType == "outgoing" || senderType == "agent_bot" || senderType == "bot" {
		if senderType == "user" {
			// Operador humano enviando mensagem no painel do Chatwoot
			return ClassificationHumanMessage, "human_operator_reply"
		}
		// Mensagem disparada pelo próprio bot/sistema (ECHO)
		return ClassificationBotEcho, "outgoing_bot_echo"
	}

	// Se for incoming e o sender for contact:
	if msgType == "incoming" && (senderType == "contact" || senderType == "") {
		return ClassificationClientMessage, "client_inbound_message"
	}

	// Caso não se encaixe nos padrões operacionais
	return ClassificationIgnored, fmt.Sprintf("unhandled_type_or_sender:%s:%s", msgType, senderType)
}

package dto

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
)

// ChatwootWebhookPayload representa a estrutura JSON padrão de webhook emitida pelo Chatwoot.
type ChatwootWebhookPayload struct {
	Event        string                   `json:"event"`
	ID           int64                    `json:"id"`
	Content      string                   `json:"content"`
	CreatedAt    string                   `json:"created_at"`
	MessageType  string                   `json:"message_type"` // "incoming" (user) ou "outgoing" (agent/bot)
	ContentType  string                   `json:"content_type"` // "text", "audio", etc.
	Private      bool                     `json:"private"`
	SourceID     string                   `json:"source_id"`
	Sender       *ChatwootSender          `json:"sender"`
	Contact      *ChatwootContact         `json:"contact"`
	Conversation *ChatwootConversation    `json:"conversation"`
	Account      *ChatwootAccount         `json:"account"`
	Inbox        *ChatwootInbox           `json:"inbox"`
}

type ChatwootSender struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	AvatarURL string `json:"avatar_url"`
	Type      string `json:"type"` // "contact", "user", "agent_bot"
}

type ChatwootContact struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number"`
	Identifier  string `json:"identifier"`
}

type ChatwootConversation struct {
	ID        int64  `json:"id"`
	InboxID   int64  `json:"inbox_id"`
	Status    string `json:"status"` // "open", "resolved", "pending"
	AssigneeID int64 `json:"assignee_id"`
}

type ChatwootAccount struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type ChatwootInbox struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	ChannelType string `json:"channel_type"` // "Channel::Whatsapp", "Channel::WebWidget", etc.
}

// ChatwootSendRequestDTO define o payload HTTP para enviar mensagens via API do Chatwoot.
type ChatwootSendRequestDTO struct {
	Content           string                 `json:"content"`
	MessageType       string                 `json:"message_type,omitempty"` // "outgoing"
	Private           bool                   `json:"private,omitempty"`
	EchoID            string                 `json:"echo_id,omitempty"`
	ContentAttributes map[string]interface{} `json:"content_attributes,omitempty"`
}

// ChatwootSendResponseDTO define a resposta da API do Chatwoot ao criar mensagem.
type ChatwootSendResponseDTO struct {
	ID                int64                  `json:"id"`
	Content           string                 `json:"content"`
	MessageType       interface{}            `json:"message_type"` // int (1) ou string ("outgoing")
	CreatedAt         int64                  `json:"created_at"`
	ConversationID    int64                  `json:"conversation_id"`
	EchoID            string                 `json:"echo_id,omitempty"`
	ContentAttributes map[string]interface{} `json:"content_attributes,omitempty"`
}

// ChatwootMessagesListResponseDTO define a resposta da API GET /conversations/{id}/messages.
type ChatwootMessagesListResponseDTO struct {
	Meta struct {
		AllCount int `json:"all_count"`
	} `json:"meta"`
	Payload []ChatwootSendResponseDTO `json:"payload"`
}


// MapChatwootChannelType mapeia os tipos de canais proprietários do Chatwoot para o ChannelType canônico.
func MapChatwootChannelType(inboxChannelType string) ports.ChannelType {
	switch inboxChannelType {
	case "Channel::Whatsapp":
		return ports.ChannelTypeWhatsApp
	case "Channel::WebWidget":
		return ports.ChannelTypeLiveChat
	case "Channel::Api":
		return ports.ChannelTypeAPI
	case "Channel::Telegram":
		return ports.ChannelTypeTelegram
	default:
		return ports.ChannelTypeWhatsApp // padrão seguro
	}
}

// ToCanonicalProviderEvent converte o payload do Chatwoot para ProviderEvent com Anti-Corruption Layer (ACL).
func (cw *ChatwootWebhookPayload) ToCanonicalProviderEvent(tenantID string) (*ports.ProviderEvent, error) {
	evtType := ports.ProviderEventUnknown
	switch cw.Event {
	case "message_created":
		evtType = ports.ProviderEventMessageCreated
	case "message_updated":
		evtType = ports.ProviderEventMessageUpdated
	case "conversation_status_changed":
		evtType = ports.ProviderEventConversationStatusChanged
	case "conversation_assigned":
		evtType = ports.ProviderEventConversationAssigned
	default:
		evtType = ports.ProviderEventUnknown
	}

	channel := ports.ChannelTypeWhatsApp
	var inboxID string
	if cw.Inbox != nil {
		channel = MapChatwootChannelType(cw.Inbox.ChannelType)
		inboxID = fmt.Sprintf("%d", cw.Inbox.ID)
	}

	var accountID string
	if cw.Account != nil {
		accountID = fmt.Sprintf("%d", cw.Account.ID)
	}

	var convID string
	var newStatus string
	if cw.Conversation != nil {
		convID = fmt.Sprintf("%d", cw.Conversation.ID)
		newStatus = cw.Conversation.Status
	}

	var contactID string
	if cw.Contact != nil {
		contactID = fmt.Sprintf("%d", cw.Contact.ID)
	}

	identity := ports.ChannelIdentity{
		TenantID:               tenantID,
		Channel:                channel,
		ProviderName:           "chatwoot",
		ExternalAccountID:      accountID,
		ExternalInboxID:        inboxID,
		ExternalConversationID: convID,
		ExternalContactID:      contactID,
	}

	event := &ports.ProviderEvent{
		Type:            evtType,
		TenantID:        tenantID,
		ChannelIdentity: identity,
		NewStatus:       newStatus,
		Timestamp:       time.Now().UTC(),
	}

	if cw.Event == "message_created" || cw.Event == "message_updated" {
		isEcho := false
		role := domain.RoleUser
		// No Chatwoot, message_type="outgoing" ou sender.type="agent_bot" / "user" indica mensagem do agente/bot (echo)
		if cw.MessageType == "outgoing" || (cw.Sender != nil && (cw.Sender.Type == "agent_bot" || cw.Sender.Type == "user")) {
			isEcho = true
			role = domain.RoleAgent
		}

		senderID := contactID
		senderName := ""
		if cw.Sender != nil {
			senderID = fmt.Sprintf("%d", cw.Sender.ID)
			senderName = cw.Sender.Name
		} else if cw.Contact != nil {
			senderName = cw.Contact.Name
		}

		rawBytes, _ := json.Marshal(cw)

		event.IncomingMessage = &ports.IncomingMessageDTO{
			EventID:                fmt.Sprintf("evt-cw-%d", cw.ID),
			TenantID:               tenantID,
			ExternalMessageID:      fmt.Sprintf("%d", cw.ID),
			ExternalConversationID: convID,
			ExternalSenderID:       senderID,
			SenderName:             senderName,
			Role:                   role,
			Channel:                channel,
			ContentType:            cw.ContentType,
			Content:                cw.Content,
			SentAt:                 time.Now().UTC(),
			IsEcho:                 isEcho,
			RawPayload:             rawBytes,
		}
	}

	return event, nil
}

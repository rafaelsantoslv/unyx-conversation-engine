package ports

import (
	"context"
	"errors"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

var (
	ErrUnsupportedChannel     = errors.New("unsupported channel")
	ErrInvalidWebhookPayload  = errors.New("invalid webhook payload")
	ErrUnauthorizedWebhook    = errors.New("unauthorized webhook signature or token")
	ErrDeliveryFailed         = errors.New("message delivery failed")
	ErrExternalEntityNotFound = errors.New("external entity not found in provider")
)

// ChannelType identifica o canal canônico de atendimento.
type ChannelType string

const (
	ChannelTypeWhatsApp  ChannelType = "whatsapp"
	ChannelTypeLiveChat  ChannelType = "livechat"
	ChannelTypeInstagram ChannelType = "instagram"
	ChannelTypeTelegram  ChannelType = "telegram"
	ChannelTypeAPI       ChannelType = "api"
)

func (c ChannelType) IsValid() bool {
	switch c {
	case ChannelTypeWhatsApp, ChannelTypeLiveChat, ChannelTypeInstagram, ChannelTypeTelegram, ChannelTypeAPI:
		return true
	default:
		return false
	}
}

// ChannelIdentity encapsula os identificadores do canal externo isolados por tenant.
type ChannelIdentity struct {
	TenantID               string            `json:"tenant_id"`
	Channel                ChannelType       `json:"channel"`
	ProviderName           string            `json:"provider_name"`
	ExternalAccountID      string            `json:"external_account_id"`
	ExternalInboxID        string            `json:"external_inbox_id"`
	ExternalConversationID string            `json:"external_conversation_id"`
	ExternalContactID      string            `json:"external_contact_id"`
	ExternalUserID         string            `json:"external_user_id,omitempty"`
	ChannelMetadata        map[string]string `json:"channel_metadata,omitempty"`
}

// IncomingMessageDTO representa os dados canônicos normalizados recebidos de um canal externo antes do domínio.
type IncomingMessageDTO struct {
	EventID                string            `json:"event_id"`
	TenantID               string            `json:"tenant_id"`
	ExternalMessageID      string            `json:"external_message_id"`
	ExternalConversationID string            `json:"external_conversation_id"`
	ExternalSenderID       string            `json:"external_sender_id"`
	SenderName             string            `json:"sender_name,omitempty"`
	Role                   domain.MessageRole `json:"role"`
	Channel                ChannelType       `json:"channel"`
	ContentType            string            `json:"content_type"` // text, audio, image, etc.
	Content                string            `json:"content"`
	MediaURL               string            `json:"media_url,omitempty"`
	SentAt                 time.Time         `json:"sent_at"`
	IsEcho                 bool              `json:"is_echo"` // Se a mensagem foi enviada pelo próprio bot/sistema
	RawPayload             []byte            `json:"raw_payload,omitempty"`
}

// OutboundMessageDTO representa a intenção canônica de envio a ser despachada para o canal externo.
type OutboundMessageDTO struct {
	OutboundIntentID       string            `json:"outbound_intent_id"`
	TenantID               string            `json:"tenant_id"`
	ConversationID         string            `json:"conversation_id"`
	ExternalConversationID string            `json:"external_conversation_id"`
	ExternalInboxID        string            `json:"external_inbox_id,omitempty"`
	Channel                ChannelType       `json:"channel"`
	RecipientID            string            `json:"recipient_id"`
	Content                string            `json:"content"`
	ContentType            string            `json:"content_type"`
	IdempotencyKey         string            `json:"idempotency_key"`
	Metadata               map[string]string `json:"metadata,omitempty"`
}

// DeliveryReceipt representa a confirmação de entrega do provedor externo.
type DeliveryReceipt struct {
	OutboundIntentID  string            `json:"outbound_intent_id"`
	ExternalMessageID string            `json:"external_message_id"`
	DeliveredAt       time.Time         `json:"delivered_at"`
	Status            string            `json:"status"` // SENT, DELIVERED, READ, FAILED
	ResponsePayload   []byte            `json:"response_payload,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

// ProviderEventType define os tipos de eventos recebidos dos provedores externos.
type ProviderEventType string

const (
	ProviderEventMessageCreated             ProviderEventType = "message_created"
	ProviderEventMessageUpdated             ProviderEventType = "message_updated"
	ProviderEventConversationStatusChanged  ProviderEventType = "conversation_status_changed"
	ProviderEventConversationAssigned       ProviderEventType = "conversation_assigned"
	ProviderEventUnknown                    ProviderEventType = "unknown"
)

// ProviderEvent encapsula um evento externo normalizado.
type ProviderEvent struct {
	Type            ProviderEventType   `json:"type"`
	TenantID        string              `json:"tenant_id"`
	ChannelIdentity ChannelIdentity     `json:"channel_identity"`
	IncomingMessage *IncomingMessageDTO `json:"incoming_message,omitempty"`
	NewStatus       string              `json:"new_status,omitempty"`
	AssigneeID      string              `json:"assignee_id,omitempty"`
	Timestamp       time.Time           `json:"timestamp"`
}

// InboundWebhookParser define o contrato para decodificar webhooks brutos do provedor.
type InboundWebhookParser interface {
	ParseWebhook(ctx context.Context, tenantID string, headers map[string]string, body []byte) (*ProviderEvent, error)
	ValidateAuth(ctx context.Context, tenantID string, headers map[string]string, token string) error
}

// OutboundSender define o contrato para despacho de respostas para o provedor externo.
type OutboundSender interface {
	Send(ctx context.Context, msg OutboundMessageDTO) (*DeliveryReceipt, error)
}

// ChannelAdapter agrega as portas de entrada e saída para um provedor específico.
type ChannelAdapter interface {
	Channel() ChannelType
	Inbound() InboundWebhookParser
	Outbound() OutboundSender
}

package dto

import (
	"errors"
	"strings"
	"time"
)

type IngestMessageRequest struct {
	TenantID          string `json:"tenant_id"`
	ConversationID    string `json:"conversation_id"`
	ExternalMessageID string `json:"external_message_id"`
	Channel           string `json:"channel"`
	Role              string `json:"role"`
	Content           string `json:"content"`
}

var (
	ErrMissingTenantID          = errors.New("field 'tenant_id' is required")
	ErrMissingConversationID    = errors.New("field 'conversation_id' is required")
	ErrMissingExternalMessageID = errors.New("field 'external_message_id' is required")
	ErrMissingChannel           = errors.New("field 'channel' is required")
	ErrMissingRole              = errors.New("field 'role' is required")
	ErrInvalidRole              = errors.New("field 'role' must be one of: user, agent, system")
	ErrMissingContent           = errors.New("field 'content' is required")
)

func (req *IngestMessageRequest) Validate() error {
	if strings.TrimSpace(req.TenantID) == "" {
		return ErrMissingTenantID
	}
	if strings.TrimSpace(req.ConversationID) == "" {
		return ErrMissingConversationID
	}
	if strings.TrimSpace(req.ExternalMessageID) == "" {
		return ErrMissingExternalMessageID
	}
	if strings.TrimSpace(req.Channel) == "" {
		return ErrMissingChannel
	}
	if strings.TrimSpace(req.Role) == "" {
		return ErrMissingRole
	}
	switch req.Role {
	case "user", "agent", "system":
	default:
		return ErrInvalidRole
	}
	if strings.TrimSpace(req.Content) == "" {
		return ErrMissingContent
	}
	return nil
}

type IngestMessageResponse struct {
	Status         string    `json:"status"`
	MessageID      string    `json:"message_id"`
	ConversationID string    `json:"conversation_id"`
	AcceptedAt     time.Time `json:"accepted_at"`
}

type ErrorResponse struct {
	Error     string `json:"error"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

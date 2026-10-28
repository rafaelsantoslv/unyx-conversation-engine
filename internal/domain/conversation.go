package domain

import (
	"errors"
	"strings"
	"time"
)

type ConversationState string

const (
	StateAIActive    ConversationState = "AI_ACTIVE"
	StateHumanActive ConversationState = "HUMAN_ACTIVE"
	StateClosed      ConversationState = "CLOSED"
)

func (s ConversationState) IsValid() bool {
	switch s {
	case StateAIActive, StateHumanActive, StateClosed:
		return true
	default:
		return false
	}
}

type Conversation struct {
	ID                     string                 `json:"id"`
	TenantID               string                 `json:"tenant_id"`
	UserID                 string                 `json:"user_id,omitempty"`
	ExternalConversationID string                 `json:"external_conversation_id"`
	Channel                string                 `json:"channel"`
	State                  ConversationState      `json:"state"`
	Version                int64                  `json:"version"`
	Metadata               map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt              time.Time              `json:"created_at"`
	UpdatedAt              time.Time              `json:"updated_at"`
}

var (
	ErrEmptyConversationID         = errors.New("conversation id cannot be empty")
	ErrEmptyTenantID               = errors.New("tenant id cannot be empty")
	ErrEmptyExternalConversationID = errors.New("external conversation id cannot be empty")
	ErrEmptyChannel                = errors.New("channel cannot be empty")
	ErrInvalidConversationState    = errors.New("invalid conversation state")
)

func (c *Conversation) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return ErrEmptyConversationID
	}
	if strings.TrimSpace(c.TenantID) == "" {
		return ErrEmptyTenantID
	}
	if strings.TrimSpace(c.ExternalConversationID) == "" {
		return ErrEmptyExternalConversationID
	}
	if strings.TrimSpace(c.Channel) == "" {
		return ErrEmptyChannel
	}
	if !c.State.IsValid() {
		return ErrInvalidConversationState
	}
	return nil
}

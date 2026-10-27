package domain

import (
	"errors"
	"strings"
	"time"
)

type MessageRole string

const (
	RoleUser   MessageRole = "user"
	RoleAgent  MessageRole = "agent"
	RoleSystem MessageRole = "system"
)

func (r MessageRole) IsValid() bool {
	switch r {
	case RoleUser, RoleAgent, RoleSystem:
		return true
	default:
		return false
	}
}

type Message struct {
	ID                string      `json:"id"`
	TenantID          string      `json:"tenant_id"`
	ConversationID    string      `json:"conversation_id"`
	ExternalMessageID string      `json:"external_message_id"`
	Role              MessageRole `json:"role"`
	Content           string      `json:"content"`
	SequenceNumber    int64       `json:"sequence_number"`
	CreatedAt         time.Time   `json:"created_at"`
}

var (
	ErrEmptyMessageID         = errors.New("message id cannot be empty")
	ErrEmptyConversationRef   = errors.New("conversation id cannot be empty")
	ErrEmptyExternalMessageID = errors.New("external message id cannot be empty")
	ErrInvalidMessageRole     = errors.New("invalid message role")
	ErrEmptyMessageContent    = errors.New("message content cannot be empty")
	ErrInvalidSequenceNumber  = errors.New("sequence number must be greater than or equal to 0")
)

func (m *Message) Validate() error {
	if strings.TrimSpace(m.ID) == "" {
		return ErrEmptyMessageID
	}
	if strings.TrimSpace(m.TenantID) == "" {
		return ErrEmptyTenantID
	}
	if strings.TrimSpace(m.ConversationID) == "" {
		return ErrEmptyConversationRef
	}
	if strings.TrimSpace(m.ExternalMessageID) == "" {
		return ErrEmptyExternalMessageID
	}
	if !m.Role.IsValid() {
		return ErrInvalidMessageRole
	}
	if strings.TrimSpace(m.Content) == "" {
		return ErrEmptyMessageContent
	}
	if m.SequenceNumber < 0 {
		return ErrInvalidSequenceNumber
	}
	return nil
}

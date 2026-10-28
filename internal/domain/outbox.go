package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

func (s OutboxStatus) IsValid() bool {
	switch s {
	case OutboxStatusPending, OutboxStatusPublished, OutboxStatusFailed:
		return true
	default:
		return false
	}
}

type OutboxEvent struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	ConversationID string          `json:"conversation_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	PartitionKey   string          `json:"partition_key"`
	Status         OutboxStatus    `json:"status"`
	RetryCount     int             `json:"retry_count"`
	CreatedAt      time.Time       `json:"created_at"`
	PublishedAt    *time.Time      `json:"published_at,omitempty"`
}

var (
	ErrEmptyOutboxID           = errors.New("outbox id cannot be empty")
	ErrEmptyOutboxPartitionKey = errors.New("outbox partition_key cannot be empty")
	ErrEmptyOutboxPayload      = errors.New("outbox payload cannot be empty")
	ErrInvalidOutboxStatus     = errors.New("invalid outbox status")
)

func (o *OutboxEvent) Validate() error {
	if strings.TrimSpace(o.ID) == "" {
		return ErrEmptyOutboxID
	}
	if strings.TrimSpace(o.TenantID) == "" {
		return ErrEmptyTenantID
	}
	if strings.TrimSpace(o.ConversationID) == "" {
		return ErrEmptyConversationRef
	}
	if strings.TrimSpace(o.EventType) == "" {
		return ErrEmptyEventType
	}
	if len(o.Payload) == 0 {
		return ErrEmptyOutboxPayload
	}
	if strings.TrimSpace(o.PartitionKey) == "" {
		return ErrEmptyOutboxPartitionKey
	}
	if !o.Status.IsValid() {
		return ErrInvalidOutboxStatus
	}
	return nil
}

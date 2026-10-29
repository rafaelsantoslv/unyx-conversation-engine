package domain

import (
	"errors"
	"strings"
	"time"
)

type ScheduleStatus string

const (
	ScheduleStatusPending   ScheduleStatus = "PENDING"
	ScheduleStatusClaimed   ScheduleStatus = "CLAIMED"
	ScheduleStatusExecuted  ScheduleStatus = "EXECUTED"
	ScheduleStatusCancelled ScheduleStatus = "CANCELLED"

	EventTypeExecutionReady = "conversation.execution.ready"
)

func (s ScheduleStatus) IsValid() bool {
	switch s {
	case ScheduleStatusPending, ScheduleStatusClaimed, ScheduleStatusExecuted, ScheduleStatusCancelled:
		return true
	default:
		return false
	}
}

// ConversationSchedule representa um agendamento durável persistido no banco (INV-03, INV-05).
type ConversationSchedule struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id"`
	ConversationID string         `json:"conversation_id"`
	ScheduledFor   time.Time      `json:"scheduled_for"`
	Status         ScheduleStatus `json:"status"`
	ClaimedAt      *time.Time     `json:"claimed_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// ExecutionReadyPayload representa o payload emitido quando o agendamento dispara e está pronto para o worker.
type ExecutionReadyPayload struct {
	ExecutionRequestID string    `json:"execution_request_id,omitempty"`
	ConversationID     string    `json:"conversation_id"`
	TenantID           string    `json:"tenant_id"`
	WatermarkSequence  int64     `json:"watermark_sequence,omitempty"`
	MessageIDs         []string  `json:"message_ids,omitempty"`
	TriggeredAt        time.Time `json:"triggered_at"`
	Reason             string    `json:"reason"`
}

var (
	ErrEmptyScheduleID       = errors.New("schedule id cannot be empty")
	ErrInvalidScheduleStatus = errors.New("invalid schedule status")
	ErrZeroScheduledFor      = errors.New("scheduled_for must not be zero")
)

func (s *ConversationSchedule) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return ErrEmptyScheduleID
	}
	if strings.TrimSpace(s.TenantID) == "" {
		return ErrEmptyTenantID
	}
	if strings.TrimSpace(s.ConversationID) == "" {
		return ErrEmptyConversationRef
	}
	if !s.Status.IsValid() {
		return ErrInvalidScheduleStatus
	}
	if s.ScheduledFor.IsZero() {
		return ErrZeroScheduledFor
	}
	return nil
}

// IsDue retorna verdadeiro se o agendamento já deve ser executado no instante now.
func (s *ConversationSchedule) IsDue(now time.Time) bool {
	if s.Status != ScheduleStatusPending {
		return false
	}
	return !now.Before(s.ScheduledFor)
}

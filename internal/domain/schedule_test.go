package domain_test

import (
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestConversationSchedule_Validate(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name    string
		sched   domain.ConversationSchedule
		wantErr error
	}{
		{
			name: "valid schedule",
			sched: domain.ConversationSchedule{
				ID:             "sched-1",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				ScheduledFor:   now.Add(5 * time.Second),
				Status:         domain.ScheduleStatusPending,
			},
			wantErr: nil,
		},
		{
			name: "missing id",
			sched: domain.ConversationSchedule{
				ID:             "",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				ScheduledFor:   now.Add(5 * time.Second),
				Status:         domain.ScheduleStatusPending,
			},
			wantErr: domain.ErrEmptyScheduleID,
		},
		{
			name: "missing tenant",
			sched: domain.ConversationSchedule{
				ID:             "sched-1",
				TenantID:       "",
				ConversationID: "conv-1",
				ScheduledFor:   now.Add(5 * time.Second),
				Status:         domain.ScheduleStatusPending,
			},
			wantErr: domain.ErrEmptyTenantID,
		},
		{
			name: "invalid status",
			sched: domain.ConversationSchedule{
				ID:             "sched-1",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				ScheduledFor:   now.Add(5 * time.Second),
				Status:         "UNKNOWN",
			},
			wantErr: domain.ErrInvalidScheduleStatus,
		},
		{
			name: "zero scheduled_for",
			sched: domain.ConversationSchedule{
				ID:             "sched-1",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				ScheduledFor:   time.Time{},
				Status:         domain.ScheduleStatusPending,
			},
			wantErr: domain.ErrZeroScheduledFor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sched.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConversationSchedule_IsDue(t *testing.T) {
	now := time.Now().UTC()

	sched := domain.ConversationSchedule{
		ID:             "sched-1",
		TenantID:       "tenant-1",
		ConversationID: "conv-1",
		ScheduledFor:   now,
		Status:         domain.ScheduleStatusPending,
	}

	// Exatamente no momento
	assert.True(t, sched.IsDue(now))
	// 1 segundo depois
	assert.True(t, sched.IsDue(now.Add(time.Second)))
	// 1 segundo antes
	assert.False(t, sched.IsDue(now.Add(-time.Second)))

	// Se não estiver PENDING
	sched.Status = domain.ScheduleStatusClaimed
	assert.False(t, sched.IsDue(now.Add(time.Second)))
}

package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
	"github.com/stretchr/testify/assert"
)

type mockScheduleClaimer struct {
	claimFunc func(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error)
}

func (m *mockScheduleClaimer) ClaimDueSchedulesWithOutbox(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx, batchSize, now)
	}
	return nil, nil
}

func TestConversationScheduler_PollOnce(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully claims due schedules", func(t *testing.T) {
		mockClaimer := &mockScheduleClaimer{
			claimFunc: func(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error) {
				return []domain.ConversationSchedule{
					{
						ID:             "s-1",
						TenantID:       "t-1",
						ConversationID: "c-1",
						ScheduledFor:   now.Add(-time.Second),
						Status:         domain.ScheduleStatusClaimed,
					},
					{
						ID:             "s-2",
						TenantID:       "t-1",
						ConversationID: "c-2",
						ScheduledFor:   now.Add(-time.Second),
						Status:         domain.ScheduleStatusClaimed,
					},
				}, nil
			},
		}

		scheduler := worker.NewConversationScheduler(mockClaimer, worker.ConversationSchedulerConfig{
			BatchSize: 10,
		})

		count, err := scheduler.PollOnce(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("handles claim error", func(t *testing.T) {
		mockClaimer := &mockScheduleClaimer{
			claimFunc: func(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error) {
				return nil, errors.New("database connection failed")
			},
		}

		scheduler := worker.NewConversationScheduler(mockClaimer, worker.ConversationSchedulerConfig{})
		count, err := scheduler.PollOnce(ctx)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestConversationScheduler_StartGracefulShutdown(t *testing.T) {
	mockClaimer := &mockScheduleClaimer{
		claimFunc: func(ctx context.Context, batchSize int, now time.Time) ([]domain.ConversationSchedule, error) {
			return nil, nil
		},
	}

	scheduler := worker.NewConversationScheduler(mockClaimer, worker.ConversationSchedulerConfig{
		PollInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		scheduler.Start(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Sucesso no encerramento gracioso
	case <-time.After(1 * time.Second):
		t.Fatal("scheduler did not stop gracefully")
	}
}

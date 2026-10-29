package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleRepository_ScheduleAndClaim(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewScheduleRepository(pool)
	coreRepo := postgres.NewRepository(pool)

	tenantID := "tenant-sched-test"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	now := time.Now().UTC()
	scheduledFor := now.Add(-10 * time.Millisecond) // já devido

	// 1. Agenda execução
	sched, err := repo.ScheduleExecution(ctx, tenantID, conv.ID, scheduledFor)
	require.NoError(t, err)
	assert.NotEmpty(t, sched.ID)
	assert.Equal(t, domain.ScheduleStatusPending, sched.Status)
	assert.Equal(t, conv.ID, sched.ConversationID)

	// 2. Reivindica agendamentos prontos
	claimed, err := repo.ClaimDueSchedulesWithOutbox(ctx, 10, now)
	require.NoError(t, err)

	// Deve ter reivindicado pelo menos a nossa conversa
	var found *domain.ConversationSchedule
	for _, c := range claimed {
		if c.ConversationID == conv.ID {
			found = &c
			break
		}
	}
	require.NotNil(t, found, "schedule must be claimed")
	assert.Equal(t, domain.ScheduleStatusClaimed, found.Status)
	assert.NotNil(t, found.ClaimedAt)

	// 3. Verifica se o evento correspondente foi gravado atomicamente na outbox_events
	outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	var outboxFound *domain.OutboxEvent
	for _, ob := range outboxEvents {
		if ob.ConversationID == conv.ID && ob.EventType == domain.EventTypeExecutionReady {
			outboxFound = ob
			break
		}
	}
	require.NotNil(t, outboxFound, "outbox event for execution ready must exist")
	assert.Equal(t, domain.OutboxStatusPending, outboxFound.Status)
	assert.Equal(t, conv.ID, outboxFound.PartitionKey)

	var payload domain.Event[domain.ExecutionReadyPayload]
	err = json.Unmarshal(outboxFound.Payload, &payload)
	require.NoError(t, err)
	assert.Equal(t, conv.ID, payload.Payload.ConversationID)
	assert.Equal(t, tenantID, payload.Payload.TenantID)

	// 4. Se chamar ClaimDueSchedulesWithOutbox novamente, não deve reivindicar a mesma conversa (SKIP LOCKED / Status CLAIMED)
	claimedAgain, err := repo.ClaimDueSchedulesWithOutbox(ctx, 10, now)
	require.NoError(t, err)
	for _, c := range claimedAgain {
		assert.NotEqual(t, conv.ID, c.ConversationID, "must not re-claim already claimed schedule")
	}

	// 5. Marca como EXECUTED
	err = repo.MarkScheduleExecuted(ctx, conv.ID)
	require.NoError(t, err)

	updatedSched, err := repo.GetSchedule(ctx, conv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ScheduleStatusExecuted, updatedSched.Status)
}

func TestScheduleRepository_ConcurrentSkipLocked(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewScheduleRepository(pool)
	coreRepo := postgres.NewRepository(pool)

	tenantID := "tenant-concurrent-sched"
	numSchedules := 20
	var convIDs []string

	now := time.Now().UTC()
	for i := 0; i < numSchedules; i++ {
		conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, fmt.Sprintf("ext-conc-%d-%s", i, uuid.New().String()))
		require.NoError(t, err)
		convIDs = append(convIDs, conv.ID)

		_, err = repo.ScheduleExecution(ctx, tenantID, conv.ID, now.Add(-time.Second))
		require.NoError(t, err)
	}

	// 10 Goroutines competindo para reivindicar as tarefas via SKIP LOCKED
	concurrency := 10
	var wg sync.WaitGroup
	claimedMap := sync.Map{}
	var mu sync.Mutex
	duplicateClaims := 0

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			claimed, err := repo.ClaimDueSchedulesWithOutbox(ctx, 5, time.Now().UTC())
			if err != nil {
				t.Errorf("worker %d claim error: %v", workerNum, err)
				return
			}

			for _, s := range claimed {
				if _, loaded := claimedMap.LoadOrStore(s.ConversationID, true); loaded {
					mu.Lock()
					duplicateClaims++
					mu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()

	assert.Equal(t, 0, duplicateClaims, "no schedule should be claimed by multiple workers simultaneously (INV-05)")
}

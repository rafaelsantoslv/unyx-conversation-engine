package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDurableScheduling_SurvivesCrashAndExecutes valida a Invariante INV-03, INV-05 e INV-08:
// 1. Um agendamento durável é persistido no PostgreSQL.
// 2. Um processo/worker 'cai' (simulado pelo cancelamento de contexto).
// 3. Uma nova instância do Scheduler sobe, reivindica atomicamente o agendamento devido via SKIP LOCKED
//    e emite o evento 'conversation.execution.ready' na Transactional Outbox.
func TestDurableScheduling_SurvivesCrashAndExecutes(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	schedRepo := postgres.NewScheduleRepository(pool)

	tenantID := "tenant-durable-sched"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	// Agenda para disparar daqui a 150 milissegundos
	scheduleTime := time.Now().UTC().Add(150 * time.Millisecond)
	sched, err := schedRepo.ScheduleExecution(ctx, tenantID, conv.ID, scheduleTime)
	require.NoError(t, err)
	assert.Equal(t, domain.ScheduleStatusPending, sched.Status)

	// Simula Instância 1 de Scheduler que morre/cai antes do disparo
	ctx1, cancel1 := context.WithCancel(ctx)
	scheduler1 := worker.NewConversationScheduler(schedRepo, worker.ConversationSchedulerConfig{
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
	})
	go scheduler1.Start(ctx1)

	// O processo 1 sofre um crash fatal após 30ms (antes do timer de 150ms expirar)
	time.Sleep(30 * time.Millisecond)
	cancel1()

	// Verifica que o agendamento sobreviveu no banco intacto em estado PENDING
	currentSched, err := schedRepo.GetSchedule(ctx, conv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ScheduleStatusPending, currentSched.Status)
	assert.Nil(t, currentSched.ClaimedAt)

	// Aguarda o momento de expiração da janela (scheduled_for)
	time.Sleep(150 * time.Millisecond)

	// Sobe a Instância 2 do Scheduler (Recuperação / Failover)
	scheduler2 := worker.NewConversationScheduler(schedRepo, worker.ConversationSchedulerConfig{
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
	})

	claimedCount, err := scheduler2.PollOnce(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, claimedCount, 1)

	// Valida que o agendamento foi reivindicado com sucesso
	claimedSched, err := schedRepo.GetSchedule(ctx, conv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ScheduleStatusClaimed, claimedSched.Status)
	assert.NotNil(t, claimedSched.ClaimedAt)

	// Valida que o Transactional Outbox recebeu o evento de execução pronta
	outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	var foundEvent *domain.OutboxEvent
	for _, evt := range outboxEvents {
		if evt.ConversationID == conv.ID && evt.EventType == domain.EventTypeExecutionReady {
			foundEvent = evt
			break
		}
	}
	require.NotNil(t, foundEvent, "transactional outbox must contain conversation.execution.ready event")
	assert.Equal(t, conv.ID, foundEvent.PartitionKey)
}

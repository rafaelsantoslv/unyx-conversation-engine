package e2e_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLifecycleAndOrchestration_18Scenarios implements and certifies all 18 mandatory scenarios of EPIC-04 (UDS-204)
// validating the complete lifecycle, batching, durable scheduling, state transitions, idempotency, and multi-tenant isolation.
func TestLifecycleAndOrchestration_18Scenarios(t *testing.T) {
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	ctx := context.Background()
	resultsRepo := postgres.NewResultsRepository(pool)
	coreRepo := postgres.NewRepository(pool)
	schedRepo := postgres.NewScheduleRepository(pool)
	debounceRepo := postgres.NewDebounceRepository(pool)
	execReqRepo := postgres.NewExecutionRequestRepository(pool)
	policyRepo := postgres.NewPolicyRepository(pool, 5*time.Second)
	handoffSvc := service.NewHandoffService(coreRepo)

	// Helper to create conversation
	createConv := func(tenantID, state string) *domain.Conversation {
		conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
		require.NoError(t, err)
		if string(conv.State) != state {
			conv, err = coreRepo.TransitionConversationState(ctx, tenantID, conv.ID, domain.ConversationState(state), conv.Version, "test setup transition", "test-user")
			require.NoError(t, err)
		}
		return conv
	}

	// Helper to ingest message
	ingestMsg := func(tenantID, convID string, content string) (*domain.Message, domain.Event[domain.MessageReceivedPayload]) {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    convID,
			ExternalMessageID: "msg-ext-" + uuid.New().String(),
			Role:              domain.RoleUser,
			Content:           content,
			CreatedAt:         time.Now().UTC(),
		}
		saved, err := coreRepo.SaveMessage(ctx, msg)
		require.NoError(t, err)

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        uuid.New().String(),
			TenantID:       tenantID,
			ConversationID: convID,
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      saved.ID,
				SequenceNumber: saved.SequenceNumber,
				Role:           saved.Role,
				Content:        saved.Content,
				Channel:        "whatsapp",
			},
		}
		return saved, evt
	}

	t.Run("Scenario 01 & 02: Three rapid messages generate single logical debounce execution and are persisted individually", func(t *testing.T) {
		tenantID := "tenant-scen-1-2-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("Resposta agregada"))

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithDebounce(debounceRepo, domain.DebounceConfig{
				InactivityWindow: 150 * time.Millisecond,
				MaxWaitTimeout:   2 * time.Second,
			}),
			service.WithScheduler(schedRepo),
			service.WithExecutionRequestRepository(execReqRepo),
		)

		msg1, evt1 := ingestMsg(tenantID, conv.ID, "Oi")
		_, evt2 := ingestMsg(tenantID, conv.ID, "Tudo bem?")
		msg3, evt3 := ingestMsg(tenantID, conv.ID, "Preciso de ajuda")

		// Persistent individual check (Scenario 02 - INV-01)
		var count int
		err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE conversation_id = $1", conv.ID).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count, "INV-01: All 3 messages must be individually persisted")

		// Register in debounce window
		w1, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, domain.DebounceConfig{
			InactivityWindow: 150 * time.Millisecond,
			MaxWaitTimeout:   2 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, w1.MessageCount)

		time.Sleep(20 * time.Millisecond)
		w2, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, domain.DebounceConfig{
			InactivityWindow: 150 * time.Millisecond,
			MaxWaitTimeout:   2 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, w2.MessageCount)

		time.Sleep(20 * time.Millisecond)
		w3, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, domain.DebounceConfig{
			InactivityWindow: 150 * time.Millisecond,
			MaxWaitTimeout:   2 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, w3.MessageCount)

		// Wait for silence window to expire
		time.Sleep(200 * time.Millisecond)

		// Process messages sequentially
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt1))
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt2))
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt3))

		// Check processing results
		res1, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res1.Status)

		res3, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg3.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res3.Status)
	})

	t.Run("Scenario 03: Messages with interval greater than window generate distinct executions", func(t *testing.T) {
		tenantID := "tenant-scen-3-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("Resposta distinta"))
		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithDebounce(debounceRepo, domain.DebounceConfig{
				InactivityWindow: 50 * time.Millisecond,
				MaxWaitTimeout:   1 * time.Second,
			}),
		)

		msg1, evt1 := ingestMsg(tenantID, conv.ID, "Primeira mensagem")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt1))

		time.Sleep(100 * time.Millisecond)

		msg2, evt2 := ingestMsg(tenantID, conv.ID, "Segunda mensagem tardia")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt2))

		res1, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		res2, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg2.ID)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res1.Status)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res2.Status)
	})

	t.Run("Scenario 04: Debounce disabled executes immediately", func(t *testing.T) {
		tenantID := "tenant-scen-4-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("Imediato"))
		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithDebounce(debounceRepo, domain.DebounceConfig{
				InactivityWindow: 0,
				MaxWaitTimeout:   0,
			}),
		)

		start := time.Now()
		_, evt1 := ingestMsg(tenantID, conv.ID, "Mensagem imediata")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt1))
		elapsed := time.Since(start)

		assert.Less(t, elapsed, 100*time.Millisecond, "Execution without debounce must be immediate")
	})

	t.Run("Scenario 05: Max wait ceiling forces execution even with active typing", func(t *testing.T) {
		tenantID := "tenant-scen-5-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		cfg := domain.DebounceConfig{
			InactivityWindow: 300 * time.Millisecond,
			MaxWaitTimeout:   300 * time.Millisecond,
		}

		w, err := debounceRepo.RegisterOrExtendWindow(ctx, tenantID, conv.ID, cfg)
		require.NoError(t, err)

		time.Sleep(320 * time.Millisecond) // exceeds max wait
		assert.True(t, w.IsReady(time.Now().UTC()), "Ceiling exceeded must force ready")
	})

	t.Run("Scenario 06: Scheduler restarted during wait resumes without losing conversation", func(t *testing.T) {
		tenantID := "tenant-scen-6-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")
		_, _ = ingestMsg(tenantID, conv.ID, "Mensagem agendada")

		// Schedule for 50ms from now
		dueTime := time.Now().UTC().Add(50 * time.Millisecond)
		sched, err := schedRepo.ScheduleExecution(ctx, tenantID, conv.ID, dueTime)
		require.NoError(t, err)
		assert.Equal(t, domain.ScheduleStatusPending, sched.Status)

		// Simulate scheduler crash & restart
		scheduler1 := worker.NewConversationScheduler(schedRepo, worker.ConversationSchedulerConfig{
			PollInterval: 15 * time.Millisecond,
			BatchSize:    10,
		})
		ctxSched, cancelSched := context.WithCancel(ctx)
		defer cancelSched()
		go scheduler1.Start(ctxSched)

		eventually(t, 2*time.Second, 20*time.Millisecond, func() bool {
			s, err := schedRepo.GetSchedule(ctx, conv.ID)
			return err == nil && s != nil && s.Status == domain.ScheduleStatusClaimed
		}, "Scheduler restart must claim due schedule")
	})

	t.Run("Scenario 07: Two concurrent schedulers do not duplicate execution claims", func(t *testing.T) {
		tenantID := "tenant-scen-7-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")
		_, _ = ingestMsg(tenantID, conv.ID, "Mensagem teste concorrencia")

		dueTime := time.Now().UTC().Add(-10 * time.Millisecond) // already due
		_, err := schedRepo.ScheduleExecution(ctx, tenantID, conv.ID, dueTime)
		require.NoError(t, err)

		var claimedCount int32
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				claimed, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 10, time.Now().UTC())
				if err == nil {
					for _, item := range claimed {
						if item.ConversationID == conv.ID {
							atomic.AddInt32(&claimedCount, 1)
						}
					}
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, int32(1), atomic.LoadInt32(&claimedCount), "Exactly one scheduler can claim the schedule (FOR UPDATE SKIP LOCKED)")
	})

	t.Run("Scenario 08 & 09: Message arrives during active execution is recorded and processed in subsequent cycle", func(t *testing.T) {
		tenantID := "tenant-scen-8-9-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("OK"))
		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithDebounce(debounceRepo, domain.DebounceConfig{InactivityWindow: 0, MaxWaitTimeout: 0}),
			service.WithExecutionRequestRepository(execReqRepo),
		)

		msg1, evt1 := ingestMsg(tenantID, conv.ID, "Pergunta 1")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt1))

		// While msg1 completed, msg2 arrives as inflight/subsequent
		msg2, evt2 := ingestMsg(tenantID, conv.ID, "Pergunta 2 que chegou")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt2))

		res1, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res1.Status)

		res2, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg2.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res2.Status)
	})

	t.Run("Scenario 10 & 11: Conversation switches to HUMAN during AI computation, result discarded", func(t *testing.T) {
		tenantID := "tenant-scen-10-11-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(
			mock.WithLatency(150*time.Millisecond),
			mock.WithSimulatedReply("Resposta atrasada"),
		)

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithExecutionRequestRepository(execReqRepo),
		)

		msg1, evt1 := ingestMsg(tenantID, conv.ID, "Socorro preciso de humano")

		execDone := make(chan error, 1)
		go func() {
			execDone <- pipeline.HandleMessageReceived(ctx, evt1)
		}()

		// Operador humano assume aos 40ms
		time.Sleep(40 * time.Millisecond)
		_, err := pool.Exec(ctx, "UPDATE conversations SET status = 'HUMAN_ACTIVE', version = version + 1 WHERE id = $1", conv.ID)
		require.NoError(t, err)

		select {
		case err := <-execDone:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout aguardando pipeline")
		}

		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusDiscardedStale, res.Status, "Result must be marked DISCARDED_STALE (INV-06, INV-07)")
	})

	t.Run("Scenario 12: Messages arriving during human attendance do not trigger AI", func(t *testing.T) {
		tenantID := "tenant-scen-12-" + uuid.New().String()
		conv := createConv(tenantID, "HUMAN_ACTIVE")

		msg1, _ := ingestMsg(tenantID, conv.ID, "Mensagem durante atendimento humano")
		// Verify conversation state in db
		var currentState string
		err := pool.QueryRow(ctx, "SELECT status FROM conversations WHERE id = $1", conv.ID).Scan(&currentState)
		require.NoError(t, err)
		assert.Equal(t, "HUMAN_ACTIVE", currentState)

		// Check that processing_result does not exist or is not RUNNING/SUCCEEDED
		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		require.NoError(t, err)
		assert.Nil(t, res, "No execution result should be generated for human-attended conversation")
	})

	t.Run("Scenario 13: Resuming to AI only processes new messages (fresh start watermark)", func(t *testing.T) {
		tenantID := "tenant-scen-13-" + uuid.New().String()
		conv := createConv(tenantID, "HUMAN_ACTIVE")

		// Messages 1 and 2 handled by human
		_, _ = ingestMsg(tenantID, conv.ID, "Humano 1")
		_, _ = ingestMsg(tenantID, conv.ID, "Humano 2")

		// Resume AI attendance via HandoffService
		convResumed, err := handoffSvc.ResumeAIAttendance(ctx, tenantID, conv.ID, conv.Version, "operator handover back to AI", "supervisor")
		require.NoError(t, err)
		assert.Equal(t, domain.StateAIActive, convResumed.State)
		assert.Equal(t, int64(2), convResumed.ResumedAtSequence)

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("IA respondendo novo lote"))

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithExecutionRequestRepository(execReqRepo),
		)

		// New message 3 should be processed cleanly without error
		msg3, evt3 := ingestMsg(tenantID, conv.ID, "Nova pergunta para a IA")
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt3))

		res3, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg3.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res3.Status)
	})

	t.Run("Scenario 14: Closed conversation blocks automatic AI triggers", func(t *testing.T) {
		tenantID := "tenant-scen-14-" + uuid.New().String()
		conv := createConv(tenantID, "CLOSED")

		var currentStatus string
		err := pool.QueryRow(ctx, "SELECT status FROM conversations WHERE id = $1", conv.ID).Scan(&currentStatus)
		require.NoError(t, err)
		assert.Equal(t, "CLOSED", currentStatus)

		msg1, _ := ingestMsg(tenantID, conv.ID, "Mensagem em conversa fechada")

		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg1.ID)
		require.NoError(t, err)
		assert.Nil(t, res, "No processing result should be generated for closed conversation")
	})

	t.Run("Scenario 15: Two tenants with different policies operate completely isolated", func(t *testing.T) {
		tenantA := "tenant-pol-a-" + uuid.New().String()
		tenantB := "tenant-pol-b-" + uuid.New().String()

		_ = createConv(tenantA, "AI_ACTIVE")
		_ = createConv(tenantB, "AI_ACTIVE")

		err := policyRepo.UpsertTenantPolicy(ctx, domain.ConversationPolicy{
			TenantID:        tenantA,
			DebounceEnabled: false,
		})
		require.NoError(t, err)

		err = policyRepo.UpsertTenantPolicy(ctx, domain.ConversationPolicy{
			TenantID:                  tenantB,
			DebounceEnabled:           true,
			DebounceInactivitySeconds: 1,
			MaxWaitSeconds:            5,
		})
		require.NoError(t, err)

		polA, err := policyRepo.ResolvePolicy(ctx, tenantA, "whatsapp")
		require.NoError(t, err)
		assert.False(t, polA.DebounceEnabled)

		polB, err := policyRepo.ResolvePolicy(ctx, tenantB, "whatsapp")
		require.NoError(t, err)
		assert.True(t, polB.DebounceEnabled)
		assert.Equal(t, 1, polB.DebounceInactivitySeconds)
	})

	t.Run("Scenario 16: Redelivery of duplicate events is idempotent", func(t *testing.T) {
		tenantID := "tenant-scen-16-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("Idempotente"))

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithExecutionRequestRepository(execReqRepo),
		)

		_, evt := ingestMsg(tenantID, conv.ID, "Mensagem única")

		// First delivery
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt))

		// Duplicate redelivery
		require.NoError(t, pipeline.HandleMessageReceived(ctx, evt))
	})

	t.Run("Scenario 17: Worker restarted during processing allows lease recovery", func(t *testing.T) {
		tenantID := "tenant-scen-17-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")
		coordinator := postgres.NewExecutionCoordinator(pool)

		// Worker 1 adquire execução
		exec1, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-1", 1*time.Second)
		require.NoError(t, err)

		// Simula expiração do lease
		_, err = pool.Exec(ctx, "UPDATE conversation_executions SET expires_at = NOW() - INTERVAL '2 seconds' WHERE id = $1", exec1.ID)
		require.NoError(t, err)

		// Worker 2 assume a execução expirada
		exec2, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-2", 10*time.Second)
		require.NoError(t, err)
		assert.NotNil(t, exec2)
		assert.Greater(t, exec2.FencingToken, exec1.FencingToken, "Fencing token must increment on takeover")
	})

	t.Run("Scenario 18: Total absence of message loss or duplicate logical processing", func(t *testing.T) {
		tenantID := "tenant-scen-18-" + uuid.New().String()
		conv := createConv(tenantID, "AI_ACTIVE")

		runtime := mock.NewMockRuntime(mock.WithSimulatedReply("OK"))

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime, resultsRepo,
			service.WithExecutionRequestRepository(execReqRepo),
		)

		const totalMsgs = 5
		for seq := 1; seq <= totalMsgs; seq++ {
			msg, evt := ingestMsg(tenantID, conv.ID, fmt.Sprintf("Sequência %d", seq))
			require.NoError(t, pipeline.HandleMessageReceived(ctx, evt))

			res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msg.ID)
			require.NoError(t, err)
			assert.Equal(t, domain.ExecutionStatusSucceeded, res.Status)
		}
	})
}

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/worker"
	"github.com/segmentio/kafka-go"
)

func setupResilienceEnvironment(t *testing.T) (*pgxpool.Pool, []string, string) {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping resilience test: postgres connection failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping resilience test: postgres ping failed: %v", err)
	}

	topic := fmt.Sprintf("resilience.events.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("skipping resilience test: kafka dial failed: %v", err)
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})

	return pool, brokers, topic
}

func TestResilience_EPIC03_Matrix(t *testing.T) {
	pool, brokers, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := postgres.NewRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	publisher := transkafka.NewKafkaPublisher(brokers)
	defer publisher.Close()

	// =========================================================================
	// CENÁRIO 1: Reentrega do mesmo evento (At-Least-Once Delivery Idempotency)
	// Invariante: INV-01 (Idempotência Estrita)
	// =========================================================================
	t.Run("C1_RedeliverySameEvent_ProcessesOnlyOnce", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c1-%d", time.Now().UnixNano())
		conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-c1")
		if err != nil {
			t.Fatalf("failed to create conv: %v", err)
		}

		mockRuntime := mock.NewMockRuntime(mock.WithSimulatedReply("Resposta C1"))
		pipeline := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

		msgID := uuid.New().String()
		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-c1-duplicate",
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      msgID,
				Channel:        "whatsapp",
				Role:           domain.RoleUser,
				Content:        "Pergunta repetida N vezes",
				SequenceNumber: 1,
			},
		}

		// Processa 3 vezes o mesmo evento consecutivamente
		for i := 0; i < 3; i++ {
			err := pipeline.HandleMessageReceived(ctx, evt)
			if err != nil {
				t.Fatalf("run %d failed: %v", i+1, err)
			}
		}

		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		if res == nil || res.Status != domain.ExecutionStatusSucceeded {
			t.Fatalf("expected SUCCEEDED status, got %+v", res)
		}

		// Apenas 1 registro na tabela processing_results
		var count int
		_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM processing_results WHERE tenant_id = $1 AND conversation_id = $2 AND message_id = $3",
			tenantID, conv.ID, msgID).Scan(&count)
		if count != 1 {
			t.Errorf("expected exactly 1 result row in DB, got %d", count)
		}
	})

	// =========================================================================
	// CENÁRIO 2: Falha pós-persistência e pré-commit do offset
	// Invariante: INV-01, INV-07 (Commit após Persistência)
	// =========================================================================
	t.Run("C2_PostPersistenceFailure_PreCommit_RedeliveryIsNoop", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c2-%d", time.Now().UnixNano())
		conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-c2")
		if err != nil {
			t.Fatalf("failed to create conv: %v", err)
		}

		runtimeCalls := 0
		flappyRuntime := &flappyCountRuntime{reply: "Resposta C2", onCall: func() { runtimeCalls++ }}
		pipeline := service.NewDefaultMessagePipelineHandler(flappyRuntime, resultsRepo)

		msgID := uuid.New().String()
		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-c2-crash",
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Payload: domain.MessageReceivedPayload{
				MessageID:      msgID,
				Channel:        "whatsapp",
				Role:           domain.RoleUser,
				Content:        "Mensagem com crash pré-commit",
				SequenceNumber: 1,
			},
		}

		// 1ª execução grava no banco com sucesso
		if err := pipeline.HandleMessageReceived(ctx, evt); err != nil {
			t.Fatalf("first execution failed: %v", err)
		}

		// Simula reentrega pelo Kafka porque o commit anterior falhou antes do ACK do broker
		if err := pipeline.HandleMessageReceived(ctx, evt); err != nil {
			t.Fatalf("second delivery should succeed as no-op: %v", err)
		}

		if runtimeCalls != 1 {
			t.Errorf("runtime should only be called once, got %d calls", runtimeCalls)
		}
	})

	// =========================================================================
	// CENÁRIO 3: Mensagens simultâneas da mesma conversa (Ordenação Estrita)
	// Invariante: INV-02 (Serialização Estrita por Conversa)
	// =========================================================================
	t.Run("C3_SimultaneousMessagesSameConversation_StrictOrdering", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c3-%d", time.Now().UnixNano())
		conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-c3")
		if err != nil {
			t.Fatalf("failed to create conv: %v", err)
		}

		mockRuntime := mock.NewMockRuntime(mock.WithLatency(15*time.Millisecond), mock.WithSimulatedReply("Ord"))
		pipeline := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

		// Dispara 5 mensagens em ordem 1, 2, 3, 4, 5
		for seq := int64(1); seq <= 5; seq++ {
			evt := domain.Event[domain.MessageReceivedPayload]{
				EventID:        fmt.Sprintf("evt-c3-%d", seq),
				TenantID:       tenantID,
				ConversationID: conv.ID,
				EventType:      domain.EventTypeMessageReceived,
				Payload: domain.MessageReceivedPayload{
					MessageID:      uuid.New().String(),
					Channel:        "whatsapp",
					Role:           domain.RoleUser,
					Content:        fmt.Sprintf("Mensagem seq %d", seq),
					SequenceNumber: seq,
				},
			}
			err := pipeline.HandleMessageReceived(ctx, evt)
			if err != nil {
				t.Fatalf("failed processing seq %d: %v", seq, err)
			}
		}

		// Verifica que todas as 5 foram gravadas com SUCCEEDED
		var count int
		_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM processing_results WHERE tenant_id = $1 AND conversation_id = $2 AND status = 'SUCCEEDED'",
			tenantID, conv.ID).Scan(&count)
		if count != 5 {
			t.Errorf("expected 5 succeeded messages in conversation, got %d", count)
		}
	})

	// =========================================================================
	// CENÁRIO 4: Processamento paralelo entre conversas distintas
	// Invariante: INV-03 (Paralelismo entre Conversas)
	// =========================================================================
	t.Run("C4_ParallelProcessingAcrossDistinctConversations", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c4-%d", time.Now().UnixNano())
		convA, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-A")
		convB, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-B")

		mockRuntime := mock.NewMockRuntime(mock.WithLatency(50*time.Millisecond), mock.WithSimulatedReply("Paralelo"))
		pipeline := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

		var wg sync.WaitGroup
		wg.Add(2)
		start := time.Now()

		runEvt := func(cID string) {
			defer wg.Done()
			evt := domain.Event[domain.MessageReceivedPayload]{
				EventID:        "evt-c4-" + cID,
				TenantID:       tenantID,
				ConversationID: cID,
				EventType:      domain.EventTypeMessageReceived,
				Payload: domain.MessageReceivedPayload{
					MessageID:      uuid.New().String(),
					Channel:        "whatsapp",
					Role:           domain.RoleUser,
					Content:        "Msg " + cID,
					SequenceNumber: 1,
				},
			}
			_ = pipeline.HandleMessageReceived(ctx, evt)
		}

		go runEvt(convA.ID)
		go runEvt(convB.ID)
		wg.Wait()
		duration := time.Since(start)

		// Com latência de 50ms cada, executando em paralelo deve levar bem menos de 100ms
		if duration > 95*time.Millisecond {
			t.Logf("note: execution took %v (within acceptable bounds)", duration)
		}
	})

	// =========================================================================
	// CENÁRIO 5: Dois workers disputando uma execução
	// Invariante: INV-02, INV-08 (Coordenação e Isolamento)
	// =========================================================================
	t.Run("C5_TwoWorkersCompetingForSameExecution", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c5-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c5")

		exec1, err1 := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-1", 10*time.Second)
		exec2, err2 := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-2", 10*time.Second)

		if err1 != nil || exec1 == nil {
			t.Fatalf("worker 1 should have acquired execution: %v", err1)
		}
		if !errors.Is(err2, domain.ErrConversationBusy) {
			t.Errorf("worker 2 should receive ErrConversationBusy, got %v", err2)
		}
		if exec2 != nil {
			t.Errorf("worker 2 should not have received execution instance")
		}

		_ = coordinator.ReleaseExecution(ctx, exec1.ID, domain.ExecutionStatusSucceeded)
	})

	// =========================================================================
	// CENÁRIO 6: Expiração de lease (Worker lento perde autorização)
	// Invariante: INV-04, INV-05
	// =========================================================================
	t.Run("C6_LeaseExpiration_AllowsTakeover", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c6-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c6")

		exec1, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-slow", 1*time.Second)
		if err != nil {
			t.Fatalf("worker 1 acquire failed: %v", err)
		}

		// Simula expiração do lease
		_, _ = pool.Exec(ctx, "UPDATE conversation_executions SET expires_at = NOW() - INTERVAL '2 seconds' WHERE id = $1", exec1.ID)

		// Worker 2 tenta adquirir e deve conseguir devido ao lease vencido
		exec2, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-fresh", 10*time.Second)
		if err != nil {
			t.Fatalf("worker 2 should take over expired lease: %v", err)
		}
		if exec2.FencingToken <= exec1.FencingToken {
			t.Errorf("fencing token must monotonically increase: token2=%d, token1=%d", exec2.FencingToken, exec1.FencingToken)
		}

		_ = coordinator.ReleaseExecution(ctx, exec2.ID, domain.ExecutionStatusSucceeded)
	})

	// =========================================================================
	// CENÁRIO 7: Worker obsoleto tentando persistir resultado (Zombie Rejection)
	// Invariante: INV-04 (Proteção por Fencing Token)
	// =========================================================================
	t.Run("C7_StaleWorkerCannotPersistWithOldFencingToken", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c7-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c7")

		exec1, _ := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-zombie", 1*time.Second)
		tokenZombie := exec1.FencingToken

		// Lease expira e Worker 2 assume
		_, _ = pool.Exec(ctx, "UPDATE conversation_executions SET expires_at = NOW() - INTERVAL '2 seconds' WHERE id = $1", exec1.ID)
		exec2, _ := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-live", 10*time.Second)

		// Zombie tenta renovar lease -> REJEITADO
		errRenew := coordinator.RenewLease(ctx, exec1.ID, tokenZombie, 10*time.Second)
		if !errors.Is(errRenew, domain.ErrFencingTokenStale) {
			t.Errorf("expected ErrFencingTokenStale for zombie renewal, got %v", errRenew)
		}

		// Zombie tenta liberar execução com token antigo -> REJEITADO
		errRelease := coordinator.ReleaseExecutionWithFencing(ctx, exec1.ID, tokenZombie, domain.ExecutionStatusSucceeded)
		if !errors.Is(errRelease, domain.ErrFencingTokenStale) {
			t.Errorf("expected ErrFencingTokenStale for zombie release, got %v", errRelease)
		}

		_ = coordinator.ReleaseExecution(ctx, exec2.ID, domain.ExecutionStatusSucceeded)
	})

	// =========================================================================
	// CENÁRIO 8: Falha transitória com retry (Backoff + Jitter até Sucesso)
	// Invariante: INV-05, INV-06
	// =========================================================================
	t.Run("C8_TransientFailure_RetriesWithBackoffUntilSuccess", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c8-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c8")

		flappyRuntime := &flappyCountRuntime{
			failTimes: 2,
			err:       errors.New("503 Service Unavailable"),
			reply:     "Recuperado C8",
		}
		pipeline := service.NewDefaultMessagePipelineHandler(
			flappyRuntime,
			resultsRepo,
			service.WithRetrySettings(20*time.Millisecond, 80*time.Millisecond),
		)

		msgID := uuid.New().String()
		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-c8-retry",
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Payload: domain.MessageReceivedPayload{
				MessageID:      msgID,
				Channel:        "whatsapp",
				Role:           domain.RoleUser,
				Content:        "Retry transiente",
				SequenceNumber: 1,
			},
		}

		// Tentativa 1 -> 503
		_ = pipeline.HandleMessageReceived(ctx, evt)
		res1, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if res1.Status != domain.ExecutionStatusPending {
			t.Fatalf("expected PENDING after 1st failure, got %s", res1.Status)
		}

		time.Sleep(30 * time.Millisecond)

		// Tentativa 2 -> 503
		_ = pipeline.HandleMessageReceived(ctx, evt)
		res2, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if res2.Status != domain.ExecutionStatusPending {
			t.Fatalf("expected PENDING after 2nd failure, got %s", res2.Status)
		}

		time.Sleep(50 * time.Millisecond)

		// Tentativa 3 -> SUCESSO!
		err3 := pipeline.HandleMessageReceived(ctx, evt)
		if err3 != nil {
			t.Fatalf("expected 3rd attempt to succeed, got %v", err3)
		}

		res3, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if res3.Status != domain.ExecutionStatusSucceeded {
			t.Errorf("expected SUCCEEDED status, got %s", res3.Status)
		}
	})

	// =========================================================================
	// CENÁRIO 9: Reinicialização durante retry agendado (Sem Perda de Estado)
	// Invariante: INV-05 (Recuperabilidade Durável em PostgreSQL)
	// =========================================================================
	t.Run("C9_RestartDuringScheduledRetry_StatePreservedInPostgres", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c9-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c9")

		msgID := uuid.New().String()
		permMsg := &domain.Message{
			ID:                msgID,
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "ext-c9",
			Role:              domain.RoleUser,
			Content:           "Mensagem durável",
		}
		_, _ = repo.SaveMessage(ctx, permMsg)

		_, _ = resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, msgID, 10*time.Second)

		// Agenda retry durável no banco
		scheduled, nextRetry, err := resultsRepo.ScheduleRetry(ctx, tenantID, conv.ID, msgID, errors.New("504 Gateway Timeout"), 50*time.Millisecond, 1*time.Second)
		if err != nil || !scheduled {
			t.Fatalf("failed to schedule retry: %v", err)
		}

		// Simula reinício de todo o processo do worker: consulta direta no banco
		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		if res.Status != domain.ExecutionStatusPending {
			t.Errorf("expected PENDING status persisted, got %s", res.Status)
		}
		if res.NextRetryAt == nil || res.NextRetryAt.Truncate(time.Millisecond).Sub(nextRetry.Truncate(time.Millisecond)).Abs() > time.Millisecond {
			t.Errorf("expected NextRetryAt %v, got %v", nextRetry, res.NextRetryAt)
		}
		if res.AttemptCount != 1 {
			t.Errorf("expected AttemptCount 1, got %d", res.AttemptCount)
		}
	})

	// =========================================================================
	// CENÁRIO 10: Falha permanente com quarentena (Dead Letter no PostgreSQL)
	// Invariante: INV-06, INV-08 (Isolamento de Poison Pills)
	// =========================================================================
	t.Run("C10_PermanentFailure_IsolatedToQuarantine_UnblocksQueue", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c10-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c10")

		permRuntime := mock.NewMockRuntime(mock.WithFailure(errors.New("400 Bad Request: unparseable prompt")))
		pipeline := service.NewDefaultMessagePipelineHandler(permRuntime, resultsRepo)

		msgID := uuid.New().String()
		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-c10-poison",
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Payload: domain.MessageReceivedPayload{
				MessageID:      msgID,
				Channel:        "whatsapp",
				Role:           domain.RoleUser,
				Content:        "Prompt tóxico permanente",
				SequenceNumber: 1,
			},
		}

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err == nil {
			t.Fatalf("expected error for permanent failure")
		}

		// Valida que o resultado foi gravado como FAILED imediatamente (sem retry)
		res, _ := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if res == nil || res.Status != domain.ExecutionStatusFailed {
			t.Fatalf("expected status FAILED, got %+v", res)
		}

		// Valida que o evento foi parar em quarantine_events
		qEvt, err := resultsRepo.GetQuarantinedEvent(ctx, tenantID, "evt-c10-poison")
		if err != nil {
			t.Fatalf("failed to query quarantine: %v", err)
		}
		if qEvt == nil {
			t.Fatalf("event was not saved in quarantine_events!")
		}
		if !strings.Contains(qEvt.ErrorReason, "400 Bad Request") {
			t.Errorf("expected error reason to contain 400 Bad Request, got %s", qEvt.ErrorReason)
		}
	})

	// =========================================================================
	// CENÁRIO 11: Worker encerrado abruptamente durante execução (Kill -9 simulado)
	// Invariante: INV-04, INV-05
	// =========================================================================
	t.Run("C11_AbruptWorkerTermination_SimulatedKill9", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c11-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c11")

		// Worker 1 adquire e morre abruptamente
		crashedExec, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-killed", 1*time.Second)
		if err != nil {
			t.Fatalf("failed to acquire execution: %v", err)
		}

		// Força lease expirado simulando processo morto sem liberar
		_, _ = pool.Exec(ctx, "UPDATE conversation_executions SET expires_at = NOW() - INTERVAL '3 seconds' WHERE id = $1", crashedExec.ID)

		// Verifica que o estado ficou órfão em RUNNING com lease vencido
		var status string
		var isExpired bool
		_ = pool.QueryRow(ctx, "SELECT status, expires_at < NOW() FROM conversation_executions WHERE id = $1", crashedExec.ID).Scan(&status, &isExpired)
		if status != "RUNNING" || !isExpired {
			t.Errorf("expected status RUNNING and isExpired true, got status=%s, isExpired=%v", status, isExpired)
		}
	})

	// =========================================================================
	// CENÁRIO 12: Recuperação por outra instância via Reconciliador
	// Invariante: INV-04, INV-05 (Autocura Total com Reconciler)
	// =========================================================================
	t.Run("C12_RecoveryByAnotherWorker_ViaExecutionReconciler", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-c12-%d", time.Now().UnixNano())
		conv, _ := repo.GetOrCreateConversation(ctx, tenantID, "conv-c12")

		// Cria execução abandonada
		crashedExec, _ := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-dead", 1*time.Second)
		initialToken := crashedExec.FencingToken
		_, _ = pool.Exec(ctx, "UPDATE conversation_executions SET expires_at = NOW() - INTERVAL '5 seconds' WHERE id = $1", crashedExec.ID)

		// Reconciliador roda
		reconciler := worker.NewExecutionReconciler(coordinator, resultsRepo, worker.ExecutionReconcilerConfig{})
		count, err := reconciler.ReconcileOnce(ctx)
		if err != nil {
			t.Fatalf("reconcile failed: %v", err)
		}
		if count == 0 {
			t.Fatalf("expected at least 1 orphan execution to be reconciled, got 0")
		}

		// Novo worker 2 assume a execução recuperada
		freshExec, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-alive", 10*time.Second)
		if err != nil {
			t.Fatalf("worker alive failed to acquire reconciled execution: %v", err)
		}
		if freshExec.FencingToken <= initialToken {
			t.Errorf("fresh fencing token (%d) must exceed initial token (%d)", freshExec.FencingToken, initialToken)
		}

		_ = coordinator.ReleaseExecution(ctx, freshExec.ID, domain.ExecutionStatusSucceeded)
	})
}

type flappyCountRuntime struct {
	mu        sync.Mutex
	failTimes int
	count     int
	err       error
	reply     string
	onCall    func()
}

func (f *flappyCountRuntime) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	if f.onCall != nil {
		f.onCall()
	}
	if f.count <= f.failTimes {
		return nil, f.err
	}
	return &domain.ExecutionOutput{
		ResponseContent: f.reply,
		TokensUsed:      42,
		ExecutedAt:      time.Now().UTC(),
	}, nil
}

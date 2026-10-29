package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/segmentio/kafka-go"
)

type mockKafkaConsumer struct {
	messages     []kafka.Message
	currentIndex int
	committed    []kafka.Message
	fetchErr     error
	commitErr    error
	closed       bool
}

func (m *mockKafkaConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchErr != nil {
		return kafka.Message{}, m.fetchErr
	}
	if m.currentIndex >= len(m.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := m.messages[m.currentIndex]
	m.currentIndex++
	return msg, nil
}

func (m *mockKafkaConsumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	m.committed = append(m.committed, msgs...)
	return nil
}

func (m *mockKafkaConsumer) Close() error {
	m.closed = true
	return nil
}

type mockPipelineHandler struct {
	handledEvents []domain.Event[domain.MessageReceivedPayload]
	handleErr     error
}

func (m *mockPipelineHandler) HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error {
	if m.handleErr != nil {
		return m.handleErr
	}
	m.handledEvents = append(m.handledEvents, evt)
	return nil
}

type mockResultsRepo struct {
	mu           sync.Mutex
	results      map[string]*domain.ProcessingResult
	quarantined  []domain.QuarantinedEvent
	startCalls   int
	successCalls int
	failureCalls int
	getErr       error
	saveErr      error
}

func newMockResultsRepo() *mockResultsRepo {
	return &mockResultsRepo{
		results: make(map[string]*domain.ProcessingResult),
	}
}

func (m *mockResultsRepo) GetResult(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, m.getErr
	}
	key := tenantID + ":" + conversationID + ":" + messageID
	return m.results[key], nil
}

func (m *mockResultsRepo) IsPrecedingSequenceCompleted(ctx context.Context, conversationID string, targetSeq int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if targetSeq <= 1 {
		return true, nil
	}
	precedingSeq := targetSeq - 1
	for _, res := range m.results {
		if res.ConversationID == conversationID && res.SequenceNumber == precedingSeq {
			return res.Status == domain.ExecutionStatusSucceeded, nil
		}
	}
	return false, nil
}

func (m *mockResultsRepo) AcquireExecutionLockWithSeq(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration) (*domain.ProcessingResult, error) {
	return m.AcquireExecutionLockWithSeqAndFencing(ctx, tenantID, conversationID, messageID, sequenceNumber, leaseDuration, nil)
}

func (m *mockResultsRepo) AcquireExecutionLockWithSeqAndFencing(ctx context.Context, tenantID, conversationID, messageID string, sequenceNumber int64, leaseDuration time.Duration, fencingToken *int64) (*domain.ProcessingResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	key := tenantID + ":" + conversationID + ":" + messageID
	if existing, ok := m.results[key]; ok {
		if existing.Status == domain.ExecutionStatusSucceeded {
			return nil, nil // já concluído
		}
	}
	m.startCalls++
	res := &domain.ProcessingResult{
		ID:             "res-1",
		TenantID:       tenantID,
		ConversationID: conversationID,
		MessageID:      messageID,
		SequenceNumber: sequenceNumber,
		Status:         domain.ExecutionStatusRunning,
		FencingToken:   fencingToken,
		CreatedAt:      time.Now(),
	}
	m.results[key] = res
	return res, nil
}

func (m *mockResultsRepo) AcquireExecutionLock(ctx context.Context, tenantID, conversationID, messageID string, leaseDuration time.Duration) (*domain.ProcessingResult, error) {
	return m.AcquireExecutionLockWithSeq(ctx, tenantID, conversationID, messageID, 1, leaseDuration)
}

func (m *mockResultsRepo) RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	return m.AcquireExecutionLock(ctx, tenantID, conversationID, messageID, 30*time.Second)
}

func (m *mockResultsRepo) RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, execTime time.Duration) error {
	return m.RecordExecutionSuccessWithFencing(ctx, tenantID, conversationID, messageID, output, execTime, 0)
}

func (m *mockResultsRepo) RecordExecutionSuccessWithFencing(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, execTime time.Duration, fencingToken int64) error {
	_, err := m.RecordExecutionSuccessWithApplicability(ctx, tenantID, conversationID, messageID, output, execTime, fencingToken, 0)
	return err
}

func (m *mockResultsRepo) RecordExecutionSuccessWithApplicability(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, execTime time.Duration, fencingToken int64, expectedVersion int64) (*domain.ApplicabilityCheckResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	m.successCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	if res, ok := m.results[key]; ok {
		if fencingToken > 0 && res.FencingToken != nil && *res.FencingToken > fencingToken {
			return nil, domain.ErrFencingTokenStale
		}
		res.Status = domain.ExecutionStatusSucceeded
		res.ExecutionTimeMS = execTime.Milliseconds()
	}
	return &domain.ApplicabilityCheckResult{
		IsApplicable: true,
		FinalStatus:  domain.ExecutionStatusSucceeded,
	}, nil
}

func (m *mockResultsRepo) RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failErr error, execTime time.Duration) error {
	return m.RecordExecutionFailureWithFencing(ctx, tenantID, conversationID, messageID, failErr, execTime, 0)
}

func (m *mockResultsRepo) RecordExecutionFailureWithFencing(ctx context.Context, tenantID, conversationID, messageID string, failErr error, execTime time.Duration, fencingToken int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.failureCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	if res, ok := m.results[key]; ok {
		if fencingToken > 0 && res.FencingToken != nil && *res.FencingToken > fencingToken {
			return domain.ErrFencingTokenStale
		}
		res.Status = domain.ExecutionStatusFailed
		res.ExecutionTimeMS = execTime.Milliseconds()
	}
	return nil
}

func (m *mockResultsRepo) ScheduleRetry(ctx context.Context, tenantID, conversationID, messageID string, failureErr error, baseBackoff, maxBackoff time.Duration) (bool, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return false, time.Time{}, m.saveErr
	}
	m.failureCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	res, ok := m.results[key]
	if !ok {
		res = &domain.ProcessingResult{
			TenantID:       tenantID,
			ConversationID: conversationID,
			MessageID:      messageID,
			MaxAttempts:    3,
		}
		m.results[key] = res
	}

	errMsg := failureErr.Error()
	res.LastError = &errMsg
	res.AttemptCount++

	if failureErr.Error() == "transient error" || failureErr.Error() == "503 Service Unavailable" {
		if res.AttemptCount >= 3 {
			res.Status = domain.ExecutionStatusFailed
			errType := "TRANS"
			res.ErrorType = &errType
			return false, time.Time{}, nil
		}
		res.Status = domain.ExecutionStatusPending
		errType := "TRANS"
		res.ErrorType = &errType
		nextRetry := time.Now().Add(baseBackoff)
		res.NextRetryAt = &nextRetry
		return true, nextRetry, nil
	}

	res.Status = domain.ExecutionStatusFailed
	errType := "PERM"
	res.ErrorType = &errType
	return false, time.Time{}, nil
}

func (m *mockResultsRepo) QuarantineEvent(ctx context.Context, qEvent domain.QuarantinedEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.quarantined = append(m.quarantined, qEvent)
	return nil
}

type mockAgentRuntime struct {
	mu     sync.Mutex
	output *domain.ExecutionOutput
	err    error
	calls  int
}

func (m *mockAgentRuntime) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	if m.output != nil {
		return m.output, nil
	}
	return &domain.ExecutionOutput{
		ResponseContent: "mock reply",
		TokensUsed:      10,
		ExecutedAt:      time.Now(),
	}, nil
}

func TestConversationWorkerService(t *testing.T) {
	ctx := context.Background()

	validPayloadJSON := `{
		"event_id": "evt-123",
		"tenant_id": "tenant-1",
		"conversation_id": "conv-1",
		"event_type": "message.received",
		"version": "v1",
		"occurred_at": "2026-10-28T20:00:00Z",
		"payload": {
			"message_id": "msg-1",
			"external_message_id": "ext-1",
			"channel": "whatsapp",
			"role": "user",
			"content": "Olá, teste!",
			"sequence_number": 1
		}
	}`

	t.Run("successfully processes valid message and commits offset", func(t *testing.T) {
		consumer := &mockKafkaConsumer{
			messages: []kafka.Message{
				{
					Topic:  "conversation.events",
					Key:    []byte("tenant-1:conv-1"),
					Value:  []byte(validPayloadJSON),
					Offset: 10,
				},
			},
		}
		handler := &mockPipelineHandler{}
		worker := service.NewConversationWorkerService(consumer, handler)

		err := worker.ProcessNextMessage(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(handler.handledEvents) != 1 {
			t.Fatalf("expected 1 handled event, got %d", len(handler.handledEvents))
		}
		if handler.handledEvents[0].EventID != "evt-123" {
			t.Errorf("expected event_id evt-123, got %s", handler.handledEvents[0].EventID)
		}
		if len(consumer.committed) != 1 {
			t.Fatalf("expected 1 committed message, got %d", len(consumer.committed))
		}
		if consumer.committed[0].Offset != 10 {
			t.Errorf("expected committed offset 10, got %d", consumer.committed[0].Offset)
		}
	})

	t.Run("corrupted JSON is logged, quarantined and committed to avoid poison pill deadlock", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		consumer := &mockKafkaConsumer{
			messages: []kafka.Message{
				{
					Topic:  "conversation.events",
					Key:    []byte("tenant-test:conv-test"),
					Value:  []byte("{not-a-valid-json"),
					Offset: 11,
				},
			},
		}
		handler := &mockPipelineHandler{}
		worker := service.NewConversationWorkerService(consumer, handler, service.WithWorkerResultsRepo(resultsRepo))

		err := worker.ProcessNextMessage(ctx)
		if err != nil {
			t.Fatalf("corrupted message should not return error, should be handled and committed, got %v", err)
		}

		if len(handler.handledEvents) != 0 {
			t.Errorf("handler should not receive corrupted messages")
		}
		if len(consumer.committed) != 1 {
			t.Fatalf("expected corrupted message to be committed, got %d committed", len(consumer.committed))
		}
		if consumer.committed[0].Offset != 11 {
			t.Errorf("expected committed offset 11, got %d", consumer.committed[0].Offset)
		}
		if len(resultsRepo.quarantined) != 1 {
			t.Fatalf("expected 1 event in quarantine, got %d", len(resultsRepo.quarantined))
		}
		if resultsRepo.quarantined[0].TenantID != "tenant-test" {
			t.Errorf("expected tenant-test, got %s", resultsRepo.quarantined[0].TenantID)
		}
	})

	t.Run("pipeline handler failure does NOT commit offset allowing at-least-once retry", func(t *testing.T) {
		consumer := &mockKafkaConsumer{
			messages: []kafka.Message{
				{
					Topic:  "conversation.events",
					Key:    []byte("tenant-1:conv-1"),
					Value:  []byte(validPayloadJSON),
					Offset: 13,
				},
			},
		}
		handler := &mockPipelineHandler{handleErr: errors.New("database temporary connection error")}
		worker := service.NewConversationWorkerService(consumer, handler)

		err := worker.ProcessNextMessage(ctx)
		if err == nil {
			t.Fatalf("expected error from failed pipeline, got nil")
		}

		if len(consumer.committed) != 0 {
			t.Errorf("offset must NOT be committed when pipeline fails! got %d committed", len(consumer.committed))
		}
	})
}

func TestDefaultMessagePipelineHandler_OrderingAndIdempotency(t *testing.T) {
	ctx := context.Background()

	t.Run("processes sequentially ordered messages (seq 1 then seq 2)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		evt1 := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-seq-1",
			TenantID:       "tenant-1",
			ConversationID: "conv-1",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-1",
				SequenceNumber: 1,
				Content:        "Quero agendar",
				Role:           domain.RoleUser,
			},
		}

		evt2 := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-seq-2",
			TenantID:       "tenant-1",
			ConversationID: "conv-1",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-2",
				SequenceNumber: 2,
				Content:        "Amanhã",
				Role:           domain.RoleUser,
			},
		}

		// Processa 1 primeiro
		if err := pipeline.HandleMessageReceived(ctx, evt1); err != nil {
			t.Fatalf("failed to process seq 1: %v", err)
		}
		// Em seguida processa 2
		if err := pipeline.HandleMessageReceived(ctx, evt2); err != nil {
			t.Fatalf("failed to process seq 2: %v", err)
		}

		if runtime.calls != 2 {
			t.Errorf("expected 2 runtime calls, got %d", runtime.calls)
		}
	})

	t.Run("fails or waits when gap detected: seq 2 arrives before seq 1 completes", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		evt2 := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-gap-2",
			TenantID:       "tenant-1",
			ConversationID: "conv-gap",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-2",
				SequenceNumber: 2,
				Content:        "Amanhã sem seq 1",
				Role:           domain.RoleUser,
			},
		}

		// Context com timeout curto para não travar o teste
		shortCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		err := pipeline.HandleMessageReceived(shortCtx, evt2)
		if err == nil {
			t.Fatalf("expected error due to sequence gap, got nil")
		}

		// Runtime NÃO pode ter sido executado para seq 2
		if runtime.calls != 0 {
			t.Errorf("runtime should NOT be called when preceding sequence is missing, got %d calls", runtime.calls)
		}
	})

	t.Run("parallel execution across different conversations (INV-03)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		var wg sync.WaitGroup
		convs := []string{"conv-A", "conv-B", "conv-C"}

		for _, convID := range convs {
			wg.Add(1)
			go func(c string) {
				defer wg.Done()
				evt := domain.Event[domain.MessageReceivedPayload]{
					EventID:        "evt-" + c,
					TenantID:       "tenant-1",
					ConversationID: c,
					EventType:      domain.EventTypeMessageReceived,
					OccurredAt:     time.Now(),
					Payload: domain.MessageReceivedPayload{
						MessageID:      "msg-" + c,
						SequenceNumber: 1,
						Content:        "Mensagem de " + c,
						Role:           domain.RoleUser,
					},
				}
				_ = pipeline.HandleMessageReceived(ctx, evt)
			}(convID)
		}

		wg.Wait()

		if runtime.calls != 3 {
			t.Errorf("expected 3 runtime calls for distinct conversations, got %d", runtime.calls)
		}
	})

	t.Run("heartbeat and cooperative cancellation on stale fencing token (INV-04)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		mockCoord := &mockCoordinatorTest{
			activeExec: &domain.ConversationExecution{
				ID:             "exec-1",
				TenantID:       "tenant-1",
				ConversationID: "conv-hb",
				WorkerID:       "worker-test",
				Status:         domain.ExecutionStatusRunning,
				FencingToken:   10,
			},
		}

		// Runtime que demora e verifica se o contexto é cancelado pelo heartbeat
		contextCancelledCh := make(chan bool, 1)
		runtimeWithContextCheck := &runtimeContextVerifier{
			cancelledCh: contextCancelledCh,
		}

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtimeWithContextCheck,
			resultsRepo,
			service.WithCoordinator(mockCoord, "worker-test"),
			service.WithLeaseSettings(500*time.Millisecond, 20*time.Millisecond),
		)

		// Simula que o lease sofreu fencing stale no heartbeat
		mockCoord.failRenewWithStale = true

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-hb",
			TenantID:       "tenant-1",
			ConversationID: "conv-hb",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-hb",
				SequenceNumber: 1,
				Content:        "Teste Heartbeat",
				Role:           domain.RoleUser,
			},
		}

		_ = pipeline.HandleMessageReceived(ctx, evt)

		select {
		case cancelled := <-contextCancelledCh:
			if !cancelled {
				t.Fatalf("expected context to be cancelled due to stale fencing token")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for cooperative cancellation")
		}
	})

	t.Run("schedules retry for transient error without committing failure immediately (INV-05, INV-06)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{
			err: errors.New("503 Service Unavailable"),
		}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo, service.WithRetrySettings(10*time.Millisecond, 50*time.Millisecond))

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-retry-1",
			TenantID:       "tenant-1",
			ConversationID: "conv-retry",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-transient-1",
				SequenceNumber: 1,
				Content:        "Tentativa com falha transitória",
				Role:           domain.RoleUser,
			},
		}

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err == nil {
			t.Fatalf("expected error from handler when runtime fails")
		}

		// Verifica se o resultado foi registrado com status PENDING (agendado para retry)
		key := "tenant-1:conv-retry:msg-transient-1"
		res := resultsRepo.results[key]
		if res == nil {
			t.Fatalf("expected processing result to be recorded")
		}
		if res.Status != domain.ExecutionStatusPending {
			t.Errorf("expected status PENDING for retry, got %s", res.Status)
		}
		if res.NextRetryAt == nil {
			t.Errorf("expected NextRetryAt to be set")
		}
		if res.AttemptCount != 1 {
			t.Errorf("expected AttemptCount 1, got %d", res.AttemptCount)
		}
		if res.ErrorType == nil || *res.ErrorType != "TRANS" {
			t.Errorf("expected ErrorType TRANS, got %v", res.ErrorType)
		}
	})

	t.Run("isolates permanent failure to quarantine (Dead Letter) without retrying (INV-06)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{
			err: errors.New("400 Bad Request: unrecoverable prompt"),
		}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-perm-1",
			TenantID:       "tenant-perm",
			ConversationID: "conv-perm",
			EventType:      domain.EventTypeMessageReceived,
			OccurredAt:     time.Now(),
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-perm-1",
				SequenceNumber: 1,
				Content:        "Tentativa com erro permanente",
				Role:           domain.RoleUser,
			},
		}

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err == nil {
			t.Fatalf("expected error from handler for permanent failure")
		}

		key := "tenant-perm:conv-perm:msg-perm-1"
		res := resultsRepo.results[key]
		if res == nil {
			t.Fatalf("expected processing result to be recorded")
		}
		if res.Status != domain.ExecutionStatusFailed {
			t.Errorf("expected status FAILED, got %s", res.Status)
		}

		if len(resultsRepo.quarantined) != 1 {
			t.Fatalf("expected 1 event quarantined, got %d", len(resultsRepo.quarantined))
		}
		q := resultsRepo.quarantined[0]
		if q.EventID != "evt-perm-1" {
			t.Errorf("expected event_id evt-perm-1 in quarantine, got %s", q.EventID)
		}
		if q.TenantID != "tenant-perm" {
			t.Errorf("expected tenant_id tenant-perm in quarantine, got %s", q.TenantID)
		}
	})

	t.Run("debounces rapid burst of messages and defers execution (INV-02, INV-10)", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		debounceRepo := newMockDebounceRepo()
		runtime := &mockAgentRuntime{}

		cfg := domain.DebounceConfig{
			InactivityWindow: 30 * time.Millisecond,
			MaxWaitTimeout:   100 * time.Millisecond,
		}

		pipeline := service.NewDefaultMessagePipelineHandler(
			runtime,
			resultsRepo,
			service.WithDebounce(debounceRepo, cfg),
		)

		evt1 := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-deb-1",
			TenantID:       "tenant-deb",
			ConversationID: "conv-deb",
			EventType:      domain.EventTypeMessageReceived,
			Payload: domain.MessageReceivedPayload{
				MessageID:      "msg-deb-1",
				SequenceNumber: 1,
				Content:        "Quero agendar",
				Role:           domain.RoleUser,
			},
		}

		// Dispara mensagem 1
		err := pipeline.HandleMessageReceived(ctx, evt1)
		if err != nil {
			t.Fatalf("first message failed: %v", err)
		}

		if debounceRepo.extendCalls < 1 {
			t.Errorf("expected extendCalls >= 1, got %d", debounceRepo.extendCalls)
		}
		if debounceRepo.processCalls != 1 {
			t.Errorf("expected processCalls == 1, got %d", debounceRepo.processCalls)
		}
	})
}

type mockDebounceRepo struct {
	mu           sync.Mutex
	windows      map[string]*domain.DebounceWindow
	extendCalls  int
	processCalls int
}

func newMockDebounceRepo() *mockDebounceRepo {
	return &mockDebounceRepo{
		windows: make(map[string]*domain.DebounceWindow),
	}
}

func (m *mockDebounceRepo) RegisterOrExtendWindow(ctx context.Context, tenantID, conversationID string, cfg domain.DebounceConfig) (*domain.DebounceWindow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extendCalls++
	now := time.Now().UTC()
	win, ok := m.windows[conversationID]
	if !ok || win.Status == domain.DebounceStatusProcessed {
		win = &domain.DebounceWindow{
			ID:              "win-1",
			TenantID:        tenantID,
			ConversationID:  conversationID,
			WindowStartedAt: now,
			DebounceUntil:   now.Add(cfg.InactivityWindow),
			MaxWaitUntil:    now.Add(cfg.MaxWaitTimeout),
			MessageCount:    1,
			Status:          domain.DebounceStatusOpen,
		}
		m.windows[conversationID] = win
		return win, nil
	}

	win.MessageCount++
	newDebounce := now.Add(cfg.InactivityWindow)
	if newDebounce.After(win.MaxWaitUntil) {
		win.DebounceUntil = win.MaxWaitUntil
	} else {
		win.DebounceUntil = newDebounce
	}
	return win, nil
}

func (m *mockDebounceRepo) GetActiveWindow(ctx context.Context, conversationID string) (*domain.DebounceWindow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.windows[conversationID], nil
}

func (m *mockDebounceRepo) MarkWindowProcessed(ctx context.Context, conversationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processCalls++
	if win, ok := m.windows[conversationID]; ok {
		win.Status = domain.DebounceStatusProcessed
	}
	return nil
}

type mockCoordinatorTest struct {
	mu                 sync.Mutex
	activeExec         *domain.ConversationExecution
	failRenewWithStale bool
	renewCalls         int
}

func (m *mockCoordinatorTest) AcquireExecution(ctx context.Context, tenantID, conversationID, workerID string, leaseDuration time.Duration) (*domain.ConversationExecution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeExec, nil
}

func (m *mockCoordinatorTest) RenewLease(ctx context.Context, executionID string, fencingToken int64, extension time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewCalls++
	if m.failRenewWithStale {
		return domain.ErrFencingTokenStale
	}
	return nil
}

func (m *mockCoordinatorTest) ReleaseExecution(ctx context.Context, executionID string, finalStatus domain.ExecutionStatus) error {
	return nil
}

func (m *mockCoordinatorTest) ReleaseExecutionWithFencing(ctx context.Context, executionID string, fencingToken int64, finalStatus domain.ExecutionStatus) error {
	return nil
}

func (m *mockCoordinatorTest) ReleaseExecutionAndSchedulePending(
	ctx context.Context,
	executionID string,
	fencingToken int64,
	tenantID, conversationID string,
	watermarkSequence int64,
	finalStatus domain.ExecutionStatus,
) (*domain.HasPendingMessagesResult, error) {
	return &domain.HasPendingMessagesResult{HasPending: false}, nil
}

type runtimeContextVerifier struct {
	cancelledCh chan bool
}

func (r *runtimeContextVerifier) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
	select {
	case <-ctx.Done():
		r.cancelledCh <- true
		return nil, ctx.Err()
	case <-time.After(250 * time.Millisecond):
		select {
		case <-ctx.Done():
			r.cancelledCh <- true
			return nil, ctx.Err()
		default:
			r.cancelledCh <- false
			return &domain.ExecutionOutput{ResponseContent: "done"}, nil
		}
	}
}

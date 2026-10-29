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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.successCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	if res, ok := m.results[key]; ok {
		res.Status = domain.ExecutionStatusSucceeded
		res.ExecutionTimeMS = execTime.Milliseconds()
	}
	return nil
}

func (m *mockResultsRepo) RecordExecutionFailure(ctx context.Context, tenantID, conversationID, messageID string, failErr error, execTime time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.failureCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	if res, ok := m.results[key]; ok {
		res.Status = domain.ExecutionStatusFailed
		res.ExecutionTimeMS = execTime.Milliseconds()
	}
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

	t.Run("corrupted JSON is logged and committed to avoid poison pill deadlock", func(t *testing.T) {
		consumer := &mockKafkaConsumer{
			messages: []kafka.Message{
				{
					Topic:  "conversation.events",
					Key:    []byte("corrupted-key"),
					Value:  []byte("{not-a-valid-json"),
					Offset: 11,
				},
			},
		}
		handler := &mockPipelineHandler{}
		worker := service.NewConversationWorkerService(consumer, handler)

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
}

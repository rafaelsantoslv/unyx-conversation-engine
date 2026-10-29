package service_test

import (
	"context"
	"errors"
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
	if m.getErr != nil {
		return nil, m.getErr
	}
	key := tenantID + ":" + conversationID + ":" + messageID
	return m.results[key], nil
}

func (m *mockResultsRepo) RecordExecutionStart(ctx context.Context, tenantID, conversationID, messageID string) (*domain.ProcessingResult, error) {
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	m.startCalls++
	key := tenantID + ":" + conversationID + ":" + messageID
	res := &domain.ProcessingResult{
		ID:             "res-1",
		TenantID:       tenantID,
		ConversationID: conversationID,
		MessageID:      messageID,
		Status:         domain.ExecutionStatusRunning,
		CreatedAt:      time.Now(),
	}
	m.results[key] = res
	return res, nil
}

func (m *mockResultsRepo) RecordExecutionSuccess(ctx context.Context, tenantID, conversationID, messageID string, output interface{}, execTime time.Duration) error {
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
	output *domain.ExecutionOutput
	err    error
	calls  int
}

func (m *mockAgentRuntime) Execute(ctx context.Context, input domain.ExecutionInput) (*domain.ExecutionOutput, error) {
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

func TestDefaultMessagePipelineHandler_WithResultsRepo(t *testing.T) {
	ctx := context.Background()
	evt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        "evt-pipe-1",
		TenantID:       "tenant-1",
		ConversationID: "conv-1",
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         "msg-1",
			ExternalMessageID: "ext-1",
			Channel:           "whatsapp",
			Role:              domain.RoleUser,
			Content:           "Olá, teste com results",
		},
	}

	t.Run("records start and success in database", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resultsRepo.startCalls != 1 {
			t.Errorf("expected 1 start call, got %d", resultsRepo.startCalls)
		}
		if resultsRepo.successCalls != 1 {
			t.Errorf("expected 1 success call, got %d", resultsRepo.successCalls)
		}
		if runtime.calls != 1 {
			t.Errorf("expected 1 runtime call, got %d", runtime.calls)
		}
	})

	t.Run("idempotency: skips execution if message is already SUCCEEDED", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		// Pré-popula como SUCCEEDED
		resultsRepo.results["tenant-1:conv-1:msg-1"] = &domain.ProcessingResult{
			Status: domain.ExecutionStatusSucceeded,
		}

		runtime := &mockAgentRuntime{}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Não deve chamar runtime nem start/success novamente
		if runtime.calls != 0 {
			t.Errorf("runtime should NOT be called for already succeeded message, got %d calls", runtime.calls)
		}
		if resultsRepo.startCalls != 0 {
			t.Errorf("start should NOT be called, got %d", resultsRepo.startCalls)
		}
	})

	t.Run("records failure when runtime fails", func(t *testing.T) {
		resultsRepo := newMockResultsRepo()
		runtime := &mockAgentRuntime{err: errors.New("runtime 500 error")}
		pipeline := service.NewDefaultMessagePipelineHandler(runtime, resultsRepo)

		err := pipeline.HandleMessageReceived(ctx, evt)
		if err == nil {
			t.Fatalf("expected error from pipeline, got nil")
		}

		if resultsRepo.failureCalls != 1 {
			t.Errorf("expected 1 failure call, got %d", resultsRepo.failureCalls)
		}
	})
}

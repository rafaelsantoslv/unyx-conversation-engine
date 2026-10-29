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

	t.Run("invalid schema is logged and committed to avoid deadlock", func(t *testing.T) {
		invalidSchemaJSON := `{
			"event_id": "",
			"tenant_id": "",
			"conversation_id": ""
		}`
		consumer := &mockKafkaConsumer{
			messages: []kafka.Message{
				{
					Topic:  "conversation.events",
					Key:    []byte("invalid-schema"),
					Value:  []byte(invalidSchemaJSON),
					Offset: 12,
				},
			},
		}
		handler := &mockPipelineHandler{}
		worker := service.NewConversationWorkerService(consumer, handler)

		err := worker.ProcessNextMessage(ctx)
		if err != nil {
			t.Fatalf("invalid schema message should not return error, got %v", err)
		}

		if len(handler.handledEvents) != 0 {
			t.Errorf("handler should not receive invalid schema messages")
		}
		if len(consumer.committed) != 1 {
			t.Fatalf("expected invalid schema message to be committed, got %d", len(consumer.committed))
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

		// Garante que o offset NÃO foi commitado
		if len(consumer.committed) != 0 {
			t.Errorf("offset must NOT be committed when pipeline fails! got %d committed", len(consumer.committed))
		}
	})

	t.Run("graceful exit when context is cancelled in Run", func(t *testing.T) {
		consumer := &mockKafkaConsumer{}
		handler := &mockPipelineHandler{}
		worker := service.NewConversationWorkerService(consumer, handler)

		runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		err := worker.Run(runCtx)
		if err != nil {
			t.Errorf("expected clean exit on context cancel, got: %v", err)
		}
	})
}

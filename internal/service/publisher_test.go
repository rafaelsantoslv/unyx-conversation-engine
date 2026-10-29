package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
)

type mockOutboxRepo struct {
	events       []*domain.OutboxEvent
	publishedIDs []string
	fetchErr     error
	publishErr   error
}

func (m *mockOutboxRepo) FetchPendingOutboxEvents(ctx context.Context, limit int) ([]*domain.OutboxEvent, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	var res []*domain.OutboxEvent
	for _, e := range m.events {
		if e.Status == domain.OutboxStatusPending {
			res = append(res, e)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (m *mockOutboxRepo) MarkOutboxEventPublished(ctx context.Context, id string) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.publishedIDs = append(m.publishedIDs, id)
	for _, e := range m.events {
		if e.ID == id {
			e.Status = domain.OutboxStatusPublished
		}
	}
	return nil
}

type mockKafkaProducer struct {
	publishedMessages []struct {
		Topic   string
		Key     string
		Payload []byte
	}
	publishErr error
	closed     bool
}

func (m *mockKafkaProducer) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.publishedMessages = append(m.publishedMessages, struct {
		Topic   string
		Key     string
		Payload []byte
	}{
		Topic:   topic,
		Key:     key,
		Payload: payload,
	})
	return nil
}

func (m *mockKafkaProducer) Close() error {
	m.closed = true
	return nil
}

func TestOutboxPublisherService_ProcessBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully drains pending events and updates database", func(t *testing.T) {
		events := []*domain.OutboxEvent{
			{
				ID:             "evt-1",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				EventType:      domain.EventTypeMessageReceived,
				Payload:        []byte(`{"msg": 1}`),
				PartitionKey:   "tenant-1:conv-1",
				Status:         domain.OutboxStatusPending,
				CreatedAt:      time.Now(),
			},
			{
				ID:             "evt-2",
				TenantID:       "tenant-2",
				ConversationID: "conv-2",
				EventType:      domain.EventTypeMessageReceived,
				Payload:        []byte(`{"msg": 2}`),
				PartitionKey:   "tenant-2:conv-2",
				Status:         domain.OutboxStatusPending,
				CreatedAt:      time.Now(),
			},
		}

		repo := &mockOutboxRepo{events: events}
		producer := &mockKafkaProducer{}
		pubSvc := service.NewOutboxPublisherService(repo, producer, service.WithDefaultTopic("conversation.events"))

		count, err := pubSvc.ProcessBatch(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if count != 2 {
			t.Fatalf("expected 2 processed events, got %d", count)
		}

		if len(producer.publishedMessages) != 2 {
			t.Fatalf("expected 2 published messages in kafka, got %d", len(producer.publishedMessages))
		}

		if producer.publishedMessages[0].Key != "tenant-1:conv-1" {
			t.Errorf("expected partition key tenant-1:conv-1, got %s", producer.publishedMessages[0].Key)
		}
		if producer.publishedMessages[0].Topic != "conversation.events" {
			t.Errorf("expected topic conversation.events, got %s", producer.publishedMessages[0].Topic)
		}

		if len(repo.publishedIDs) != 2 {
			t.Fatalf("expected 2 events marked as published, got %d", len(repo.publishedIDs))
		}
	})

	t.Run("stops and keeps event pending if kafka fails", func(t *testing.T) {
		events := []*domain.OutboxEvent{
			{
				ID:             "evt-fail",
				TenantID:       "tenant-1",
				ConversationID: "conv-1",
				EventType:      domain.EventTypeMessageReceived,
				Payload:        []byte(`{"msg": "fail"}`),
				PartitionKey:   "tenant-1:conv-1",
				Status:         domain.OutboxStatusPending,
				CreatedAt:      time.Now(),
			},
		}

		repo := &mockOutboxRepo{events: events}
		producer := &mockKafkaProducer{publishErr: errors.New("kafka unavailable")}
		pubSvc := service.NewOutboxPublisherService(repo, producer)

		count, err := pubSvc.ProcessBatch(ctx)
		if err == nil {
			t.Fatalf("expected error when kafka fails, got nil")
		}

		if count != 0 {
			t.Errorf("expected 0 processed events, got %d", count)
		}

		if len(repo.publishedIDs) != 0 {
			t.Errorf("expected 0 events marked as published, got %d", len(repo.publishedIDs))
		}

		// Evento continua como PENDING
		if events[0].Status != domain.OutboxStatusPending {
			t.Errorf("expected event to remain PENDING, got %s", events[0].Status)
		}
	})

	t.Run("empty outbox returns 0 without error", func(t *testing.T) {
		repo := &mockOutboxRepo{events: nil}
		producer := &mockKafkaProducer{}
		pubSvc := service.NewOutboxPublisherService(repo, producer)

		count, err := pubSvc.ProcessBatch(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 count, got %d", count)
		}
	})
}

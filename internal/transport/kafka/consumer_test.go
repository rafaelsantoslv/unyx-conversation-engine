package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

type recordingPipelineHandler struct {
	mu       sync.Mutex
	received []domain.Event[domain.MessageReceivedPayload]
}

func (h *recordingPipelineHandler) HandleMessageReceived(ctx context.Context, evt domain.Event[domain.MessageReceivedPayload]) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.received = append(h.received, evt)
	return nil
}

func (h *recordingPipelineHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

func (h *recordingPipelineHandler) getReceived() []domain.Event[domain.MessageReceivedPayload] {
	h.mu.Lock()
	defer h.mu.Unlock()
	copied := make([]domain.Event[domain.MessageReceivedPayload], len(h.received))
	copy(copied, h.received)
	return copied
}

func TestConversationWorker_Integration(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Cria tópico dedicado para o teste
	topic := fmt.Sprintf("test.worker.events.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("skipping integration test: kafka dial failed: %v", err)
		return
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}
	if err := conn.CreateTopics(topicConfig); err != nil {
		t.Fatalf("failed to create test topic: %v", err)
	}

	// 2. Prepara produtor para publicar 3 mensagens com a MESMA chave de partição
	publisher := transkafka.NewKafkaPublisher(brokers)
	defer publisher.Close()

	partitionKey := fmt.Sprintf("tenant-test:%d", time.Now().UnixNano())
	numMessages := 3
	var sentEventIDs []string

	for i := 1; i <= numMessages; i++ {
		evtID := fmt.Sprintf("evt-seq-%d-%d", i, time.Now().UnixNano())
		sentEventIDs = append(sentEventIDs, evtID)

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        evtID,
			TenantID:       "tenant-test",
			ConversationID: "conv-test",
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:         fmt.Sprintf("msg-%d", i),
				ExternalMessageID: fmt.Sprintf("ext-msg-%d", i),
				Channel:           "whatsapp",
				Role:              domain.RoleUser,
				Content:           fmt.Sprintf("Mensagem ordenada número %d", i),
				SequenceNumber:    int64(i),
			},
		}

		payloadBytes, err := json.Marshal(evt)
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}

		if err := publisher.Publish(ctx, topic, partitionKey, payloadBytes); err != nil {
			t.Fatalf("failed to publish test message %d: %v", i, err)
		}
	}

	// 3. Inicializa o Worker com Consumer Group
	groupID := fmt.Sprintf("test-worker-group-%d", time.Now().UnixNano())
	consumer := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		MaxWait: 200 * time.Millisecond,
	})
	defer consumer.Close()

	handler := &recordingPipelineHandler{}
	workerService := service.NewConversationWorkerService(consumer, handler)

	// 4. Executa o worker em uma goroutine com context cancelável
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerService.Run(workerCtx)
	}()

	// 5. Aguarda o consumo das 3 mensagens
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for worker to consume messages: consumed=%d, expected=%d",
				handler.count(), numMessages)
		case <-ticker.C:
			if handler.count() >= numMessages {
				goto CheckResults
			}
		}
	}

CheckResults:
	// 6. Encerra o worker de forma graciosa disparando o cancelamento do context
	workerCancel()
	select {
	case err := <-workerDone:
		if err != nil && err != context.Canceled {
			t.Errorf("worker exited with unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("worker took too long to shutdown gracefully")
	}

	// 7. Valida se todas as 3 mensagens chegaram e foram processadas na ordem exata enviada
	received := handler.getReceived()
	if len(received) != numMessages {
		t.Fatalf("expected %d messages, got %d", numMessages, len(received))
	}

	for i := 0; i < numMessages; i++ {
		if received[i].EventID != sentEventIDs[i] {
			t.Errorf("order mismatch at index %d: expected event %s, got %s",
				i, sentEventIDs[i], received[i].EventID)
		}
		expectedSeq := int64(i + 1)
		if received[i].Payload.SequenceNumber != expectedSeq {
			t.Errorf("sequence number mismatch at index %d: expected %d, got %d",
				i, expectedSeq, received[i].Payload.SequenceNumber)
		}
	}
}

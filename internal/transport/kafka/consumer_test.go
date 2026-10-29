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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
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


func TestConversationWorker_WithPostgresResults_Integration(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Conecta ao banco de dados PostgreSQL
	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping integration test: postgres connection failed: %v", err)
		return
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		t.Skipf("skipping integration test: postgres ping failed: %v", err)
		return
	}

	repo := postgres.NewRepository(dbPool)
	resultsRepo := postgres.NewResultsRepository(dbPool)

	// Cria conversa no banco
	tenantID := fmt.Sprintf("tenant-e2e-%d", time.Now().UnixNano())
	extConvID := fmt.Sprintf("conv-e2e-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, extConvID)
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	// 2. Cria tópico de teste no Kafka
	topic := fmt.Sprintf("test.results.worker.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("skipping test: kafka dial failed: %v", err)
		return
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	publisher := transkafka.NewKafkaPublisher(brokers)
	defer publisher.Close()

	// 3. Publica mensagem no Kafka
	msgID := uuid.New().String()
	evtID := fmt.Sprintf("evt-e2e-%d", time.Now().UnixNano())
	evt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        evtID,
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         msgID,
			ExternalMessageID: fmt.Sprintf("ext-e2e-%d", time.Now().UnixNano()),
			Channel:           "whatsapp",
			Role:              domain.RoleUser,
			Content:           "Pergunta de teste para persistir resultado",
			SequenceNumber:    1,
		},
	}
	payloadBytes, _ := json.Marshal(evt)
	err = publisher.Publish(ctx, topic, tenantID+":"+conv.ID, payloadBytes)
	if err != nil {
		t.Fatalf("failed to publish to kafka: %v", err)
	}

	// 4. Inicializa o Worker com DefaultMessagePipelineHandler conectado ao Postgres
	mockRuntime := mock.NewMockRuntime(
		mock.WithLatency(10*time.Millisecond),
		mock.WithSimulatedReply("Resposta persistida no Postgres com sucesso!"),
	)
	pipeline := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

	consumer := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("group-results-%d", time.Now().UnixNano()),
		MaxWait: 200 * time.Millisecond,
	})
	defer consumer.Close()

	worker := service.NewConversationWorkerService(consumer, pipeline)

	// 5. Executa uma iteração de consumo
	err = worker.ProcessNextMessage(ctx)
	if err != nil {
		t.Fatalf("worker failed to process message: %v", err)
	}

	// 6. Verifica no PostgreSQL se o registro foi salvo com status SUCCEEDED
	savedRes, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
	if err != nil {
		t.Fatalf("failed to query processing result: %v", err)
	}
	if savedRes == nil {
		t.Fatalf("processing result was NOT saved in postgres!")
	}
	if savedRes.Status != domain.ExecutionStatusSucceeded {
		t.Errorf("expected status SUCCEEDED, got %s", savedRes.Status)
	}
	if savedRes.CompletedAt == nil {
		t.Errorf("expected completed_at to be populated")
	}

	// 7. Teste de Idempotência: processa o mesmo evento novamente
	// O pipeline deve detectar que já está SUCCEEDED e não deve reexecutar o runtime
	err = pipeline.HandleMessageReceived(ctx, evt)
	if err != nil {
		t.Fatalf("repeated execution failed: %v", err)
	}
}


func TestConversationWorker_StrictOrdering_Integration(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping integration test: postgres connection failed: %v", err)
		return
	}
	defer dbPool.Close()

	repo := postgres.NewRepository(dbPool)
	resultsRepo := postgres.NewResultsRepository(dbPool)

	tenantID := fmt.Sprintf("tenant-ord-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-ord-1")
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	topic := fmt.Sprintf("test.ordering.events.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("skipping test: kafka dial failed: %v", err)
		return
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	publisher := transkafka.NewKafkaPublisher(brokers)
	defer publisher.Close()

	// 1. Envia 5 mensagens sequenciais para a mesma conversa
	numMsgs := 5
	var msgIDs []string
	for seq := 1; seq <= numMsgs; seq++ {
		msgID := uuid.New().String()
		msgIDs = append(msgIDs, msgID)

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        fmt.Sprintf("evt-ord-%d", seq),
			TenantID:       tenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:         msgID,
				ExternalMessageID: fmt.Sprintf("ext-ord-%d", seq),
				Channel:           "whatsapp",
				Role:              domain.RoleUser,
				Content:           fmt.Sprintf("Mensagem ordenada seq=%d", seq),
				SequenceNumber:    int64(seq),
			},
		}

		payloadBytes, _ := json.Marshal(evt)
		err = publisher.Publish(ctx, topic, tenantID+":"+conv.ID, payloadBytes)
		if err != nil {
			t.Fatalf("failed to publish message seq %d: %v", seq, err)
		}
	}

	mockRuntime := mock.NewMockRuntime(
		mock.WithLatency(15*time.Millisecond),
		mock.WithSimulatedReply("Resposta ordenada"),
	)
	pipeline := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

	consumer := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("group-ord-%d", time.Now().UnixNano()),
		MaxWait: 100 * time.Millisecond,
	})
	defer consumer.Close()

	worker := service.NewConversationWorkerService(consumer, pipeline)

	// 2. Processa as 5 mensagens
	for i := 0; i < numMsgs; i++ {
		err := worker.ProcessNextMessage(ctx)
		if err != nil {
			t.Fatalf("failed processing message index %d: %v", i, err)
		}
	}

	// 3. Valida no PostgreSQL que todas as 5 mensagens foram concluídas estritamente com seus sequence_numbers corretos
	for seq := 1; seq <= numMsgs; seq++ {
		msgID := msgIDs[seq-1]
		res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
		if err != nil {
			t.Fatalf("failed to fetch result for seq %d: %v", seq, err)
		}
		if res == nil {
			t.Fatalf("result for seq %d was not found in postgres", seq)
		}
		if res.Status != domain.ExecutionStatusSucceeded {
			t.Errorf("expected SUCCEEDED for seq %d, got %s", seq, res.Status)
		}
		if res.SequenceNumber != int64(seq) {
			t.Errorf("expected sequence_number %d, got %d", seq, res.SequenceNumber)
		}
	}
}


func TestConversationWorker_DistributedCoordination_Integration(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping integration test: postgres connection failed: %v", err)
		return
	}
	defer dbPool.Close()

	repo := postgres.NewRepository(dbPool)
	resultsRepo := postgres.NewResultsRepository(dbPool)
	coordinator := postgres.NewExecutionCoordinator(dbPool)

	tenantID := fmt.Sprintf("tenant-dist-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, "conv-dist-1")
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	mockRuntime := mock.NewMockRuntime(
		mock.WithLatency(40*time.Millisecond),
		mock.WithSimulatedReply("Resposta distribuída com sucesso"),
	)

	// Cria dois pipelines representando dois workers distintos
	pipeline1 := service.NewDefaultMessagePipelineHandler(
		mockRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, "worker-node-1"),
	)

	pipeline2 := service.NewDefaultMessagePipelineHandler(
		mockRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, "worker-node-2"),
	)

	msgID := uuid.New().String()
	evt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        "evt-dist-1",
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         msgID,
			ExternalMessageID: "ext-dist-1",
			Channel:           "whatsapp",
			Role:              domain.RoleUser,
			Content:           "Pergunta disputada entre 2 workers",
			SequenceNumber:    1,
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var err1, err2 error

	go func() {
		defer wg.Done()
		err1 = pipeline1.HandleMessageReceived(ctx, evt)
	}()

	go func() {
		defer wg.Done()
		err2 = pipeline2.HandleMessageReceived(ctx, evt)
	}()

	wg.Wait()

	if err1 != nil {
		t.Errorf("pipeline 1 failed unexpectedly: %v", err1)
	}
	if err2 != nil {
		t.Errorf("pipeline 2 failed unexpectedly: %v", err2)
	}

	// Verifica se o resultado em processing_results foi gravado como SUCCEEDED
	res, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, msgID)
	if err != nil {
		t.Fatalf("failed to get result: %v", err)
	}
	if res == nil || res.Status != domain.ExecutionStatusSucceeded {
		t.Errorf("expected SUCCEEDED status in DB, got %+v", res)
	}
}

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

func TestOutboxPublisher_Integration(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Conecta ao banco de dados e roda migrações se necessário
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

	// 2. Garante que o tópico dedicado exista antes do teste
	uniqueTopic := fmt.Sprintf("test.outbox.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("failed to dial kafka broker: %v", err)
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             uniqueTopic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}
	if err := conn.CreateTopics(topicConfig); err != nil {
		t.Fatalf("failed to create test topic: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    uniqueTopic,
		GroupID:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer reader.Close()

	// 3. Prepara repository e publisher service
	repo := postgres.NewRepository(dbPool)
	kafkaPub := transkafka.NewKafkaPublisher(brokers)
	defer kafkaPub.Close()

	// Limpa quaisquer eventos pendentes residuais de outros testes para isolamento estrito
	_, _ = dbPool.Exec(ctx, "DELETE FROM outbox_events WHERE status = 'PENDING'")

	pubService := service.NewOutboxPublisherService(
		repo,
		kafkaPub,
		service.WithDefaultTopic(uniqueTopic),
		service.WithBatchSize(10),
	)

	// 4. Cria conversa e insere evento de teste na outbox
	tenantID := fmt.Sprintf("tenant-%d", time.Now().UnixNano())
	extConvID := fmt.Sprintf("ext-conv-%d", time.Now().UnixNano())
	conv, err := repo.GetOrCreateConversation(ctx, tenantID, extConvID)
	if err != nil {
		t.Fatalf("failed to create conversation: %v", err)
	}

	expectedKey := fmt.Sprintf("%s:%s", tenantID, conv.ID)
	evtPayload := map[string]interface{}{
		"event_id": fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		"content":  "Mensagem de teste de integracao do Outbox Publisher",
	}
	payloadBytes, _ := json.Marshal(evtPayload)

	msg := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: fmt.Sprintf("ext-msg-%d", time.Now().UnixNano()),
		Role:              domain.RoleUser,
		Content:           "Mensagem de teste de integracao do Outbox Publisher",
	}

	outboxEvt := &domain.OutboxEvent{
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Payload:        payloadBytes,
		PartitionKey:   expectedKey,
		Status:         domain.OutboxStatusPending,
	}

	_, err = repo.SaveMessageWithOutbox(ctx, msg, outboxEvt)
	if err != nil {
		t.Fatalf("failed to save message with outbox: %v", err)
	}

	// 5. Executa uma rodada do ProcessBatch
	processed, err := pubService.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if processed == 0 {
		t.Fatalf("expected at least 1 event to be processed, got 0")
	}

	// 6. Lê do Kafka para verificar se o evento foi gravado com a chave e payload corretos
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()

	receivedMsg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("failed to read message from kafka: %v", err)
	}

	if string(receivedMsg.Key) != expectedKey {
		t.Errorf("expected partition key %s, got %s", expectedKey, string(receivedMsg.Key))
	}

	var expectedMap, receivedMap map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &expectedMap); err != nil {
		t.Fatalf("failed to unmarshal payloadBytes: %v", err)
	}
	if err := json.Unmarshal(receivedMsg.Value, &receivedMap); err != nil {
		t.Fatalf("failed to unmarshal receivedMsg.Value: %v", err)
	}

	if expectedMap["event_id"] != receivedMap["event_id"] || expectedMap["content"] != receivedMap["content"] {
		t.Errorf("payload mismatch: expected %v, got %v", expectedMap, receivedMap)
	}

	// 7. Verifica se o status do evento na outbox foi atualizado para PUBLISHED
	var status string
	var publishedAt *time.Time
	err = dbPool.QueryRow(ctx, "SELECT status, published_at FROM outbox_events WHERE id = $1", outboxEvt.ID).Scan(&status, &publishedAt)
	if err != nil {
		t.Fatalf("failed to check outbox event status in db: %v", err)
	}

	if status != string(domain.OutboxStatusPublished) {
		t.Errorf("expected outbox status to be PUBLISHED, got %s", status)
	}
	if publishedAt == nil {
		t.Errorf("expected published_at timestamp to be populated, got nil")
	}
}

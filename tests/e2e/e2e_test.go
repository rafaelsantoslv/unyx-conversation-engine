package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transhttp "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/segmentio/kafka-go"
)

// Helper para asserção assíncrona com polling (Eventually)
func eventually(t *testing.T, timeout, interval time.Duration, condition func() bool, msgAndArgs ...interface{}) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("condition not met within %v: %v", timeout, fmt.Sprint(msgAndArgs...))
}

func setupTestEnvironment(t *testing.T) (*pgxpool.Pool, []string, string) {
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
		t.Skipf("skipping e2e test: postgres connection failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping e2e test: postgres ping failed: %v", err)
	}

	// Executa migrações
	if err := postgres.RunMigrations(ctx, pool, "../../migrations"); err != nil {
		// Tenta caminho relativo migrations
		_ = postgres.RunMigrations(ctx, pool, "migrations")
	}

	topic := fmt.Sprintf("e2e.events.%d", time.Now().UnixNano())
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Skipf("skipping e2e test: kafka dial failed: %v", err)
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})

	return pool, brokers, topic
}

func TestE2E_FullPipeline(t *testing.T) {
	pool, brokers, topic := setupTestEnvironment(t)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup da Camada de Armazenamento
	repo := postgres.NewRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)

	// 2. Setup do Servidor HTTP (API)
	ingestionSvc := service.NewIngestionService(repo)
	msgHandler := handler.NewMessageHandler(ingestionSvc)
	mux := http.NewServeMux()
	mux.Handle("/v1/messages", transhttp.RequestIDMiddleware(http.HandlerFunc(msgHandler.HandleIngest)))
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	// 3. Setup do Outbox Publisher em Background
	kafkaPublisher := transkafka.NewKafkaPublisher(brokers)
	defer kafkaPublisher.Close()

	pubService := service.NewOutboxPublisherService(
		repo,
		kafkaPublisher,
		service.WithDefaultTopic(topic),
		service.WithBatchSize(20),
		service.WithPollInterval(50*time.Millisecond),
	)

	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		_ = pubService.Run(ctx)
	}()

	// 4. Setup do Conversation Worker em Background
	mockRuntime := mock.NewMockRuntime(
		mock.WithLatency(30*time.Millisecond),
		mock.WithSimulatedReply("Olá! Esta é uma resposta de teste E2E gerada pelo MockRuntime."),
	)
	pipelineHandler := service.NewDefaultMessagePipelineHandler(mockRuntime, resultsRepo)

	consumer := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("e2e-workers-%d", time.Now().UnixNano()),
		MaxWait: 100 * time.Millisecond,
	})
	defer consumer.Close()

	workerService := service.NewConversationWorkerService(consumer, pipelineHandler)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = workerService.Run(ctx)
	}()

	// =========================================================================
	// CENÁRIO 1: Caminho Feliz (Happy Path)
	// =========================================================================
	t.Run("HappyPath: HTTP 202 -> DB -> Outbox -> Kafka -> Worker -> Mock -> DB Succeeded", func(t *testing.T) {
		tenantID := fmt.Sprintf("tenant-e2e-%d", time.Now().UnixNano())
		convExternalID := fmt.Sprintf("conv-ext-%d", time.Now().UnixNano())
		msgExternalID := fmt.Sprintf("msg-ext-%d", time.Now().UnixNano())

		reqBody := dto.IngestMessageRequest{
			TenantID:          tenantID,
			ConversationID:    convExternalID,
			ExternalMessageID: msgExternalID,
			Channel:           "whatsapp",
			Role:              "user",
			Content:           "Olá, preciso de ajuda com um pedido!",
		}
		bodyBytes, _ := json.Marshal(reqBody)

		// 1. Requisição POST /v1/messages
		resp, err := http.Post(testServer.URL+"/v1/messages", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("failed to post message: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			respBytes, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected status 202 Accepted, got %d. Body: %s", resp.StatusCode, string(respBytes))
		}

		var ingestResp dto.IngestMessageResponse
		if err := json.NewDecoder(resp.Body).Decode(&ingestResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if ingestResp.Status != "accepted" || ingestResp.MessageID == "" {
			t.Fatalf("invalid ingest response: %+v", ingestResp)
		}

		messageID := ingestResp.MessageID

		// 2. Asserção assíncrona: Evento Outbox processado pelo Publisher e marcado como PUBLISHED
		eventually(t, 5*time.Second, 100*time.Millisecond, func() bool {
			var status string
			err := pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE tenant_id = $1 AND status = 'PUBLISHED'", tenantID).Scan(&status)
			return err == nil && status == string(domain.OutboxStatusPublished)
		}, "outbox event was not marked as PUBLISHED by publisher")

		// 3. Asserção assíncrona: Worker consumiu do Kafka e gravou SUCCEEDED em processing_results
		eventually(t, 6*time.Second, 100*time.Millisecond, func() bool {
			var status string
			var execTime int64
			err := pool.QueryRow(ctx, "SELECT status, execution_time_ms FROM processing_results WHERE tenant_id = $1 AND message_id = $2",
				tenantID, messageID).Scan(&status, &execTime)
			return err == nil && status == string(domain.ExecutionStatusSucceeded) && execTime >= 20
		}, "processing result was not marked as SUCCEEDED by worker")
	})

	// =========================================================================
	// CENÁRIO 2: Payload Inválido (HTTP 422/400 sem sujeira no banco)
	// =========================================================================
	t.Run("InvalidPayload: Returns 422 and does not pollute database", func(t *testing.T) {
		invalidTenantID := fmt.Sprintf("tenant-invalid-%d", time.Now().UnixNano())
		invalidReq := dto.IngestMessageRequest{
			TenantID:          invalidTenantID,
			ConversationID:    "", // Faltando campo obrigatório
			ExternalMessageID: "msg-123",
			Channel:           "whatsapp",
			Role:              "user",
			Content:           "Conteúdo",
		}
		bodyBytes, _ := json.Marshal(invalidReq)

		resp, err := http.Post(testServer.URL+"/v1/messages", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("failed to post: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Errorf("expected 422 Unprocessable Entity, got %d", resp.StatusCode)
		}

		// Garante que não criou mensagens nem outbox para esse tenant
		var count int
		_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE tenant_id = $1", invalidTenantID).Scan(&count)
		if count != 0 {
			t.Errorf("expected 0 messages in DB, got %d", count)
		}
	})

	// =========================================================================
	// CENÁRIO 3: Idempotência de Mensagem Duplicada (Deduplicação)
	// =========================================================================
	t.Run("DuplicateMessage: Sending same external_message_id twice processes only once", func(t *testing.T) {
		dupTenantID := fmt.Sprintf("tenant-dup-%d", time.Now().UnixNano())
		dupConvID := fmt.Sprintf("conv-dup-%d", time.Now().UnixNano())
		dupMsgExtID := "msg-unique-external-id-123"

		reqBody := dto.IngestMessageRequest{
			TenantID:          dupTenantID,
			ConversationID:    dupConvID,
			ExternalMessageID: dupMsgExtID,
			Channel:           "whatsapp",
			Role:              "user",
			Content:           "Mensagem duplicada",
		}
		bodyBytes, _ := json.Marshal(reqBody)

		// Primeiro envio
		resp1, err := http.Post(testServer.URL+"/v1/messages", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("post 1 failed: %v", err)
		}
		resp1.Body.Close()
		if resp1.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 on post 1, got %d", resp1.StatusCode)
		}

		// Segundo envio (mesmo external_message_id para o mesmo tenant)
		resp2, err := http.Post(testServer.URL+"/v1/messages", "application/json", bytes.NewBuffer(bodyBytes))
		if err != nil {
			t.Fatalf("post 2 failed: %v", err)
		}
		resp2.Body.Close()
		if resp2.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 on post 2, got %d", resp2.StatusCode)
		}

		// Aguarda o processamento
		eventually(t, 5*time.Second, 100*time.Millisecond, func() bool {
			var msgCount int
			_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE tenant_id = $1 AND external_id = $2",
				dupTenantID, dupMsgExtID).Scan(&msgCount)
			return msgCount == 1
		}, "expected exactly 1 message stored for duplicate external_message_id")

		// Confirma que só existe 1 resultado de processamento
		var resultsCount int
		_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM processing_results WHERE tenant_id = $1", dupTenantID).Scan(&resultsCount)
		if resultsCount > 1 {
			t.Errorf("expected at most 1 processing result, got %d", resultsCount)
		}
	})

	// =========================================================================
	// CENÁRIO 4: Simulação de Falha Controlada no Runtime (Status FAILED gravado)
	// =========================================================================
	t.Run("RuntimeFailure: Controlled failure records FAILED in processing_results", func(t *testing.T) {
		failTenantID := fmt.Sprintf("tenant-fail-%d", time.Now().UnixNano())
		conv, err := repo.GetOrCreateConversation(ctx, failTenantID, "conv-fail-1")
		if err != nil {
			t.Fatalf("failed to create conv: %v", err)
		}

		msgID := uuid.New().String()
		msg := &domain.Message{
			ID:                msgID,
			TenantID:          failTenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: "msg-ext-fail",
			Role:              domain.RoleUser,
			Content:           "Gera erro na IA",
		}
		_, err = repo.SaveMessage(ctx, msg)
		if err != nil {
			t.Fatalf("failed to save msg: %v", err)
		}

		// Cria pipeline com MockRuntime que falha propositalmente com erro permanente (400 Bad Request)
		failErr := errors.New("simulated AI provider invalid payload (400 Bad Request)")
		failingRuntime := mock.NewMockRuntime(mock.WithFailure(failErr))
		failingPipeline := service.NewDefaultMessagePipelineHandler(failingRuntime, resultsRepo)

		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        "evt-fail-e2e",
			TenantID:       failTenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:         msgID,
				ExternalMessageID: msg.ExternalMessageID,
				Channel:           "whatsapp",
				Role:              domain.RoleUser,
				Content:           msg.Content,
			},
		}

		// Executa o pipeline que deve registrar FAILED no banco e retornar o erro
		execErr := failingPipeline.HandleMessageReceived(ctx, evt)
		if execErr == nil {
			t.Fatalf("expected error from failing pipeline, got nil")
		}

		// Verifica no Postgres se o status FAILED e a mensagem de erro foram registrados
		savedRes, err := resultsRepo.GetResult(ctx, failTenantID, conv.ID, msgID)
		if err != nil {
			t.Fatalf("failed to query result: %v", err)
		}
		if savedRes == nil {
			t.Fatalf("expected processing result record in DB, got nil")
		}
		if savedRes.Status != domain.ExecutionStatusFailed {
			t.Errorf("expected status FAILED, got %s", savedRes.Status)
		}
		if (savedRes.ErrorMessage == nil || !strings.Contains(*savedRes.ErrorMessage, "400")) &&
			(savedRes.LastError == nil || !strings.Contains(*savedRes.LastError, "400")) {
			t.Errorf("expected error message with '400', got error_message=%v, last_error=%v", savedRes.ErrorMessage, savedRes.LastError)
		}
	})

	// =========================================================================
	// CENÁRIO 5: Resiliência a Reinicialização do Worker (Sem Perda de Mensagens)
	// =========================================================================
	t.Run("WorkerRestart: Worker picks up unprocessed messages after restart", func(t *testing.T) {
		restartTenantID := fmt.Sprintf("tenant-restart-%d", time.Now().UnixNano())
		conv, err := repo.GetOrCreateConversation(ctx, restartTenantID, "conv-restart-1")
		if err != nil {
			t.Fatalf("failed to create conv: %v", err)
		}

		msgID := uuid.New().String()
		evt := domain.Event[domain.MessageReceivedPayload]{
			EventID:        fmt.Sprintf("evt-restart-%d", time.Now().UnixNano()),
			TenantID:       restartTenantID,
			ConversationID: conv.ID,
			EventType:      domain.EventTypeMessageReceived,
			Version:        domain.EventVersionV1,
			OccurredAt:     time.Now().UTC(),
			Payload: domain.MessageReceivedPayload{
				MessageID:         msgID,
				ExternalMessageID: "msg-ext-restart",
				Channel:           "whatsapp",
				Role:              domain.RoleUser,
				Content:           "Mensagem enviada com worker reiniciado",
			},
		}
		payloadBytes, _ := json.Marshal(evt)

		// Publica diretamente no tópico Kafka
		restartTopic := fmt.Sprintf("topic-restart-%d", time.Now().UnixNano())
		conn, _ := kafka.Dial("tcp", brokers[0])
		_ = conn.CreateTopics(kafka.TopicConfig{Topic: restartTopic, NumPartitions: 1, ReplicationFactor: 1})
		conn.Close()

		err = kafkaPublisher.Publish(ctx, restartTopic, restartTenantID+":"+conv.ID, payloadBytes)
		if err != nil {
			t.Fatalf("failed to publish to kafka: %v", err)
		}

		// Cria Worker 1 e o interrompe antes do processamento
		groupID := fmt.Sprintf("group-restart-%d", time.Now().UnixNano())
		workerConsumer1 := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
			Brokers: brokers,
			Topic:   restartTopic,
			GroupID: groupID,
			MaxWait: 50 * time.Millisecond,
		})
		workerConsumer1.Close() // Fecha / Simula queda antes de processar

		// Cria Worker 2 (reinicializado) com o mesmo consumer group
		workerConsumer2 := transkafka.NewKafkaConsumer(transkafka.ConsumerConfig{
			Brokers: brokers,
			Topic:   restartTopic,
			GroupID: groupID,
			MaxWait: 50 * time.Millisecond,
		})
		defer workerConsumer2.Close()

		worker2 := service.NewConversationWorkerService(workerConsumer2, pipelineHandler)

		// Processa a mensagem com o worker reinicializado
		processErr := worker2.ProcessNextMessage(ctx)
		if processErr != nil {
			t.Fatalf("worker 2 failed to process after restart: %v", processErr)
		}

		// Confirma que a mensagem foi concluída com sucesso
		eventually(t, 5*time.Second, 100*time.Millisecond, func() bool {
			res, err := resultsRepo.GetResult(ctx, restartTenantID, conv.ID, msgID)
			return err == nil && res != nil && res.Status == domain.ExecutionStatusSucceeded
		}, "worker after restart failed to resume and persist result")
	})
}

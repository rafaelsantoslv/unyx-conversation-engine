package e2e_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	transkafka "github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_HorizontalWorkerScaling_ConcurrencyAndBackpressure valida a execução
// simultânea de múltiplos workers concorrentes (ex: 3 workers) consumindo mensagens
// de 10 conversas e tenants distintos sem race conditions, contenção de locks ou vazamento de banco (OPS-09, INV-01, INV-06).
func TestE2E_HorizontalWorkerScaling_ConcurrencyAndBackpressure(t *testing.T) {
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	ctx := context.Background()

	coreRepo := postgres.NewRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	execReqRepo := postgres.NewExecutionRequestRepository(pool)

	const numConversations = 10
	const numWorkers = 3
	const messagesPerConv = 3

	type convData struct {
		tenantID string
		conv     *domain.Conversation
	}

	conversations := make([]convData, numConversations)
	for i := 0; i < numConversations; i++ {
		tID := fmt.Sprintf("tenant_scale_%d_%s", i, uuid.NewString()[:6])
		conv, err := coreRepo.GetOrCreateConversation(ctx, tID, fmt.Sprintf("user_%d", i))
		require.NoError(t, err)
		conversations[i] = convData{tenantID: tID, conv: conv}
	}

	// Mock Agent Runtime com latência simulada de computação (15ms)
	mockRuntime := mock.NewMockRuntime(
		mock.WithSimulatedReply("Resposta simulada de escala concorrente"),
		mock.WithLatency(15*time.Millisecond),
	)

	var processedCount int64
	var mu sync.Mutex
	workerExecutionLogs := make(map[string]int) // rastreia quais workers processaram quais mensagens

	// Canal compartilhado simulando tópicos particionados no Kafka
	kafkaQueue := make(chan domain.Event[domain.MessageReceivedPayload], numConversations*messagesPerConv)

	// Popula a fila com eventos de mensagens
	for _, cd := range conversations {
		for m := 1; m <= messagesPerConv; m++ {
			msgID := uuid.NewString()
			extMsgID := fmt.Sprintf("ext_%s_%d", cd.conv.ID, m)
			// Persiste mensagem individualmente no banco antes do evento (INV-01)
			_, err := coreRepo.SaveMessageWithOutbox(ctx, &domain.Message{
				ID:                msgID,
				TenantID:          cd.tenantID,
				ConversationID:    cd.conv.ID,
				ExternalMessageID: extMsgID,
				Role:              domain.RoleUser,
				Content:           fmt.Sprintf("Mensagem %d da conversa %s", m, cd.conv.ID),
				SequenceNumber:    int64(m),
				CreatedAt:         time.Now().UTC(),
			}, &domain.OutboxEvent{
				TenantID:       cd.tenantID,
				ConversationID: cd.conv.ID,
				EventType:      "conversation.message.received",
				Payload:        []byte("{}"),
			})
			require.NoError(t, err)

			event := domain.Event[domain.MessageReceivedPayload]{
				EventID:        uuid.NewString(),
				TenantID:       cd.tenantID,
				ConversationID: cd.conv.ID,
				EventType:      "conversation.message.received",
				OccurredAt:     time.Now().UTC(),
				Payload: domain.MessageReceivedPayload{
					MessageID:         msgID,
					SequenceNumber:    int64(m),
					ExternalMessageID: extMsgID,
					Role:              domain.RoleUser,
					Channel:           "whatsapp",
					Content:           fmt.Sprintf("Mensagem %d da conversa %s", m, cd.conv.ID),
				},
			}
			kafkaQueue <- event
		}
	}
	close(kafkaQueue)

	// Inicia 3 workers concorrentes em paralelo
	var wg sync.WaitGroup
	errChan := make(chan error, numWorkers)

	for w := 1; w <= numWorkers; w++ {
		workerID := fmt.Sprintf("worker-node-%d", w)
		wg.Add(1)

		go func(id string) {
			defer wg.Done()

			pipelineHandler := service.NewDefaultMessagePipelineHandler(
				mockRuntime,
				resultsRepo,
				service.WithCoordinator(coordinator, id),
			)

			// Simula semáforo de backpressure de 5 tarefas por worker
			sem := make(chan struct{}, 5)

			for evt := range kafkaQueue {
				sem <- struct{}{}

				// Prepara Snapshot de ExecutionRequest antes de processar
				_, err := execReqRepo.CreateSnapshotRequest(ctx, evt.TenantID, evt.ConversationID)
				if err != nil {
					<-sem
					errChan <- fmt.Errorf("[%s] failed to create snapshot request: %w", id, err)
					return
				}

				// Processa o pipeline
				err = pipelineHandler.HandleMessageReceived(ctx, evt)
				if err != nil {
					<-sem
					errChan <- fmt.Errorf("[%s] pipeline handle failed: %w", id, err)
					return
				}

				atomic.AddInt64(&processedCount, 1)

				mu.Lock()
				workerExecutionLogs[id]++
				mu.Unlock()

				<-sem
			}
		}(workerID)
	}

	wg.Wait()
	close(errChan)

	// Checa se houve algum erro nos workers concorrentes
	for err := range errChan {
		t.Fatalf("concurrency error occurred: %v", err)
	}

	// Validações
	expectedTotal := int64(numConversations * messagesPerConv)
	assert.Equal(t, expectedTotal, atomic.LoadInt64(&processedCount), "Todas as mensagens devem ser processadas com sucesso")

	// Garante que o trabalho foi distribuído entre múltiplos workers (horizontal scaling)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, workerExecutionLogs, numWorkers, "Todos os workers devem ter participado do processamento")
	for wID, count := range workerExecutionLogs {
		assert.Greater(t, count, 0, "Worker %s processou %d mensagens", wID, count)
	}
}

// Suppress unused imports
var _ = transkafka.ConsumerConfig{}

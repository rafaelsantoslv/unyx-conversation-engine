package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/runtime/mock"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/service"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplicabilityValidation_OperatorTakesOverDuringAIComputation valida o cenário de corrida crítico:
// 1. Mensagem recebida dispara computação do Agent Runtime (simulando 300ms de raciocínio).
// 2. Aos 100ms, um operador humano assume o atendimento (atualizando estado para HUMAN_ACTIVE).
// 3. A IA conclui a geração aos 300ms.
// 4. A etapa de Result Applicability Validation detecta a mudança de estado:
//    - O resultado é marcado como DISCARDED_STALE com motivo auditável (INV-07).
//    - NENHUM evento de outbound intent é publicado na Transactional Outbox (INV-06).
//    - O cliente humano NUNCA é atropelado pela mensagem obsoleta da IA.
func TestApplicabilityValidation_OperatorTakesOverDuringAIComputation(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	resultsRepo := postgres.NewResultsRepository(pool)

	tenantID := "tenant-applicability-e2e"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	msg := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-msg-app-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Preciso de ajuda urgente",
		CreatedAt:         time.Now().UTC(),
	}
	savedMsg, err := coreRepo.SaveMessage(ctx, msg)
	require.NoError(t, err)

	// Runtime com latência artificial de 300ms
	delayedRuntime := mock.NewMockRuntime(
		mock.WithLatency(300 * time.Millisecond),
		mock.WithSimulatedReply("Olá, sou a IA e vou te ajudar!"),
	)

	pipelineHandler := service.NewDefaultMessagePipelineHandler(
		delayedRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, "worker-app-e2e"),
	)

	evt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         savedMsg.ID,
			ExternalMessageID: savedMsg.ExternalMessageID,
			Channel:           "whatsapp",
			Role:              savedMsg.Role,
			Content:           savedMsg.Content,
			SequenceNumber:    savedMsg.SequenceNumber,
		},
	}

	execDone := make(chan error, 1)
	go func() {
		execDone <- pipelineHandler.HandleMessageReceived(ctx, evt)
	}()

	// 2. Operador humano assume o atendimento aos 80ms (antes da IA terminar aos 300ms)
	time.Sleep(80 * time.Millisecond)

	_, err = pool.Exec(ctx, "UPDATE conversations SET status = 'HUMAN_ACTIVE', version = version + 1 WHERE id = $1", conv.ID)
	require.NoError(t, err)

	// 3. Aguarda a conclusão da execução
	select {
	case err := <-execDone:
		require.NoError(t, err, "pipeline deve concluir sem crashar")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout aguardando pipeline")
	}

	// 4. Validação de Invariantes:
	// O resultado deve estar gravado no banco como DISCARDED_STALE
	result, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, savedMsg.ID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, domain.ExecutionStatusDiscardedStale, result.Status)
	assert.NotNil(t, result.ErrorMessage)
	assert.Contains(t, *result.ErrorMessage, "HUMAN_ACTIVE")

	// 5. Blindagem Total: NENHUM evento de envio foi inserido na Outbox
	outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	for _, ob := range outboxEvents {
		if ob.ConversationID == conv.ID && ob.EventType == domain.EventTypeOutboundIntent {
			t.Fatalf("CRITICAL BUG: outbound intent event leaked to outbox despite conversation being HUMAN_ACTIVE!")
		}
	}
}

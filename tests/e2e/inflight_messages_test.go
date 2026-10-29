package e2e_test

import (
	"context"
	"fmt"
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

// TestInFlightMessages_SubsequentExecutionCycle valida a orquestração completa de mensagens recebidas
// durante uma execução ativa (in-flight):
// 1. A IA começa a processar a mensagem 1 (simulando demora de 1.2s via mock runtime).
// 2. Concorrentemente (em 200ms e 400ms), o usuário envia as mensagens 2 e 3.
// 3. O pipeline impede execução concorrente simultânea (INV-02) e preserva todas as mensagens (INV-01).
// 4. Ao concluir a execução 1, o hook atômico detecta mensagens pendentes (seq > 1) e reativa o agendamento durável.
// 5. O Scheduler reivindica o agendamento e dispara o ciclo subsequente que processa o novo lote (INV-10).
func TestInFlightMessages_SubsequentExecutionCycle(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	schedRepo := postgres.NewScheduleRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)

	tenantID := "tenant-inflight-" + uuid.New().String()
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	// 1. Mensagem 1 inicial
	msg1 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-first-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Primeira mensagem - iniciando IA",
		CreatedAt:         time.Now().UTC(),
	}
	saved1, err := coreRepo.SaveMessage(ctx, msg1)
	require.NoError(t, err)

	// Mock Runtime com latência artificial de 600ms para simular IA pensando
	delayedRuntime := mock.NewMockRuntime(
		mock.WithLatency(600 * time.Millisecond),
		mock.WithSimulatedReply("Resposta gerada pela IA"),
	)

	pipelineHandler := service.NewDefaultMessagePipelineHandler(
		delayedRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, "worker-inflight-e2e"),
		service.WithScheduler(schedRepo),
	)

	// 2. Dispara o processamento da Mensagem 1 em background (durará 400ms)
	firstEvt := domain.Event[domain.MessageReceivedPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         saved1.ID,
			ExternalMessageID: saved1.ExternalMessageID,
			Channel:           "whatsapp",
			Role:              saved1.Role,
			Content:           saved1.Content,
			SequenceNumber:    saved1.SequenceNumber,
		},
	}

	execDone := make(chan error, 1)
	go func() {
		execDone <- pipelineHandler.HandleMessageReceived(ctx, firstEvt)
	}()

	// 3. Enquanto a IA está executando (in-flight), chegam as mensagens 2 e 3
	time.Sleep(100 * time.Millisecond)

	for i := 2; i <= 3; i++ {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-inflight-%d-%s", i, uuid.New().String()),
			Role:              domain.RoleUser,
			Content:           fmt.Sprintf("Mensagem in-flight %d", i),
			CreatedAt:         time.Now().UTC(),
		}
		_, err := coreRepo.SaveMessage(ctx, msg)
		require.NoError(t, err)
	}

	// 4. Aguarda a primeira execução finalizar
	select {
	case err := <-execDone:
		require.NoError(t, err, "primeira execução deve concluir com sucesso")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout aguardando primeira execução")
	}

	assert.True(t, saved1.SequenceNumber == 1)

	// 5. Verifica se o hook de liberação detectou as mensagens in-flight e reativou o agendamento no PostgreSQL
	sched, err := schedRepo.GetSchedule(ctx, conv.ID)
	require.NoError(t, err)
	require.NotNil(t, sched)
	assert.Equal(t, domain.ScheduleStatusPending, sched.Status, "agendamento deve estar PENDING para atender as mensagens in-flight")

	// 6. O Scheduler reivindica a execução do lote subsequente
	claimed, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 100, time.Now().UTC().Add(1*time.Second))
	require.NoError(t, err)

	var foundSubsequent *domain.ConversationSchedule
	for _, c := range claimed {
		if c.ConversationID == conv.ID {
			foundSubsequent = &c
			break
		}
	}
	require.NotNil(t, foundSubsequent, "deve existir agendamento reivindicado para o ciclo subsequente")
	assert.Equal(t, domain.ScheduleStatusClaimed, foundSubsequent.Status)

	// 7. Confirma que a outbox gerou o evento com as mensagens 2 e 3
	outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	var readyEvt *domain.OutboxEvent
	for _, ob := range outboxEvents {
		if ob.ConversationID == conv.ID && ob.EventType == domain.EventTypeExecutionReady {
			readyEvt = ob
		}
	}
	require.NotNil(t, readyEvt, "deve existir outbox event para a execução pronta das mensagens pendentes")
}

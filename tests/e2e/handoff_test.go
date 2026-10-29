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

// TestHandoffAndResumption_FreshStartCycle valida integralmente o critério de aceite de UDS-202:
// 1. Cliente envia mensagem 1 em AI_ACTIVE -> IA processa e responde normalmente.
// 2. Operador humano solicita handoff para HUMAN_ACTIVE (suspende imediatamente qualquer disparo de IA).
// 3. Cliente envia mensagem 2 e mensagem 3 durante o período humano:
//    - Mensagens são salvas normalmente no PostgreSQL (INV-01).
//    - Nenhum disparo de IA acontece (INV-06).
// 4. Operador resolve e devolve atendimento para a IA via ResumeAIAttendance (retomada):
//    - resumed_at_sequence é fixado em 3 (Fresh Start / Resumption Watermark).
//    - Mensagens 2 e 3 NÃO são respondidas retroativamente pela IA.
// 5. Cliente envia mensagem 4 (após a retomada):
//    - Apenas a mensagem 4 aciona a IA, preservando o histórico de 1 a 3 apenas como contexto passivo.
func TestHandoffAndResumption_FreshStartCycle(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	schedRepo := postgres.NewScheduleRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)
	handoffSvc := service.NewHandoffService(coreRepo)

	tenantID := "tenant-handoff-fresh"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-handoff-"+uuid.New().String())
	require.NoError(t, err)

	// Runtime que registra chamadas
	executedMessages := make([]string, 0)
	testRuntime := mock.NewMockRuntime(
		mock.WithSimulatedReply("Resposta da IA"),
	)

	pipelineHandler := service.NewDefaultMessagePipelineHandler(
		testRuntime,
		resultsRepo,
		service.WithCoordinator(coordinator, "worker-handoff-e2e"),
		service.WithScheduler(schedRepo),
	)

	// 1. Mensagem 1 em AI_ACTIVE
	msg1 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-1-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem 1 para IA",
		CreatedAt:         time.Now().UTC(),
	}
	saved1, err := coreRepo.SaveMessage(ctx, msg1)
	require.NoError(t, err)

	evt1 := domain.Event[domain.MessageReceivedPayload]{
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
	err = pipelineHandler.HandleMessageReceived(ctx, evt1)
	require.NoError(t, err)
	executedMessages = append(executedMessages, saved1.ID)

	// 2. Operador solicita Handoff para HUMAN_ACTIVE
	convHuman, err := handoffSvc.RequestHandoffToHuman(ctx, tenantID, conv.ID, conv.Version, "Cliente pediu atendente", "operator-joao")
	require.NoError(t, err)
	assert.Equal(t, domain.StateHumanActive, convHuman.State)
	assert.Equal(t, int64(2), convHuman.Version)

	// 3. Cliente envia mensagens 2 e 3 durante o período humano
	var savedMidMsgs []*domain.Message
	for i := 2; i <= 3; i++ {
		m := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-human-%d-%s", i, uuid.New().String()),
			Role:              domain.RoleUser,
			Content:           fmt.Sprintf("Mensagem %d falada com operador humano", i),
			CreatedAt:         time.Now().UTC(),
		}
		sm, err := coreRepo.SaveMessage(ctx, m)
		require.NoError(t, err)
		savedMidMsgs = append(savedMidMsgs, sm)
	}

	// 4. Operador devolve para IA via ResumeAIAttendance
	convResumed, err := handoffSvc.ResumeAIAttendance(ctx, tenantID, conv.ID, convHuman.Version, "Operador encerrou sua parte", "operator-joao")
	require.NoError(t, err)
	assert.Equal(t, domain.StateAIActive, convResumed.State)
	assert.Equal(t, int64(3), convResumed.Version)
	assert.Equal(t, int64(3), convResumed.ResumedAtSequence, "resumed_at_sequence deve ser o MAX(seq) das mensagens intermediárias (seq 3)")

	// Valida que o Scheduler NÃO reivindica nada para as mensagens 2 e 3 porque sequence <= resumed_at_sequence (INV-04, INV-06)
	claimedStale, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 10, time.Now().UTC())
	require.NoError(t, err)
	for _, c := range claimedStale {
		assert.NotEqual(t, conv.ID, c.ConversationID, "mensagens intermediárias 2 e 3 NÃO podem disparar execução retroativa")
	}

	// 5. Cliente envia nova mensagem (seq 4) APÓS a retomada da IA
	msg4 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-post-resume-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem 4 pós-retomada",
		CreatedAt:         time.Now().UTC(),
	}
	saved4, err := coreRepo.SaveMessage(ctx, msg4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), saved4.SequenceNumber)

	// Agenda e processa a mensagem 4
	_, err = schedRepo.ScheduleExecution(ctx, tenantID, conv.ID, time.Now().UTC().Add(-time.Second))
	require.NoError(t, err)

	claimedPostResume, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 10, time.Now().UTC())
	require.NoError(t, err)

	var foundPostResume *domain.ConversationSchedule
	for _, c := range claimedPostResume {
		if c.ConversationID == conv.ID {
			foundPostResume = &c
			break
		}
	}
	require.NotNil(t, foundPostResume, "mensagem 4 pós-retomada deve ser devidamente agendada e reivindicada")
	assert.Equal(t, domain.ScheduleStatusClaimed, foundPostResume.Status)

	// Processa mensagem 4 pelo pipeline
	evt4 := domain.Event[domain.MessageReceivedPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantID,
		ConversationID: conv.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         saved4.ID,
			ExternalMessageID: saved4.ExternalMessageID,
			Channel:           "whatsapp",
			Role:              saved4.Role,
			Content:           saved4.Content,
			SequenceNumber:    saved4.SequenceNumber,
		},
	}
	err = pipelineHandler.HandleMessageReceived(ctx, evt4)
	require.NoError(t, err)

	// Confirma que o resultado de seq 4 teve sucesso
	res4, err := resultsRepo.GetResult(ctx, tenantID, conv.ID, saved4.ID)
	require.NoError(t, err)
	require.NotNil(t, res4)
	assert.Equal(t, domain.ExecutionStatusSucceeded, res4.Status)
}

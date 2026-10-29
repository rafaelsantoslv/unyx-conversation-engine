package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecutionRequest_SnapshotIsolationE2E valida a Invariante INV-01, INV-04 e UDS-173:
// 1. Cria uma conversa e adiciona 3 mensagens sequenciais (seq 1, 2, 3).
// 2. Agenda a execução durável e executa ClaimDueSchedulesWithOutbox:
//    - Deve gerar a ExecutionRequest com snapshot delimitado em watermark_sequence = 3.
//    - Deve incluir no Transactional Outbox o evento 'conversation.execution.ready' apontando para essa ExecutionRequest.
// 3. Concorrentemente adiciona a mensagem seq 4 enquanto o processamento ocorre.
// 4. Carrega as mensagens do snapshot da ExecutionRequest e valida que seq 4 é estritamente ignorada,
//    garantindo que o Agent Runtime receba um snapshot imutável e determinístico.
func TestExecutionRequest_SnapshotIsolationE2E(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	schedRepo := postgres.NewScheduleRepository(pool)
	execReqRepo := postgres.NewExecutionRequestRepository(pool)

	tenantID := "tenant-snapshot-e2e"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	// 1. Insere mensagens seq 1, 2 e 3
	for i := 1; i <= 3; i++ {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-snap-%d-%s", i, uuid.New().String()),
			Role:              domain.RoleUser,
			Content:           fmt.Sprintf("Parte %d do pensamento", i),
			CreatedAt:         time.Now().UTC(),
		}
		_, err := coreRepo.SaveMessage(ctx, msg)
		require.NoError(t, err)
	}

	// 2. Agenda a conversa para o presente (já devida)
	now := time.Now().UTC()
	_, err = schedRepo.ScheduleExecution(ctx, tenantID, conv.ID, now.Add(-time.Second))
	require.NoError(t, err)

	// 3. O scheduler reivindica a execução devida, gerando ExecutionRequest e Outbox Event
	claimed, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 10, now)
	require.NoError(t, err)

	var claimedSched *domain.ConversationSchedule
	for _, c := range claimed {
		if c.ConversationID == conv.ID {
			claimedSched = &c
			break
		}
	}
	require.NotNil(t, claimedSched, "claimed schedule must exist for conversation")

	// 4. Concorrentemente chega a mensagem seq 4 (usuário continuou digitando após o fechamento da janela)
	msg4 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-snap-4-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem 4 que chegou após o corte da janela",
		CreatedAt:         time.Now().UTC(),
	}
	saved4, err := coreRepo.SaveMessage(ctx, msg4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), saved4.SequenceNumber)

	// 5. Verifica o evento emitido na Transactional Outbox
	outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
	require.NoError(t, err)

	var foundEvent *domain.OutboxEvent
	for _, evt := range outboxEvents {
		if evt.ConversationID == conv.ID && evt.EventType == domain.EventTypeExecutionReady {
			foundEvent = evt
			break
		}
	}
	require.NotNil(t, foundEvent, "execution ready event must be present in outbox")

	// 6. Obtém a ExecutionRequest gerada automaticamente e valida seu snapshot
	var payload domain.Event[domain.ExecutionReadyPayload]
	err = json.Unmarshal(foundEvent.Payload, &payload)
	require.NoError(t, err)
	assert.NotEmpty(t, payload.Payload.ExecutionRequestID)
	assert.Equal(t, int64(3), payload.Payload.WatermarkSequence)
	assert.Len(t, payload.Payload.MessageIDs, 3)

	reqFromDB, err := execReqRepo.GetExecutionRequest(ctx, payload.Payload.ExecutionRequestID)
	require.NoError(t, err)
	require.NotNil(t, reqFromDB)
	assert.Equal(t, int64(3), reqFromDB.WatermarkSequence)
	assert.Len(t, reqFromDB.MessageIDs, 3)

	// Busca mensagens estritamente do snapshot
	snapshotMsgs, err := execReqRepo.FetchMessagesForRequest(ctx, reqFromDB)
	require.NoError(t, err)

	// Critério de aceite: O snapshot contém EXATAMENTE 3 mensagens (seq 1, 2 e 3)
	// A mensagem 4 que chegou após a geração do snapshot é ignorada (INV-04)
	assert.Len(t, snapshotMsgs, 3)
	for _, m := range snapshotMsgs {
		assert.LessOrEqual(t, m.SequenceNumber, int64(3))
		assert.NotEqual(t, saved4.ID, m.ID)
		assert.NotEqual(t, int64(4), m.SequenceNumber)
	}
	assert.False(t, reqFromDB.IncludesMessage(4, saved4.ID))
}

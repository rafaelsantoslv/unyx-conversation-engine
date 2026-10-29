package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionRequestRepository_SnapshotIsolation(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	execReqRepo := postgres.NewExecutionRequestRepository(pool)

	tenantID := "tenant-snapshot-test"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	// 1. Insere mensagens seq 1, 2 e 3
	for i := 1; i <= 3; i++ {
		msg := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-msg-%d-%s", i, uuid.New().String()),
			Role:              domain.RoleUser,
			Content:           fmt.Sprintf("Mensagem %d", i),
			CreatedAt:         time.Now().UTC(),
		}
		_, err := coreRepo.SaveMessage(ctx, msg)
		require.NoError(t, err)
	}

	// 2. Cria o Snapshot lógico da ExecutionRequest (deve capturar mensagens 1 a 3 com watermark = 3)
	req, err := execReqRepo.CreateSnapshotRequest(ctx, tenantID, conv.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, req.ID)
	assert.Equal(t, int64(3), req.WatermarkSequence)
	assert.Len(t, req.MessageIDs, 3)
	assert.Equal(t, domain.ExecutionRequestStatusPending, req.Status)

	// 3. Concorrentemente chega a mensagem seq 4 enquanto o processamento do snapshot ocorre
	msg4 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-msg-4-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem 4 enviada durante a execucao",
		CreatedAt:         time.Now().UTC(),
	}
	saved4, err := coreRepo.SaveMessage(ctx, msg4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), saved4.SequenceNumber)

	// 4. Busca as mensagens do snapshot da ExecutionRequest
	snapshotMessages, err := execReqRepo.FetchMessagesForRequest(ctx, req)
	require.NoError(t, err)

	// Critério de aceite: O snapshot contém ESTRITAMENTE as mensagens 1, 2 e 3.
	// A mensagem 4 NÃO PODE aparecer no snapshot (INV-04)
	assert.Len(t, snapshotMessages, 3)
	for _, m := range snapshotMessages {
		assert.LessOrEqual(t, m.SequenceNumber, int64(3))
		assert.NotEqual(t, int64(4), m.SequenceNumber)
		assert.True(t, req.IncludesMessage(m.SequenceNumber, m.ID))
	}

	// 5. Atualiza status para RUNNING e depois COMPLETED
	err = execReqRepo.UpdateStatus(ctx, req.ID, domain.ExecutionRequestStatusRunning)
	require.NoError(t, err)

	fetchedReq, err := execReqRepo.GetExecutionRequest(ctx, req.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ExecutionRequestStatusRunning, fetchedReq.Status)

	err = execReqRepo.UpdateStatus(ctx, req.ID, domain.ExecutionRequestStatusCompleted)
	require.NoError(t, err)

	fetchedReq, err = execReqRepo.GetExecutionRequest(ctx, req.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ExecutionRequestStatusCompleted, fetchedReq.Status)
}

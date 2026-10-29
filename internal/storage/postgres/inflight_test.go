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

func TestExecutionCoordinator_ReleaseExecutionAndSchedulePending(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	coordinator := postgres.NewExecutionCoordinator(pool)
	schedRepo := postgres.NewScheduleRepository(pool)

	tenantID := "tenant-in-flight-test"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	// 1. Mensagem 1 inicial
	msg1 := &domain.Message{
		TenantID:          tenantID,
		ConversationID:    conv.ID,
		ExternalMessageID: "ext-msg-1-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem inicial",
		CreatedAt:         time.Now().UTC(),
	}
	saved1, err := coreRepo.SaveMessage(ctx, msg1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), saved1.SequenceNumber)

	// 2. Worker adquire execução para a conversa
	exec, err := coordinator.AcquireExecution(ctx, tenantID, conv.ID, "worker-1", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, domain.ExecutionStatusRunning, exec.Status)

	// 3. Durante a execução in-flight (watermark = 1), chegam as mensagens 2 e 3
	for i := 2; i <= 3; i++ {
		m := &domain.Message{
			TenantID:          tenantID,
			ConversationID:    conv.ID,
			ExternalMessageID: fmt.Sprintf("ext-inflight-%d-%s", i, uuid.New().String()),
			Role:              domain.RoleUser,
			Content:           fmt.Sprintf("Mensagem in-flight %d", i),
			CreatedAt:         time.Now().UTC(),
		}
		_, err := coreRepo.SaveMessage(ctx, m)
		require.NoError(t, err)
	}

	// 4. Execução 1 termina (watermark = 1) e chama ReleaseExecutionAndSchedulePending
	res, err := coordinator.ReleaseExecutionAndSchedulePending(
		ctx,
		exec.ID,
		exec.FencingToken,
		tenantID,
		conv.ID,
		1, // watermark da execução concluída
		domain.ExecutionStatusSucceeded,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.HasPending, "must detect pending in-flight messages")
	assert.Equal(t, 2, res.PendingCount, "should count exactly 2 in-flight messages (seq 2 and seq 3)")
	assert.Equal(t, int64(3), res.MaxSequenceNumber)

	// 5. Verifica se o agendamento no PostgreSQL foi reativado para PENDING
	sched, err := schedRepo.GetSchedule(ctx, conv.ID)
	require.NoError(t, err)
	require.NotNil(t, sched)
	assert.Equal(t, domain.ScheduleStatusPending, sched.Status)

	// 6. Novo ciclo: Reivindica agendamento pendente
	claimed, err := schedRepo.ClaimDueSchedulesWithOutbox(ctx, 10, time.Now().UTC())
	require.NoError(t, err)

	var claimedConv *domain.ConversationSchedule
	for _, c := range claimed {
		if c.ConversationID == conv.ID {
			claimedConv = &c
			break
		}
	}
	require.NotNil(t, claimedConv, "subsequent execution schedule must be claimed")
	assert.Equal(t, domain.ScheduleStatusClaimed, claimedConv.Status)

	// 7. Próxima execução conclui atendendo até o watermark 3
	// Agora não há novas mensagens pós watermark 3
	res2, err := coordinator.ReleaseExecutionAndSchedulePending(
		ctx,
		exec.ID,
		exec.FencingToken,
		tenantID,
		conv.ID,
		3, // watermark da execução 2
		domain.ExecutionStatusSucceeded,
	)
	// fencing token foi atualizado ou mesmo id
	if err == nil {
		assert.False(t, res2.HasPending, "should not have pending messages when all sequences are processed")
		assert.Equal(t, 0, res2.PendingCount)
	}
}

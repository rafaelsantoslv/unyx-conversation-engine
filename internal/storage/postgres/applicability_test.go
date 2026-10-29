package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultsRepository_ApplicabilityValidation(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	resultsRepo := postgres.NewResultsRepository(pool)

	tenantID := "tenant-applicability"
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	msgID := uuid.New().String()
	_, err = resultsRepo.AcquireExecutionLock(ctx, tenantID, conv.ID, msgID, 30*time.Second)
	require.NoError(t, err)

	output := &domain.ExecutionOutput{
		ResponseContent: "Resposta gerada pela IA",
		TokensUsed:      30,
		ExecutedAt:      time.Now().UTC(),
	}

	t.Run("success when conversation is AI_ACTIVE and version matches", func(t *testing.T) {
		res, err := resultsRepo.RecordExecutionSuccessWithApplicability(
			ctx, tenantID, conv.ID, msgID, output, 100*time.Millisecond, 0, conv.Version,
		)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.True(t, res.IsApplicable)
		assert.Equal(t, domain.ExecutionStatusSucceeded, res.FinalStatus)

		// Verifica se o evento message.outbound.intent foi inserido na Outbox
		outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
		require.NoError(t, err)

		var foundOutbound *domain.OutboxEvent
		for _, ob := range outboxEvents {
			if ob.ConversationID == conv.ID && ob.EventType == domain.EventTypeOutboundIntent {
				foundOutbound = ob
				break
			}
		}
		require.NotNil(t, foundOutbound, "outbox event for outbound intent must exist on valid applicability")
		assert.Equal(t, conv.ID, foundOutbound.PartitionKey)
	})

	t.Run("discards result and suppresses outbound event if state changed to HUMAN_ACTIVE (INV-06, INV-07)", func(t *testing.T) {
		convHuman, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-human-"+uuid.New().String())
		require.NoError(t, err)

		msgHumanID := uuid.New().String()
		_, err = resultsRepo.AcquireExecutionLock(ctx, tenantID, convHuman.ID, msgHumanID, 30*time.Second)
		require.NoError(t, err)

		// Simula operador assumindo o atendimento durante a computação (muda para HUMAN_ACTIVE)
		_, err = pool.Exec(ctx, "UPDATE conversations SET status = 'HUMAN_ACTIVE', version = version + 1 WHERE id = $1", convHuman.ID)
		require.NoError(t, err)

		res, err := resultsRepo.RecordExecutionSuccessWithApplicability(
			ctx, tenantID, convHuman.ID, msgHumanID, output, 150*time.Millisecond, 0, 1,
		)
		require.NoError(t, err)
		require.NotNil(t, res)

		// Critério de aceite: Não é aplicável, marcado como DISCARDED_STALE
		assert.False(t, res.IsApplicable)
		assert.Equal(t, domain.ExecutionStatusDiscardedStale, res.FinalStatus)
		assert.Contains(t, res.Reason, "HUMAN_ACTIVE")

		// Critério de aceite: NENHUM evento de outbound intent deve ser gerado na Outbox
		outboxEvents, err := coreRepo.FetchPendingOutboxEvents(ctx, 100)
		require.NoError(t, err)

		for _, ob := range outboxEvents {
			if ob.ConversationID == convHuman.ID && ob.EventType == domain.EventTypeOutboundIntent {
				t.Fatalf("VIOLATION: outbound intent was generated for discarded stale result!")
			}
		}

		// Verifica se o status foi gravado no banco como DISCARDED_STALE
		persistedRes, err := resultsRepo.GetResult(ctx, tenantID, convHuman.ID, msgHumanID)
		require.NoError(t, err)
		assert.Equal(t, domain.ExecutionStatusDiscardedStale, persistedRes.Status)
		assert.NotNil(t, persistedRes.ErrorMessage)
		assert.Contains(t, *persistedRes.ErrorMessage, "HUMAN_ACTIVE")
	})
}

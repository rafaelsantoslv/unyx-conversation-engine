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

func TestOutboundIntentsRepository_PersistenceAndConcurrency(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	outboundRepo := postgres.NewOutboundIntentsRepository(pool)

	tenantID := "tenant-outbound-" + uuid.New().String()
	conv, err := coreRepo.GetOrCreateConversation(ctx, tenantID, "ext-"+uuid.New().String())
	require.NoError(t, err)

	execID := uuid.New().String()

	t.Run("CreateIntent creates durable PENDING intent", func(t *testing.T) {
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: conv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content:        "Olá! Sua reserva foi confirmada.",
				Channel:        "whatsapp",
				TokensUsed:     45,
				IdempotencyKey: "idem-" + execID,
			},
		}

		created, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)
		require.NotNil(t, created)
		assert.Equal(t, domain.OutboundStatusPending, created.Status)
		assert.Equal(t, "Olá! Sua reserva foi confirmada.", created.Payload.Content)

		// Idempotência: Inserir com o mesmo execution_id retorna o mesmo registro sem duplicar
		created2, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)
		assert.Equal(t, created.ID, created2.ID)
	})

	t.Run("ClaimPendingIntents acquires locks atomically and transitions to SENDING", func(t *testing.T) {
		claimed, err := outboundRepo.ClaimPendingIntents(ctx, 10, time.Now().UTC())
		require.NoError(t, err)

		var found *domain.OutboundIntent
		for _, c := range claimed {
			if c.ExecutionID == execID {
				found = &c
				break
			}
		}
		require.NotNil(t, found, "Intent must be claimed by worker")
		assert.Equal(t, domain.OutboundStatusPending, found.Status, "Returned slice contains snapshot before atomic transition")

		// Segunda tentativa concorrente imediata não deve encontrar o mesmo item (já está em SENDING)
		claimed2, err := outboundRepo.ClaimPendingIntents(ctx, 10, time.Now().UTC())
		require.NoError(t, err)
		for _, c := range claimed2 {
			assert.NotEqual(t, execID, c.ExecutionID, "Intent must not be claimed twice concurrently")
		}

		// Marca como SENT
		err = outboundRepo.MarkSent(ctx, found.ID, "ext-msg-chatwoot-999")
		require.NoError(t, err)

		// Consulta estado final
		saved, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusSent, saved.Status)
		require.NotNil(t, saved.ExternalMessageID)
		assert.Equal(t, "ext-msg-chatwoot-999", *saved.ExternalMessageID)
	})
}

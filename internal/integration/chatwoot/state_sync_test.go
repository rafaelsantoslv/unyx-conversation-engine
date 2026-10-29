package chatwoot_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping test: database connection failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping test: database ping failed: %v", err)
	}
	return pool
}

func TestStateSyncService_LifecycleSynchronization(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	mappingRepo := postgres.NewIdentityMappingRepository(pool)
	syncSvc := chatwoot.NewStateSyncService(coreRepo, mappingRepo)

	tenantID := "tenant-sync-" + uuid.New().String()
	extConvID := "9988"

	// 1. Cria conversa inicial em AI_ACTIVE
	convInit, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantID, "chatwoot", "1", "10", extConvID, "50", "whatsapp",
	)
	require.NoError(t, err)
	assert.Equal(t, domain.StateAIActive, convInit.State)
	assert.Equal(t, int64(1), convInit.Version)

	t.Run("Assignee assigned in Chatwoot transitions to HUMAN_ACTIVE", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			Event: "conversation_updated",
			Conversation: &cwDto.ChatwootConversation{
				ID:         9988,
				Status:     "open",
				AssigneeID: 42, // Atendente humano assumiu
			},
			Sender: &cwDto.ChatwootSender{
				Name: "Operador Carlos",
			},
		}

		convUpdated, err := syncSvc.SyncChatwootEvent(ctx, tenantID, payload)
		require.NoError(t, err)
		require.NotNil(t, convUpdated)
		assert.Equal(t, domain.StateHumanActive, convUpdated.State)
		assert.Equal(t, int64(2), convUpdated.Version)
	})

	t.Run("Resolution in Chatwoot transitions to CLOSED", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			Event: "conversation_status_changed",
			Conversation: &cwDto.ChatwootConversation{
				ID:         9988,
				Status:     "resolved",
				AssigneeID: 0,
			},
		}

		convUpdated, err := syncSvc.SyncChatwootEvent(ctx, tenantID, payload)
		require.NoError(t, err)
		require.NotNil(t, convUpdated)
		assert.Equal(t, domain.StateClosed, convUpdated.State)
		assert.Equal(t, int64(3), convUpdated.Version)
	})

	t.Run("Reopen in Chatwoot transitions to AI_ACTIVE", func(t *testing.T) {
		payload := &cwDto.ChatwootWebhookPayload{
			Event: "conversation_status_changed",
			Conversation: &cwDto.ChatwootConversation{
				ID:         9988,
				Status:     "open",
				AssigneeID: 0,
			},
		}

		convUpdated, err := syncSvc.SyncChatwootEvent(ctx, tenantID, payload)
		require.NoError(t, err)
		require.NotNil(t, convUpdated)
		assert.Equal(t, domain.StateAIActive, convUpdated.State)
		assert.Equal(t, int64(4), convUpdated.Version)
	})
}

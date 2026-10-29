package outbound_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/outbound"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping postgres integration test: failed to connect to %s: %v", dbURL, err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres integration test: database ping failed: %v", err)
	}

	migrationsPath := filepath.Join("..", "..", "migrations")
	if err := postgres.RunMigrations(ctx, pool, migrationsPath); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	return pool
}

func TestOutboundWorker_DispatchAndResilience(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	// Limpa intenções residuais na tabela para garantir isolamento limpo do teste
	_, _ = pool.Exec(ctx, "DELETE FROM outbound_intents;")

	coreRepo := postgres.NewRepository(pool)

	outboundRepo := postgres.NewOutboundIntentsRepository(pool)
	integrationRepo := postgres.NewIntegrationRepository(pool)
	mappingRepo := postgres.NewIdentityMappingRepository(pool)

	tenantID := "tenant-outbound-test-" + uuid.New().String()
	extConvID := "5566"
	extAccountID := "1"
	extInboxID := "10"

	// 1. Cadastra mapeamento de conversa (já cria/resolve a conversa correspondente)
	conv, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantID, "chatwoot", extAccountID, extInboxID, extConvID, "100", "whatsapp",
	)
	require.NoError(t, err)


	t.Run("Successfully dispatches pending intent to Chatwoot API and marks SENT", func(t *testing.T) {
		var receivedAuth string
		var receivedBody cwDto.ChatwootSendRequestDTO

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("api_access_token")
			_ = json.NewDecoder(r.Body).Decode(&receivedBody)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(cwDto.ChatwootSendResponseDTO{
				ID:             778899,
				Content:        receivedBody.Content,
				MessageType:    "outgoing",
				CreatedAt:      time.Now().Unix(),
				ConversationID: 5566,
			})
		}))
		defer server.Close()

		// Configura integração do tenant apontando para o mock server
		err := integrationRepo.UpsertIntegration(ctx, postgres.TenantIntegration{
			TenantID:       tenantID,
			Channel:        "chatwoot",
			WebhookToken:   "token-123",
			APIURL:         server.URL,
			APIAccessToken: "chatwoot-secret-token",
			IsActive:       true,
		})
		require.NoError(t, err)

		execID := uuid.New().String()
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: conv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content: "Olá! Despachado com sucesso via worker.",
			},
		}
		created, err := outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)

		sender := chatwoot.NewChatwootOutboundSender(server.Client())
		workerSvc := outbound.NewOutboundWorkerService(
			outboundRepo,
			integrationRepo,
			mappingRepo,
			coreRepo,
			sender,
		)

		processed, err := workerSvc.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)

		assert.Equal(t, "chatwoot-secret-token", receivedAuth)
		assert.Equal(t, "Olá! Despachado com sucesso via worker.", receivedBody.Content)

		saved, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusSent, saved.Status)
		require.NotNil(t, saved.ExternalMessageID)
		assert.Equal(t, "778899", *saved.ExternalMessageID)
		_ = created
	})

	t.Run("Cancels intent if conversation changed to HUMAN_ACTIVE (INV-05)", func(t *testing.T) {
		// Transiciona conversa para HUMAN_ACTIVE
		c, err := coreRepo.GetConversationByID(ctx, conv.ID)
		require.NoError(t, err)
		_, err = coreRepo.TransitionConversationState(
			ctx, tenantID, conv.ID, domain.StateHumanActive, c.Version, "operator takeover", "agent-joao",
		)
		require.NoError(t, err)

		execID := uuid.New().String()
		intent := domain.OutboundIntent{
			TenantID:       tenantID,
			ConversationID: conv.ID,
			ExecutionID:    execID,
			Payload: domain.OutboundPayload{
				Content: "Mensagem que não deve ser entregue!",
			},
		}
		_, err = outboundRepo.CreateIntent(ctx, intent)
		require.NoError(t, err)

		workerSvc := outbound.NewOutboundWorkerService(
			outboundRepo,
			integrationRepo,
			mappingRepo,
			coreRepo,
			chatwoot.NewChatwootOutboundSender(nil),
		)

		processed, err := workerSvc.ProcessBatch(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, processed)

		saved, err := outboundRepo.GetIntentByExecutionID(ctx, tenantID, execID)
		require.NoError(t, err)
		assert.Equal(t, domain.OutboundStatusCancelled, saved.Status)
	})
}

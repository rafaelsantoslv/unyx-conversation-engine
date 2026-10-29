package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/outbound"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/transport/http/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type slowMockChatwootSender struct {
	started chan struct{}
	block   chan struct{}
}

func (s *slowMockChatwootSender) SendMessage(ctx context.Context, params chatwoot.SendMessageParams) (*ports.DeliveryReceipt, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	<-s.block
	return &ports.DeliveryReceipt{
		ExternalMessageID: "cw-msg-9999",
		DeliveredAt:       time.Now().UTC(),
		Status:            "DELIVERED",
	}, nil
}

func (s *slowMockChatwootSender) ReconcileAmbiguousIntent(ctx context.Context, baseURL, apiAccessToken, accountID, conversationID string, expectedEchoID, expectedCorrelationID, expectedContent string) (*ports.DeliveryReceipt, error) {
	return nil, nil
}

func TestE2E_GracefulShutdownAndProbes(t *testing.T) {
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	ctx := context.Background()

	// 1. Probes de Liveness (/healthz) e Readiness (/readyz)
	healthHandler := handler.NewHealthHandler(pool)

	// Liveness deve responder 200 OK mesmo que dependências sofram
	reqLive := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recLive := httptest.NewRecorder()
	healthHandler.LivenessHandler(recLive, reqLive)
	assert.Equal(t, http.StatusOK, recLive.Code)
	assert.Contains(t, recLive.Body.String(), `"status":"alive"`)

	// Readiness deve responder 200 OK com banco online
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	healthHandler.ReadinessHandler(recReady, reqReady)
	assert.Equal(t, http.StatusOK, recReady.Code)
	assert.Contains(t, recReady.Body.String(), `"status":"ready"`)

	// 2. Coordinated Graceful Shutdown: Worker drena tarefa ativa em andamento antes de parar
	coreRepo := postgres.NewRepository(pool)
	outboundRepo := postgres.NewOutboundIntentsRepository(pool)
	integrationRepo := postgres.NewIntegrationRepository(pool)
	mappingRepo := postgres.NewIdentityMappingRepository(pool)

	tenantID := "tenant_graceful_" + uuid.NewString()[:8]
	webhookToken := "token_graceful"

	err := integrationRepo.UpsertIntegration(ctx, postgres.TenantIntegration{
		TenantID:       tenantID,
		Channel:        "chatwoot",
		WebhookToken:   webhookToken,
		APIURL:         "https://chatwoot.example.com",
		APIAccessToken: "cw-access-token",
		IsActive:       true,
	})
	require.NoError(t, err)

	cwAccountID := int64(10)
	cwInboxID := int64(20)
	cwConvID := int64(8812)
	cwContactID := int64(88)

	// Cria mapeamento de canal e conversa de teste
	conv, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantID, "chatwoot",
		fmt.Sprintf("%d", cwAccountID),
		fmt.Sprintf("%d", cwInboxID),
		fmt.Sprintf("%d", cwConvID),
		fmt.Sprintf("%d", cwContactID),
		"whatsapp",
	)
	require.NoError(t, err)
	require.NotNil(t, conv)

	// Cria Outbound Intent pendente
	intent, err := outboundRepo.CreateIntent(ctx, domain.OutboundIntent{
		TenantID:       tenantID,
		ConversationID: conv.ID,
		ExecutionID:    uuid.NewString(),
		Payload: domain.OutboundPayload{
			Content: "Mensagem de teste de graceful shutdown",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.OutboundStatusPending, intent.Status)

	// Sender que bloqueia para simular trabalho ativo
	slowSender := &slowMockChatwootSender{
		started: make(chan struct{}),
		block:   make(chan struct{}),
	}

	workerSvc := outbound.NewOutboundWorkerService(
		outboundRepo,
		integrationRepo,
		mappingRepo,
		coreRepo,
		slowSender,
		outbound.WithPollInterval(20*time.Millisecond),
		outbound.WithBatchSize(1),
	)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerStopped := make(chan struct{})

	go func() {
		defer close(workerStopped)
		workerSvc.Start(workerCtx)
	}()

	// Aguarda worker adquirir e iniciar o envio da mensagem
	select {
	case <-slowSender.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout aguardando worker iniciar envio da mensagem")
	}

	// Cancela o contexto (simulando SIGTERM no container) enquanto o envio está em voo (in-flight)
	cancelWorker()

	// Garante que o worker NÃO encerra abruptamente antes de concluir a mensagem
	select {
	case <-workerStopped:
		t.Fatal("worker encerrou imediatamente sem drenar a tarefa em andamento!")
	case <-time.After(150 * time.Millisecond):
		// Sucesso: worker continua aguardando o envio concluir
	}

	// Desbloqueia o sender para permitir conclusão com sucesso
	close(slowSender.block)

	// Agora o worker deve encerrar graciosamente dentro do deadline
	select {
	case <-workerStopped:
		// Sucesso total no shutdown
	case <-time.After(5 * time.Second):
		t.Fatal("timeout aguardando worker concluir shutdown gracioso")
	}

	// Valida se a intenção foi persistida com status SENT (concluído com sucesso pelo worker)
	var savedStatus string
	err = pool.QueryRow(ctx, "SELECT status FROM outbound_intents WHERE id = $1", intent.ID).Scan(&savedStatus)
	require.NoError(t, err)
	assert.Equal(t, "SENT", savedStatus)
}

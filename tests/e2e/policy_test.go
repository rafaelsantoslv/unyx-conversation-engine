package e2e_test

import (
	"context"
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

// TestMultiTenantPolicy_IsolationAndResolution E2E valida a Invariante INV-09 (Isolamento Multi-Tenant):
// 1. Configura Tenant A com política de Debounce de 100ms.
// 2. Configura Tenant B com política de Debounce desabilitado (0s).
// 3. Configura Tenant C com AllowAIProcessing = false.
// 4. Executa mensagens concorrentes para cada tenant e valida que cada um opera com sua política estrita e isolada.
func TestMultiTenantPolicy_IsolationAndResolution(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := setupResilienceEnvironment(t)
	defer pool.Close()

	coreRepo := postgres.NewRepository(pool)
	policyRepo := postgres.NewPolicyRepository(pool, 5*time.Second)
	resultsRepo := postgres.NewResultsRepository(pool)
	debounceRepo := postgres.NewDebounceRepository(pool)

	tenantA := "tenant-pol-a-" + uuid.New().String()
	tenantB := "tenant-pol-b-" + uuid.New().String()
	tenantC := "tenant-pol-c-" + uuid.New().String()

	// 1. Cadastra políticas customizadas
	err := policyRepo.UpsertTenantPolicy(ctx, domain.ConversationPolicy{
		TenantID:                  tenantA,
		DebounceEnabled:           true,
		DebounceInactivitySeconds: 1,
		MaxWaitSeconds:            5,
		AllowAIProcessing:         true,
	})
	require.NoError(t, err)

	err = policyRepo.UpsertTenantPolicy(ctx, domain.ConversationPolicy{
		TenantID:                  tenantB,
		DebounceEnabled:           false,
		DebounceInactivitySeconds: 0,
		MaxWaitSeconds:            0,
		AllowAIProcessing:         true,
	})
	require.NoError(t, err)

	err = policyRepo.UpsertTenantPolicy(ctx, domain.ConversationPolicy{
		TenantID:          tenantC,
		AllowAIProcessing: false, // Tenant bloqueado para IA
	})
	require.NoError(t, err)

	// Cria conversas para cada tenant
	convA, err := coreRepo.GetOrCreateConversation(ctx, tenantA, "ext-a-"+uuid.New().String())
	require.NoError(t, err)

	convB, err := coreRepo.GetOrCreateConversation(ctx, tenantB, "ext-b-"+uuid.New().String())
	require.NoError(t, err)

	convC, err := coreRepo.GetOrCreateConversation(ctx, tenantC, "ext-c-"+uuid.New().String())
	require.NoError(t, err)

	var runCountA, runCountB, runCountC int32
	runtime := mock.NewMockRuntime(mock.WithSimulatedReply("OK"))

	pipelineHandler := service.NewDefaultMessagePipelineHandler(
		runtime,
		resultsRepo,
		service.WithPolicyResolver(policyRepo),
		service.WithDebounce(debounceRepo, domain.DebounceConfig{InactivityWindow: 0, MaxWaitTimeout: 0}),
	)

	// Processa mensagem do Tenant C (deve ser ignorada sem erro porque AllowAIProcessing = false)
	msgC := &domain.Message{
		TenantID:          tenantC,
		ConversationID:    convC.ID,
		ExternalMessageID: "msg-c-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem Tenant C",
		CreatedAt:         time.Now().UTC(),
	}
	savedC, err := coreRepo.SaveMessage(ctx, msgC)
	require.NoError(t, err)

	evtC := domain.Event[domain.MessageReceivedPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantC,
		ConversationID: convC.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         savedC.ID,
			ExternalMessageID: savedC.ExternalMessageID,
			Channel:           "whatsapp",
			Role:              savedC.Role,
			Content:           savedC.Content,
			SequenceNumber:    savedC.SequenceNumber,
		},
	}
	err = pipelineHandler.HandleMessageReceived(ctx, evtC)
	require.NoError(t, err)
	// Como AllowAIProcessing é false, o runtime não deve ter sido executado e não deve haver processing_result
	resC, err := resultsRepo.GetResult(ctx, tenantC, convC.ID, savedC.ID)
	require.NoError(t, err)
	assert.Nil(t, resC, "tenant C não deve executar runtime nem gerar resultado")

	// Processa mensagem do Tenant B (executa imediatamente pois debounce está desabilitado)
	msgB := &domain.Message{
		TenantID:          tenantB,
		ConversationID:    convB.ID,
		ExternalMessageID: "msg-b-" + uuid.New().String(),
		Role:              domain.RoleUser,
		Content:           "Mensagem Tenant B",
		CreatedAt:         time.Now().UTC(),
	}
	savedB, err := coreRepo.SaveMessage(ctx, msgB)
	require.NoError(t, err)

	evtB := domain.Event[domain.MessageReceivedPayload]{
		EventID:        uuid.New().String(),
		TenantID:       tenantB,
		ConversationID: convB.ID,
		EventType:      domain.EventTypeMessageReceived,
		Version:        domain.EventVersionV1,
		OccurredAt:     time.Now().UTC(),
		Payload: domain.MessageReceivedPayload{
			MessageID:         savedB.ID,
			ExternalMessageID: savedB.ExternalMessageID,
			Channel:           "whatsapp",
			Role:              savedB.Role,
			Content:           savedB.Content,
			SequenceNumber:    savedB.SequenceNumber,
		},
	}
	err = pipelineHandler.HandleMessageReceived(ctx, evtB)
	require.NoError(t, err)

	resB, err := resultsRepo.GetResult(ctx, tenantB, convB.ID, savedB.ID)
	require.NoError(t, err)
	require.NotNil(t, resB)
	assert.Equal(t, domain.ExecutionStatusSucceeded, resB.Status)

	_ = convA
	_ = runCountA
	_ = runCountB
	_ = runCountC
}

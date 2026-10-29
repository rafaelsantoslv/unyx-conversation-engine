package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityMappingRepository_CrossTenantIsolation(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	mappingRepo := postgres.NewIdentityMappingRepository(pool)

	tenantA := "tenant-map-a-" + uuid.New().String()
	tenantB := "tenant-map-b-" + uuid.New().String()

	// Ambas as empresas possuem exatamente o mesmo external_conversation_id = "42" e account_id = "1" no Chatwoot!
	extConvID := "42"
	extAccountID := "1"
	extInboxID := "10"
	extContactID := "999"

	// Resolve/Cria para Tenant A
	convA, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantA, "chatwoot", extAccountID, extInboxID, extConvID, extContactID, "whatsapp",
	)
	require.NoError(t, err)
	require.NotNil(t, convA)
	assert.Equal(t, tenantA, convA.TenantID)

	// Resolve/Cria para Tenant B
	convB, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantB, "chatwoot", extAccountID, extInboxID, extConvID, extContactID, "whatsapp",
	)
	require.NoError(t, err)
	require.NotNil(t, convB)
	assert.Equal(t, tenantB, convB.TenantID)

	// Invariante de Segurança e Isolamento INV-01: Os IDs internos UUID DEVEM SER DISTINTOS!
	assert.NotEqual(t, convA.ID, convB.ID, "Conversations from different tenants with identical external IDs must have distinct internal UUIDs")

	// Idempotência: Chamar novamente para Tenant A deve retornar o mesmo convA.ID
	convA2, err := mappingRepo.ResolveOrCreateConversationMapping(
		ctx, tenantA, "chatwoot", extAccountID, extInboxID, extConvID, extContactID, "whatsapp",
	)
	require.NoError(t, err)
	assert.Equal(t, convA.ID, convA2.ID)

	// Consulta reversa por UUID interno
	mapA, err := mappingRepo.GetMappingByConversationID(ctx, tenantA, convA.ID)
	require.NoError(t, err)
	require.NotNil(t, mapA)
	assert.Equal(t, extConvID, mapA.ExternalConversationID)
	assert.Equal(t, extAccountID, mapA.ExternalAccountID)

	// Tenant B não pode enxergar mapeamento de Tenant A
	mapBlind, err := mappingRepo.GetMappingByConversationID(ctx, tenantB, convA.ID)
	require.NoError(t, err)
	assert.Nil(t, mapBlind, "Tenant B must never access mapping of Tenant A")
}

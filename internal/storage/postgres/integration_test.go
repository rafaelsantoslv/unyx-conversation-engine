package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/storage/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationRepository_MultiTenantIsolation(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewIntegrationRepository(pool)
	tenantA := "tenant-int-a-" + uuid.New().String()
	tenantB := "tenant-int-b-" + uuid.New().String()

	err := repo.UpsertIntegration(ctx, postgres.TenantIntegration{
		TenantID:     tenantA,
		Channel:      "chatwoot",
		WebhookToken: "token-secret-a",
		APIURL:       "https://chatwoot.tenant-a.com",
		IsActive:     true,
	})
	require.NoError(t, err)

	err = repo.UpsertIntegration(ctx, postgres.TenantIntegration{
		TenantID:     tenantB,
		Channel:      "chatwoot",
		WebhookToken: "token-secret-b",
		APIURL:       "https://chatwoot.tenant-b.com",
		IsActive:     true,
	})
	require.NoError(t, err)

	// Validação de token Tenant A
	validA, err := repo.ValidateWebhookToken(ctx, tenantA, "chatwoot", "token-secret-a")
	require.NoError(t, err)
	assert.True(t, validA)

	// Validação de token Tenant A com token de B (deve ser rejeitado)
	invalidA, err := repo.ValidateWebhookToken(ctx, tenantA, "chatwoot", "token-secret-b")
	require.NoError(t, err)
	assert.False(t, invalidA)

	// Tenant desconhecido
	validUnknown, err := repo.ValidateWebhookToken(ctx, "tenant-unknown", "chatwoot", "any-token")
	require.NoError(t, err)
	assert.False(t, validUnknown)
}

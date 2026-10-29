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

func TestPolicyRepository_MultiTenantIsolationAndFallback(t *testing.T) {
	ctx := context.Background()
	pool := setupTestDB(t)
	defer pool.Close()

	repo := postgres.NewPolicyRepository(pool, 5*time.Second)

	tenantA := "tenant-imobiliaria-" + uuid.New().String()
	tenantB := "tenant-ecommerce-" + uuid.New().String()
	tenantUnknown := "tenant-novo-" + uuid.New().String()

	// 1. Configura Tenant A (Debounce de 4s, MaxWait 20s)
	policyA := domain.ConversationPolicy{
		TenantID:                  tenantA,
		DebounceEnabled:           true,
		DebounceInactivitySeconds: 4,
		MaxWaitSeconds:            20,
		AllowAIProcessing:         true,
	}
	err := repo.UpsertTenantPolicy(ctx, policyA)
	require.NoError(t, err)

	// 2. Configura Tenant B (Debounce desligado / 0s, AllowAI false)
	policyB := domain.ConversationPolicy{
		TenantID:                  tenantB,
		DebounceEnabled:           false,
		DebounceInactivitySeconds: 0,
		MaxWaitSeconds:            0,
		AllowAIProcessing:         false,
	}
	err = repo.UpsertTenantPolicy(ctx, policyB)
	require.NoError(t, err)

	// 3. Resolução do Tenant A: deve retornar exatamente as configurações do Tenant A
	resolvedA, err := repo.ResolvePolicy(ctx, tenantA, "whatsapp")
	require.NoError(t, err)
	assert.True(t, resolvedA.DebounceEnabled)
	assert.Equal(t, 4, resolvedA.DebounceInactivitySeconds)
	assert.Equal(t, 20, resolvedA.MaxWaitSeconds)
	assert.True(t, resolvedA.AllowAIProcessing)

	// 4. Resolução do Tenant B: deve retornar exatamente as configurações do Tenant B (Isolamento INV-09)
	resolvedB, err := repo.ResolvePolicy(ctx, tenantB, "chatwoot")
	require.NoError(t, err)
	assert.False(t, resolvedB.DebounceEnabled)
	assert.Equal(t, 0, resolvedB.DebounceInactivitySeconds)
	assert.False(t, resolvedB.AllowAIProcessing)

	// 5. Tenant desconhecido (sem registro no banco): deve receber silenciosamente os defaults seguros
	resolvedUnknown, err := repo.ResolvePolicy(ctx, tenantUnknown, "default")
	require.NoError(t, err)
	defaultRef := domain.DefaultPolicy(tenantUnknown)
	assert.Equal(t, defaultRef.DebounceEnabled, resolvedUnknown.DebounceEnabled)
	assert.Equal(t, defaultRef.DebounceInactivitySeconds, resolvedUnknown.DebounceInactivitySeconds)
	assert.Equal(t, defaultRef.MaxWaitSeconds, resolvedUnknown.MaxWaitSeconds)
	assert.Equal(t, defaultRef.AllowAIProcessing, resolvedUnknown.AllowAIProcessing)

	// 6. Teste de Cache Local: Consulta repetida deve responder rápido da memória
	start := time.Now()
	_, err = repo.ResolvePolicy(ctx, tenantA, "whatsapp")
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 5*time.Millisecond, "cached policy lookup must be under 5ms")
}

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
)

type cachedPolicy struct {
	policy    domain.ConversationPolicy
	expiresAt time.Time
}

// PolicyRepository gerencia a persistência e resolução de políticas multi-tenant com cache em memória (INV-04, INV-08, INV-09).
type PolicyRepository struct {
	pool     *pgxpool.Pool
	mu       sync.RWMutex
	cache    map[string]cachedPolicy
	cacheTTL time.Duration
}

func NewPolicyRepository(pool *pgxpool.Pool, cacheTTL time.Duration) *PolicyRepository {
	if cacheTTL <= 0 {
		cacheTTL = 30 * time.Second
	}
	return &PolicyRepository{
		pool:     pool,
		cache:    make(map[string]cachedPolicy),
		cacheTTL: cacheTTL,
	}
}

// UpsertTenantPolicy insere ou atualiza a política customizada de um tenant.
func (r *PolicyRepository) UpsertTenantPolicy(ctx context.Context, p domain.ConversationPolicy) error {
	if p.TenantID == "" {
		return errors.New("tenantID cannot be empty")
	}

	now := time.Now().UTC()
	inactivitySecs := int(p.InactivityTimeout.Seconds())
	if inactivitySecs <= 0 {
		inactivitySecs = 86400
	}

	query := `
	INSERT INTO tenant_policies (
		tenant_id, debounce_enabled, debounce_inactivity_seconds, max_wait_seconds, allow_ai_processing, inactivity_timeout_seconds, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $7
	)
	ON CONFLICT (tenant_id)
	DO UPDATE SET
		debounce_enabled = EXCLUDED.debounce_enabled,
		debounce_inactivity_seconds = EXCLUDED.debounce_inactivity_seconds,
		max_wait_seconds = EXCLUDED.max_wait_seconds,
		allow_ai_processing = EXCLUDED.allow_ai_processing,
		inactivity_timeout_seconds = EXCLUDED.inactivity_timeout_seconds,
		updated_at = EXCLUDED.updated_at;
	`

	_, err := r.pool.Exec(ctx, query,
		p.TenantID,
		p.DebounceEnabled,
		p.DebounceInactivitySeconds,
		p.MaxWaitSeconds,
		p.AllowAIProcessing,
		inactivitySecs,
		now,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert tenant policy: %w", err)
	}

	// Invalida cache local
	r.mu.Lock()
	delete(r.cache, p.TenantID)
	r.mu.Unlock()

	return nil
}

// ResolvePolicy resolve a política ativa para o tenant com:
// 1. Consulta ao cache local em memória (RWMutex, <1ms).
// 2. Leitura no PostgreSQL se ausente/expirado.
// 3. Fallback silencioso para DefaultPolicy caso não haja customização (INV-09).
func (r *PolicyRepository) ResolvePolicy(ctx context.Context, tenantID, channel string) (*domain.ConversationPolicy, error) {
	if tenantID == "" {
		p := domain.DefaultPolicy("default")
		return &p, nil
	}

	now := time.Now()

	// 1. Checa cache
	r.mu.RLock()
	cached, found := r.cache[tenantID]
	r.mu.RUnlock()

	if found && now.Before(cached.expiresAt) {
		cp := cached.policy
		cp.Channel = channel
		return &cp, nil
	}

	// 2. Consulta banco de dados
	query := `
	SELECT debounce_enabled, debounce_inactivity_seconds, max_wait_seconds, allow_ai_processing, inactivity_timeout_seconds
	FROM tenant_policies
	WHERE tenant_id = $1;
	`

	var debounceEnabled, allowAI bool
	var debounceSecs, maxWaitSecs, inactSecs int

	err := r.pool.QueryRow(ctx, query, tenantID).Scan(
		&debounceEnabled,
		&debounceSecs,
		&maxWaitSecs,
		&allowAI,
		&inactSecs,
	)

	var policy domain.ConversationPolicy
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Fallback seguro de fábrica
			policy = domain.DefaultPolicy(tenantID)
		} else {
			return nil, fmt.Errorf("failed to query tenant policy: %w", err)
		}
	} else {
		policy = domain.ConversationPolicy{
			TenantID:                  tenantID,
			DebounceEnabled:           debounceEnabled,
			DebounceInactivitySeconds: debounceSecs,
			MaxWaitSeconds:            maxWaitSecs,
			AllowAIProcessing:         allowAI,
			InactivityTimeout:         time.Duration(inactSecs) * time.Second,
		}
	}

	// Salva no cache em memória
	r.mu.Lock()
	r.cache[tenantID] = cachedPolicy{
		policy:    policy,
		expiresAt: now.Add(r.cacheTTL),
	}
	r.mu.Unlock()

	res := policy
	res.Channel = channel
	return &res, nil
}

// CanProcessWithAI valida se a política e o estado da conversa permitem atendimento por IA.
func (r *PolicyRepository) CanProcessWithAI(ctx context.Context, conv *domain.Conversation) (bool, error) {
	if conv == nil {
		return false, nil
	}
	if !conv.AllowsAIProcessing() {
		return false, nil
	}
	policy, err := r.ResolvePolicy(ctx, conv.TenantID, conv.Channel)
	if err != nil {
		return false, err
	}
	return policy.AllowAIProcessing, nil
}

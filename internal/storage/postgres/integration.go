package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TenantIntegration struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	Channel        string    `json:"channel"`
	WebhookToken   string    `json:"webhook_token"`
	APIURL         string    `json:"api_url,omitempty"`
	APIAccessToken string    `json:"api_access_token,omitempty"`
	IsActive       bool      `json:"is_active"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type IntegrationRepository struct {
	pool *pgxpool.Pool
}

func NewIntegrationRepository(pool *pgxpool.Pool) *IntegrationRepository {
	return &IntegrationRepository{pool: pool}
}

// UpsertIntegration cria ou atualiza as credenciais de integração do tenant.
func (r *IntegrationRepository) UpsertIntegration(ctx context.Context, ti TenantIntegration) error {
	if ti.ID == "" {
		ti.ID = uuid.New().String()
	}
	now := time.Now().UTC()

	query := `
	INSERT INTO tenant_integrations (id, tenant_id, channel, webhook_token, api_url, api_access_token, is_active, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
	ON CONFLICT (tenant_id, channel) DO UPDATE
	SET webhook_token = EXCLUDED.webhook_token,
	    api_url = EXCLUDED.api_url,
	    api_access_token = EXCLUDED.api_access_token,
	    is_active = EXCLUDED.is_active,
	    updated_at = EXCLUDED.updated_at;
	`
	_, err := r.pool.Exec(ctx, query, ti.ID, ti.TenantID, ti.Channel, ti.WebhookToken, ti.APIURL, ti.APIAccessToken, ti.IsActive, now)
	return err
}

// GetIntegration busca a credencial ativa do tenant para o canal.
func (r *IntegrationRepository) GetIntegration(ctx context.Context, tenantID, channel string) (*TenantIntegration, error) {
	query := `
	SELECT id, tenant_id, channel, webhook_token, api_url, api_access_token, is_active, created_at, updated_at
	FROM tenant_integrations
	WHERE tenant_id = $1 AND channel = $2 AND is_active = true
	`
	var ti TenantIntegration
	err := r.pool.QueryRow(ctx, query, tenantID, channel).Scan(
		&ti.ID,
		&ti.TenantID,
		&ti.Channel,
		&ti.WebhookToken,
		&ti.APIURL,
		&ti.APIAccessToken,
		&ti.IsActive,
		&ti.CreatedAt,
		&ti.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &ti, nil
}

// ValidateWebhookToken valida se o token informado bate com o cadastrado para o tenant e canal.
func (r *IntegrationRepository) ValidateWebhookToken(ctx context.Context, tenantID, channel, token string) (bool, error) {
	ti, err := r.GetIntegration(ctx, tenantID, channel)
	if err != nil {
		return false, err
	}
	if ti == nil {
		return false, nil
	}
	return ti.WebhookToken == token, nil
}

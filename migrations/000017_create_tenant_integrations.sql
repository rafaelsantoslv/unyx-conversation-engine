-- Cria tabela para credenciais de integração por canal e tenant (INV-01, INV-10)
CREATE TABLE IF NOT EXISTS tenant_integrations (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32) NOT NULL, -- 'chatwoot', 'whatsapp', etc.
    webhook_token VARCHAR(255) NOT NULL,
    api_url VARCHAR(512),
    api_access_token VARCHAR(512),
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_channel UNIQUE (tenant_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_tenant_integrations_lookup ON tenant_integrations(tenant_id, channel, is_active);

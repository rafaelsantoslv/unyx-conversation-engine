-- Migration: 000016_create_tenant_policies.sql
-- Parametrização e Configuração de Políticas por Tenant (INV-04, INV-08, INV-09, UDS-203)

CREATE TABLE IF NOT EXISTS tenant_policies (
    tenant_id VARCHAR(255) PRIMARY KEY,
    debounce_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    debounce_inactivity_seconds INT NOT NULL DEFAULT 3,
    max_wait_seconds INT NOT NULL DEFAULT 15,
    allow_ai_processing BOOLEAN NOT NULL DEFAULT TRUE,
    inactivity_timeout_seconds INT NOT NULL DEFAULT 86400,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Migration: 000009_create_quarantine_events.sql
-- Tabela dedicada de Quarentena / Dead Letter para isolamento de Poison Pills e falhas permanentes (INV-06, INV-08)

CREATE TABLE IF NOT EXISTS quarantine_events (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    error_reason TEXT NOT NULL,
    stack_trace TEXT,
    attempt_count INT NOT NULL DEFAULT 1,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_quarantine_events_tenant_conv 
    ON quarantine_events (tenant_id, conversation_id);

CREATE INDEX IF NOT EXISTS idx_quarantine_events_unresolved 
    ON quarantine_events (quarantined_at DESC) 
    WHERE resolved_at IS NULL;

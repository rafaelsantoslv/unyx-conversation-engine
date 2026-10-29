-- Migration: 000014_create_conversation_state_history.sql
-- Histórico e Auditoria de Transições de Estado da FSM (INV-06, INV-09, UDS-173)

CREATE TABLE IF NOT EXISTS conversation_state_history (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    from_state VARCHAR(50) NOT NULL,
    to_state VARCHAR(50) NOT NULL,
    reason TEXT NOT NULL,
    changed_by VARCHAR(255) NOT NULL DEFAULT 'system',
    from_version BIGINT NOT NULL,
    to_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conv_state_history_tenant_conv 
ON conversation_state_history (tenant_id, conversation_id, created_at DESC);

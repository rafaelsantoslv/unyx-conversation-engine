-- Migration: 000011_create_conversation_debounce_windows.sql
-- Tabela de Janelas Duráveis de Debounce para Agrupamento de Mensagens em Rajada (INV-01, INV-02, INV-10)

CREATE TABLE IF NOT EXISTS conversation_debounce_windows (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    window_started_at TIMESTAMPTZ NOT NULL,
    debounce_until TIMESTAMPTZ NOT NULL,
    max_wait_until TIMESTAMPTZ NOT NULL,
    message_count INT NOT NULL DEFAULT 1,
    status VARCHAR(50) NOT NULL DEFAULT 'OPEN', -- OPEN, READY, PROCESSED
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_active_debounce_window UNIQUE (conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_debounce_windows_status_until 
ON conversation_debounce_windows (status, debounce_until) 
WHERE status = 'OPEN';

CREATE INDEX IF NOT EXISTS idx_debounce_windows_tenant_conv 
ON conversation_debounce_windows (tenant_id, conversation_id);

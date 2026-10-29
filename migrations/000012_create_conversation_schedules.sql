-- Migration: 000012_create_conversation_schedules.sql
-- Tabela de Agendamentos Duráveis para Execução de Conversas (INV-03, INV-05, INV-08)

CREATE TABLE IF NOT EXISTS conversation_schedules (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- PENDING, CLAIMED, EXECUTED, CANCELLED
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_active_conversation_schedule UNIQUE (conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_conversation_schedules_pending 
ON conversation_schedules (status, scheduled_for ASC) 
WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_conversation_schedules_tenant_conv 
ON conversation_schedules (tenant_id, conversation_id);

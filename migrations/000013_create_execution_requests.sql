-- Migration: 000013_create_execution_requests.sql
-- Snapshot Lógico e Execution Request Imutável para o Agent Runtime (INV-01, INV-04, UDS-173)

CREATE TABLE IF NOT EXISTS execution_requests (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    generation_id BIGINT NOT NULL DEFAULT 1,
    watermark_sequence BIGINT NOT NULL,
    message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, COMPLETED, FAILED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_requests_tenant_conv 
ON execution_requests (tenant_id, conversation_id);

CREATE INDEX IF NOT EXISTS idx_execution_requests_status 
ON execution_requests (status, created_at ASC);

-- Cria tabela de intenções de envio duráveis (INV-04, INV-06, INV-07)
CREATE TABLE IF NOT EXISTS outbound_intents (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    execution_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING, SENDING, SENT, FAILED, CANCELLED, UNKNOWN
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMPTZ,
    external_message_id VARCHAR(128),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_outbound_execution UNIQUE (execution_id)
);

CREATE INDEX IF NOT EXISTS idx_outbound_intents_pending 
ON outbound_intents(status, next_retry_at, created_at) 
WHERE status IN ('PENDING', 'FAILED');

CREATE INDEX IF NOT EXISTS idx_outbound_intents_conv 
ON outbound_intents(tenant_id, conversation_id, created_at);

-- Create conversation_executions table for Distributed Execution Coordination (INV-02, INV-08)
CREATE TABLE IF NOT EXISTS conversation_executions (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    worker_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, SUCCEEDED, FAILED
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_active_conversation_execution UNIQUE (conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_conv_executions_tenant_conv ON conversation_executions (tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_conv_executions_status ON conversation_executions (status, expires_at);

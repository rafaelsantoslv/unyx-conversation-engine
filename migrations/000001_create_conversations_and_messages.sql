-- Create conversations table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    channel VARCHAR(100) NOT NULL DEFAULT 'default',
    status VARCHAR(50) NOT NULL DEFAULT 'AI_ACTIVE',
    version BIGINT NOT NULL DEFAULT 1,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_conversations_tenant_external UNIQUE (tenant_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_conversations_tenant_id ON conversations (tenant_id);

-- Create messages table
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    external_id VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_messages_tenant_external UNIQUE (tenant_id, external_id),
    CONSTRAINT uq_messages_conversation_seq UNIQUE (conversation_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_conversation ON messages (tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_seq ON messages (conversation_id, sequence_number);

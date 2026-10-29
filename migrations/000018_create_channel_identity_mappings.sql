-- Cria tabela de mapeamento de identidades de conversas e mensagens por canal (INV-01, INV-09)
CREATE TABLE IF NOT EXISTS conversation_channel_mappings (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    provider VARCHAR(32) NOT NULL, -- ex: 'chatwoot'
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    external_account_id VARCHAR(128) NOT NULL,
    external_inbox_id VARCHAR(128) NOT NULL,
    external_conversation_id VARCHAR(128) NOT NULL,
    external_contact_id VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_provider_ext_conv UNIQUE (tenant_id, provider, external_conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_conv_channel_mappings_lookup 
ON conversation_channel_mappings(tenant_id, provider, external_conversation_id);

CREATE INDEX IF NOT EXISTS idx_conv_channel_mappings_internal 
ON conversation_channel_mappings(conversation_id);

CREATE TABLE IF NOT EXISTS message_channel_mappings (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    message_id UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    external_message_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_provider_ext_msg UNIQUE (tenant_id, provider, external_message_id)
);

CREATE INDEX IF NOT EXISTS idx_msg_channel_mappings_lookup 
ON message_channel_mappings(tenant_id, provider, external_message_id);

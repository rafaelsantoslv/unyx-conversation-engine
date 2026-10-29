package dto_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/domain"
	cwDto "github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/chatwoot/dto"
	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/integration/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChatwootWebhookMapping_ACL(t *testing.T) {
	incomingJSON := `{
		"event": "message_created",
		"id": 12345,
		"content": "Olá, gostaria de saber preços!",
		"created_at": "2026-10-29T12:00:00Z",
		"message_type": "incoming",
		"content_type": "text",
		"private": false,
		"sender": {
			"id": 999,
			"name": "Maria Silva",
			"type": "contact"
		},
		"contact": {
			"id": 999,
			"name": "Maria Silva",
			"phone_number": "+5511999998888"
		},
		"conversation": {
			"id": 555,
			"inbox_id": 10,
			"status": "open"
		},
		"account": {
			"id": 1,
			"name": "Empresa Alpha"
		},
		"inbox": {
			"id": 10,
			"name": "WhatsApp Comercial",
			"channel_type": "Channel::Whatsapp"
		}
	}`

	var cwPayload cwDto.ChatwootWebhookPayload
	err := json.Unmarshal([]byte(incomingJSON), &cwPayload)
	require.NoError(t, err)

	tenantID := "tenant-alpha"
	event, err := cwPayload.ToCanonicalProviderEvent(tenantID)
	require.NoError(t, err)

	assert.Equal(t, ports.ProviderEventMessageCreated, event.Type)
	assert.Equal(t, tenantID, event.TenantID)
	assert.Equal(t, ports.ChannelTypeWhatsApp, event.ChannelIdentity.Channel)
	assert.Equal(t, "1", event.ChannelIdentity.ExternalAccountID)
	assert.Equal(t, "10", event.ChannelIdentity.ExternalInboxID)
	assert.Equal(t, "555", event.ChannelIdentity.ExternalConversationID)
	assert.Equal(t, "999", event.ChannelIdentity.ExternalContactID)

	require.NotNil(t, event.IncomingMessage)
	assert.Equal(t, "12345", event.IncomingMessage.ExternalMessageID)
	assert.Equal(t, "555", event.IncomingMessage.ExternalConversationID)
	assert.Equal(t, "Olá, gostaria de saber preços!", event.IncomingMessage.Content)
	assert.Equal(t, domain.RoleUser, event.IncomingMessage.Role)
	assert.False(t, event.IncomingMessage.IsEcho, "User message must NOT be an echo")
}

func TestChatwootWebhookMapping_EchoDetection(t *testing.T) {
	echoJSON := `{
		"event": "message_created",
		"id": 67890,
		"content": "Olá Maria, temos planos a partir de R$ 99.",
		"message_type": "outgoing",
		"sender": {
			"id": 2,
			"name": "Bot Atendente",
			"type": "agent_bot"
		},
		"conversation": {
			"id": 555,
			"inbox_id": 10,
			"status": "open"
		}
	}`

	var cwPayload cwDto.ChatwootWebhookPayload
	err := json.Unmarshal([]byte(echoJSON), &cwPayload)
	require.NoError(t, err)

	event, err := cwPayload.ToCanonicalProviderEvent("tenant-alpha")
	require.NoError(t, err)

	require.NotNil(t, event.IncomingMessage)
	assert.True(t, event.IncomingMessage.IsEcho, "Outgoing message from bot must be flagged as IsEcho=true")
	assert.Equal(t, domain.RoleAgent, event.IncomingMessage.Role)
}

func TestCleanArchitecture_DomainHasNoChatwootImports(t *testing.T) {
	// Garante que internal/domain NUNCA importa pacotes de integração ou chatwoot (INV-09)
	domainDir := "../../../domain"
	files, err := os.ReadDir(domainDir)
	require.NoError(t, err)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".go") {
			content, err := os.ReadFile(filepath.Join(domainDir, file.Name()))
			require.NoError(t, err)
			contentStr := string(content)
			assert.NotContains(t, contentStr, "chatwoot", "domain file %s must NOT import or reference chatwoot (INV-09)", file.Name())
			assert.NotContains(t, contentStr, "integration", "domain file %s must NOT import integration layer", file.Name())
		}
	}
}

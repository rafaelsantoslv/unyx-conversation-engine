package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructuredLogger_ContextualAttributesAndRedaction(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewJSONLogger(&buf, slog.LevelInfo)

	ctx := context.Background()
	ctx = telemetry.WithCorrelationID(ctx, "corr-uuid-1234")
	ctx = telemetry.WithTenantAndConversation(ctx, "tenant-alpha", "conv-uuid-5678")

	// Emite log com contexto e atributos adicionais incluindo token sensível
	logger.InfoContext(ctx, "inbound webhook received",
		slog.String("channel", "whatsapp"),
		slog.String("token", "super-secret-token"),
		slog.Int("messages_count", 3),
	)

	logOutput := buf.String()
	require.NotEmpty(t, logOutput)

	var logMap map[string]interface{}
	err := json.Unmarshal([]byte(logOutput), &logMap)
	require.NoError(t, err, "Log must be valid JSON")

	// 1. Campos padrão do slog
	assert.Equal(t, "INFO", logMap["level"])
	assert.Equal(t, "inbound webhook received", logMap["msg"])

	// 2. Injeção de contexto obrigatória (INV-01)
	assert.Equal(t, "corr-uuid-1234", logMap["correlation_id"])
	assert.Equal(t, "tenant-alpha", logMap["tenant_id"])
	assert.Equal(t, "conv-uuid-5678", logMap["conversation_id"])

	// 3. Atributos da mensagem
	assert.Equal(t, "whatsapp", logMap["channel"])
	assert.Equal(t, float64(3), logMap["messages_count"])

	// 4. Redação de credenciais sensíveis (INV-02)
	assert.Equal(t, "[REDACTED]", logMap["token"], "Token must be redacted in structured logs")
}

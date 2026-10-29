package telemetry_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestTelemetry_InitializationAndShutdown(t *testing.T) {
	ctx := context.Background()

	tp, shutdown, err := telemetry.InitTelemetry(ctx, telemetry.Config{
		ServiceName:    telemetry.ServiceAPI,
		ServiceVersion: "1.0.0-test",
		Environment:    "test",
	})
	require.NoError(t, err)
	require.NotNil(t, tp)
	assert.Equal(t, telemetry.ServiceAPI, tp.ServiceName)
	assert.NotNil(t, tp.Tracer)
	assert.NotNil(t, tp.Meter)

	// Criação de span com atributos semânticos padronizados
	trCtx, span := tp.Tracer.Start(ctx, "test_operation")
	span.SetAttributes(
		telemetry.AttrTenantID.String("tenant-123"),
		telemetry.AttrConversationID.String("conv-456"),
		telemetry.AttrSequenceNumber.Int64(1),
	)
	span.End()

	// Shutdown gracioso
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = shutdown(shutdownCtx)
	assert.NoError(t, err)
	_ = trCtx
}

func TestTelemetry_W3CTraceContextPropagation(t *testing.T) {
	ctx := context.Background()

	tp, shutdown, err := telemetry.InitTelemetry(ctx, telemetry.Config{
		ServiceName: telemetry.ServiceWorker,
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	// 1. Inicia span e injeta contexto W3C em headers HTTP
	spanCtx, span := tp.Tracer.Start(ctx, "inbound_http_request")
	defer span.End()

	req, err := http.NewRequestWithContext(spanCtx, http.MethodPost, "http://localhost:8080/messages", nil)
	require.NoError(t, err)

	propagator := otel.GetTextMapPropagator()
	propagator.Inject(spanCtx, propagation.HeaderCarrier(req.Header))

	// Valida que o header traceparent padronizado W3C foi gerado
	traceParent := req.Header.Get("traceparent")
	assert.NotEmpty(t, traceParent, "W3C traceparent header must be injected")

	// 2. Extrai contexto na ponta recebedora
	extractedCtx := propagator.Extract(context.Background(), propagation.HeaderCarrier(req.Header))
	require.NotNil(t, extractedCtx)

	// Inicia span filho e valida continuidade de contexto
	_, childSpan := tp.Tracer.Start(extractedCtx, "worker_process_message")
	defer childSpan.End()

	assert.Equal(t, span.SpanContext().TraceID(), childSpan.SpanContext().TraceID(),
		"Child span must share identical TraceID across network boundary (INV-01)")
}

func TestTelemetry_RedactSensitiveContent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Empty text",
			input:    "",
			expected: "",
		},
		{
			name:     "Normal short text",
			input:    "Bom dia! Como posso ajudar?",
			expected: "Bom dia! Como posso ajudar?",
		},
		{
			name:     "Bearer token leak protection",
			input:    "Authorization: Bearer my-secret-jwt-token-12345",
			expected: "[REDACTED_CREDENTIALS]",
		},
		{
			name:     "Chatwoot API Access Token leak protection",
			input:    `{"api_access_token": "super_secret_token"}`,
			expected: "[REDACTED_CREDENTIALS]",
		},
		{
			name:     "Generic secret leak protection",
			input:    "client_secret=987654321",
			expected: "[REDACTED_CREDENTIALS]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := telemetry.RedactSensitiveContent(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

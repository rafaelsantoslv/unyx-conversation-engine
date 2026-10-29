package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

type contextKey string

const (
	CorrelationIDContextKey contextKey = "telemetry.correlation_id"
	TenantIDContextKey      contextKey = "telemetry.tenant_id"
	ConversationIDContextKey contextKey = "telemetry.conversation_id"
)

// ContextHandler intercepta chamadas do slog para injetar automaticamente atributos do contexto
// como correlation_id, trace_id, span_id, tenant_id e conversation_id (INV-01, INV-05).
type ContextHandler struct {
	slog.Handler
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		// 1. Injeta correlation_id do contexto
		if corrID, ok := ctx.Value(CorrelationIDContextKey).(string); ok && corrID != "" {
			r.AddAttrs(slog.String("correlation_id", corrID))
		}

		// 2. Injeta tenant_id do contexto
		if tenantID, ok := ctx.Value(TenantIDContextKey).(string); ok && tenantID != "" {
			r.AddAttrs(slog.String("tenant_id", tenantID))
		}

		// 3. Injeta conversation_id do contexto
		if convID, ok := ctx.Value(ConversationIDContextKey).(string); ok && convID != "" {
			r.AddAttrs(slog.String("conversation_id", convID))
		}

		// 4. Injeta OpenTelemetry trace_id e span_id se houver span ativo
		span := trace.SpanFromContext(ctx)
		if span != nil && span.SpanContext().IsValid() {
			r.AddAttrs(
				slog.String("trace_id", span.SpanContext().TraceID().String()),
				slog.String("span_id", span.SpanContext().SpanID().String()),
			)
		}
	}

	return h.Handler.Handle(ctx, r)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithGroup(name)}
}

// NewJSONLogger cria uma instância do slog configurada com JSON Handler e injeção contextual.
func NewJSONLogger(w io.Writer, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	baseHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Redige senhas, tokens e credenciais conhecidas (INV-02)
			if a.Key == "token" || a.Key == "secret" || a.Key == "api_access_token" || a.Key == "webhook_token" {
				return slog.String(a.Key, "[REDACTED]")
			}
			return a
		},
	})
	return slog.New(&ContextHandler{Handler: baseHandler})
}

// SetGlobalLogger define o logger estruturado padrão para o runtime do Go.
func SetGlobalLogger(logger *slog.Logger) {
	if logger != nil {
		slog.SetDefault(logger)
	}
}

// WithCorrelationID associa um correlation_id ao context.Context.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, CorrelationIDContextKey, correlationID)
}

// WithTenantAndConversation associa tenant_id e conversation_id ao context.Context.
func WithTenantAndConversation(ctx context.Context, tenantID, conversationID string) context.Context {
	ctx = context.WithValue(ctx, TenantIDContextKey, tenantID)
	return context.WithValue(ctx, ConversationIDContextKey, conversationID)
}

// CorrelationIDFromContext recupera o correlation_id do contexto se presente.
func CorrelationIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(CorrelationIDContextKey).(string); ok {
		return v
	}
	return ""
}

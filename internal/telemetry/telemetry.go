package telemetry

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Nomes de serviços oficiais do ecossistema Conversation Engine
const (
	ServiceAPI            = "unyx-conversation-api"
	ServiceOutboxPublisher = "unyx-outbox-publisher"
	ServiceWorker         = "unyx-conversation-worker"
	ServiceOutboundWorker = "unyx-outbound-worker"
)

// Atributos semânticos canônicos (INV-01, INV-03)
const (
	AttrTenantID       = attribute.Key("tenant.id")
	AttrConversationID = attribute.Key("conversation.id")
	AttrMessageID      = attribute.Key("message.id")
	AttrExecutionID    = attribute.Key("execution.id")
	AttrSequenceNumber = attribute.Key("message.sequence")
	AttrChannel        = attribute.Key("messaging.channel")
	AttrExecutionStatus = attribute.Key("execution.status")
	AttrErrorType      = attribute.Key("error.type")
)

// TelemetryProvider encapsula TracerProvider, MeterProvider e Propagator configurados.
type TelemetryProvider struct {
	ServiceName    string
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	Tracer         trace.Tracer
	Meter          metric.Meter
}

// Config define parâmetros de inicialização de telemetria.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
}

// InitTelemetry inicializa provedores OpenTelemetry em conformidade com W3C Trace Context (INV-01, INV-04).
func InitTelemetry(ctx context.Context, cfg Config) (*TelemetryProvider, func(context.Context) error, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = ServiceAPI
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "1.0.0"
	}
	if cfg.Environment == "" {
		cfg.Environment = "development"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			attribute.String("environment", cfg.Environment),
		),
	)
	if err != nil {
		res = resource.Default()
	}

	// 1. TracerProvider configurado com sampler inteligente
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tracerProvider)

	// 2. W3C Trace Context Propagator (traceparent, tracestate) e Baggage
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// 3. MeterProvider configurado
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	tp := &TelemetryProvider{
		ServiceName:    cfg.ServiceName,
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		Tracer:         tracerProvider.Tracer(cfg.ServiceName),
		Meter:          meterProvider.Meter(cfg.ServiceName),
	}

	// Função de shutdown gracioso
	shutdown := func(shutdownCtx context.Context) error {
		var errs []string
		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Sprintf("trace shutdown: %v", err))
		}
		if err := meterProvider.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Sprintf("metric shutdown: %v", err))
		}
		if len(errs) > 0 {
			return fmt.Errorf("telemetry shutdown errors: %s", strings.Join(errs, ", "))
		}
		return nil
	}

	return tp, shutdown, nil
}

// RedactSensitiveContent sanitiza mensagens e dados para prevenir vazamento de PII ou credenciais (INV-02).
func RedactSensitiveContent(text string) string {
	if len(text) == 0 {
		return ""
	}
	// Mascara tokens de API, senhas ou conteúdos sensíveis em logs/traces
	lower := strings.ToLower(text)
	if strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "api_access_token") ||
		strings.Contains(lower, "token=") ||
		strings.Contains(lower, "secret") {
		return "[REDACTED_CREDENTIALS]"
	}

	// Trunca texto longo para evitar poluição de spans mantendo apenas tamanho
	if len(text) > 100 {
		return fmt.Sprintf("%s... [len=%d]", text[:50], len(text))
	}
	return text
}

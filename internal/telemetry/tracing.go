package telemetry

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)


// KafkaHeaderCarrier adapta os headers do segmentio/kafka-go para a interface TextMapCarrier do OpenTelemetry.
type KafkaHeaderCarrier []kafka.Header

func (c *KafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *KafkaHeaderCarrier) Set(key string, value string) {
	// Substitui se já existir
	for i, h := range *c {
		if h.Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (c *KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c))
	for _, h := range *c {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectKafkaHeaders injeta o contexto W3C Traceparent nos headers de uma mensagem do Kafka (INV-01).
func InjectKafkaHeaders(ctx context.Context, headers *[]kafka.Header) {
	if headers == nil {
		return
	}
	carrier := (*KafkaHeaderCarrier)(headers)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractKafkaHeaders extrai o contexto W3C Traceparent dos headers de uma mensagem do Kafka.
// Se nenhum traceparent for encontrado, retorna o contexto base para permitir início de novo root trace.
func ExtractKafkaHeaders(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := (*KafkaHeaderCarrier)(&headers)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// StartSpan inicia um novo span semântico OpenTelemetry registrando o nome da operação.
func StartSpan(ctx context.Context, tracerName, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := otel.GetTracerProvider().Tracer(tracerName)
	return tracer.Start(ctx, spanName, opts...)
}

// LinkSpans cria um SpanStartOption contendo links para spans precedentes (ex: no Debounce batching de mensagens).
func LinkSpans(spanContexts ...trace.SpanContext) trace.SpanStartOption {
	links := make([]trace.Link, 0, len(spanContexts))
	for _, sc := range spanContexts {
		if sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	return trace.WithLinks(links...)
}

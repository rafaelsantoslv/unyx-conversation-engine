package telemetry_test

import (
	"context"
	"testing"

	"github.com/rafaelsantoslv/unyx-conversation-engine/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func TestTracing_KafkaHeaderCarrierAndPropagation(t *testing.T) {
	ctx := context.Background()

	tp, shutdown, err := telemetry.InitTelemetry(ctx, telemetry.Config{
		ServiceName: telemetry.ServiceOutboxPublisher,
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	// 1. Inicia span no Publisher
	pubCtx, pubSpan := tp.Tracer.Start(ctx, "kafka.publish_message")
	defer pubSpan.End()

	// 2. Injeta nos headers do Kafka
	headers := make([]kafka.Header, 0)
	telemetry.InjectKafkaHeaders(pubCtx, &headers)

	require.NotEmpty(t, headers)
	var foundTraceparent bool
	for _, h := range headers {
		if h.Key == "traceparent" {
			foundTraceparent = true
			assert.NotEmpty(t, string(h.Value))
		}
	}
	assert.True(t, foundTraceparent, "traceparent header must be injected into Kafka message headers")

	// 3. Extrai no Worker e inicia child span
	workerBaseCtx := context.Background()
	workerCtx := telemetry.ExtractKafkaHeaders(workerBaseCtx, headers)

	workerChildCtx, workerSpan := tp.Tracer.Start(workerCtx, "kafka.consume_message")
	defer workerSpan.End()

	// Valida que o TraceID propagado pelo Kafka é exatamente idêntico ao original (INV-01)
	assert.Equal(t, pubSpan.SpanContext().TraceID(), workerSpan.SpanContext().TraceID(),
		"TraceID must be perfectly preserved across Kafka boundaries")
	_ = workerChildCtx
}

func TestTracing_SpanLinksForBatching(t *testing.T) {
	ctx := context.Background()

	tp, shutdown, err := telemetry.InitTelemetry(ctx, telemetry.Config{
		ServiceName: telemetry.ServiceWorker,
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	// Simula 3 mensagens individuais com seus próprios spans
	_, span1 := tp.Tracer.Start(ctx, "msg_1")
	defer span1.End()
	_, span2 := tp.Tracer.Start(ctx, "msg_2")
	defer span2.End()
	_, span3 := tp.Tracer.Start(ctx, "msg_3")
	defer span3.End()

	// Inicia span de execução do lote vinculado aos spans das mensagens originais via Span Links
	linkOption := telemetry.LinkSpans(
		span1.SpanContext(),
		span2.SpanContext(),
		span3.SpanContext(),
	)

	_, batchSpan := tp.Tracer.Start(ctx, "execution_request.batch", linkOption)
	defer batchSpan.End()

	assert.True(t, batchSpan.SpanContext().IsValid())
}

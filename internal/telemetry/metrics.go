package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricRegistry holds all Prometheus metrics for the Conversation Engine.
// It enforces low cardinality label usage to prevent memory leaks (INV-03).
// Specifically, IDs (message_id, conversation_id, customer_phone, etc.) are strictly prohibited as labels.
type Metrics struct {
	Registry *prometheus.Registry

	// Ingestion metrics
	MessagesReceivedTotal *prometheus.CounterVec
	MessagesRejectedTotal *prometheus.CounterVec
	IngestDurationSeconds *prometheus.HistogramVec

	// Outbox metrics
	OutboxEventsPendingCount *prometheus.GaugeVec
	OutboxEventsPublishedTotal *prometheus.CounterVec
	OutboxPublishLatencySeconds *prometheus.HistogramVec

	// Kafka metrics
	KafkaConsumerLagRecords *prometheus.GaugeVec
	KafkaMessagesConsumedTotal *prometheus.CounterVec

	// Worker & Execution metrics
	ExecutionsTotal *prometheus.CounterVec
	ExecutionDurationSeconds *prometheus.HistogramVec
	ActiveWorkersCount *prometheus.GaugeVec

	// Scheduler metrics
	SchedulesPendingCount *prometheus.GaugeVec
	ScheduleDelaySeconds *prometheus.HistogramVec

	// Outbound delivery metrics
	OutboundIntentsTotal *prometheus.CounterVec
	OutboundDeliveryDurationSeconds *prometheus.HistogramVec

	// Reliability metrics
	RetriesTotal *prometheus.CounterVec
	QuarantineEventsTotal *prometheus.CounterVec
	ZombieFencingRejectionsTotal *prometheus.CounterVec
}

var (
	// DefaultMetrics is the singleton metrics instance using custom registry to avoid contamination.
	defaultMetrics *Metrics
)

// InitMetrics creates and registers all standard Prometheus metrics in a new or provided registry.
func InitMetrics(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &Metrics{
		Registry: reg,

		// Ingestion
		MessagesReceivedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "conversation_messages_received_total",
				Help: "Total count of inbound messages received by the ingestion layer.",
			},
			[]string{"tenant_id", "channel"},
		),
		MessagesRejectedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "conversation_messages_rejected_total",
				Help: "Total count of inbound messages rejected by the ingestion layer.",
			},
			[]string{"tenant_id", "channel", "reason"},
		),
		IngestDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "conversation_ingest_duration_seconds",
				Help:    "Latency of inbound message ingestion and validation.",
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
			},
			[]string{"tenant_id", "channel"},
		),

		// Outbox
		OutboxEventsPendingCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "outbox_events_pending_count",
				Help: "Number of outbox events currently pending dispatch.",
			},
			[]string{"tenant_id", "event_type"},
		),
		OutboxEventsPublishedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbox_events_published_total",
				Help: "Total count of transactional outbox events successfully published to Kafka.",
			},
			[]string{"tenant_id", "event_type"},
		),
		OutboxPublishLatencySeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "outbox_publish_latency_seconds",
				Help:    "Latency from outbox event creation to Kafka publication.",
				Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
			},
			[]string{"tenant_id", "event_type"},
		),

		// Kafka
		KafkaConsumerLagRecords: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_lag_records",
				Help: "Estimated number of unconsumed records remaining in Kafka topic/partition.",
			},
			[]string{"topic", "consumer_group"},
		),
		KafkaMessagesConsumedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_messages_consumed_total",
				Help: "Total count of Kafka messages consumed by workers.",
			},
			[]string{"topic", "consumer_group", "status"},
		),

		// Worker
		ExecutionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "conversation_executions_total",
				Help: "Total count of AI agent executions processed.",
			},
			[]string{"tenant_id", "status"},
		),
		ExecutionDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "conversation_execution_duration_seconds",
				Help:    "Duration of AI agent runtime execution in seconds.",
				Buckets: []float64{0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 60},
			},
			[]string{"tenant_id", "status"},
		),
		ActiveWorkersCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "active_workers_count",
				Help: "Current count of active execution worker routines.",
			},
			[]string{"worker_type"},
		),

		// Scheduler
		SchedulesPendingCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "conversation_schedules_pending_count",
				Help: "Number of conversations currently in debouncing / pending schedule queue.",
			},
			[]string{"tenant_id"},
		),
		ScheduleDelaySeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "conversation_schedule_delay_seconds",
				Help:    "Time elapsed between message receipt and debounce trigger execution.",
				Buckets: []float64{0.1, 0.5, 1, 2, 3, 5, 10},
			},
			[]string{"tenant_id"},
		),

		// Outbound
		OutboundIntentsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "outbound_intents_total",
				Help: "Total count of outbound delivery intents processed by status.",
			},
			[]string{"tenant_id", "channel", "status"},
		),
		OutboundDeliveryDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "outbound_delivery_duration_seconds",
				Help:    "Latency of outbound message dispatch to external channel (e.g. Chatwoot).",
				Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
			},
			[]string{"tenant_id", "channel", "status"},
		),

		// Reliability
		RetriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "conversation_retries_total",
				Help: "Total count of retries attempted across operations.",
			},
			[]string{"tenant_id", "operation"},
		),
		QuarantineEventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "quarantine_events_total",
				Help: "Total count of poisoned or corrupted payloads placed in dead letter/quarantine.",
			},
			[]string{"tenant_id", "reason"},
		),
		ZombieFencingRejectionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zombie_fencing_rejections_total",
				Help: "Total count of stale/zombie execution completion attempts rejected by optimistic locking.",
			},
			[]string{"tenant_id", "reason"},
		),
	}

	// Register all collectors
	reg.MustRegister(
		m.MessagesReceivedTotal,
		m.MessagesRejectedTotal,
		m.IngestDurationSeconds,
		m.OutboxEventsPendingCount,
		m.OutboxEventsPublishedTotal,
		m.OutboxPublishLatencySeconds,
		m.KafkaConsumerLagRecords,
		m.KafkaMessagesConsumedTotal,
		m.ExecutionsTotal,
		m.ExecutionDurationSeconds,
		m.ActiveWorkersCount,
		m.SchedulesPendingCount,
		m.ScheduleDelaySeconds,
		m.OutboundIntentsTotal,
		m.OutboundDeliveryDurationSeconds,
		m.RetriesTotal,
		m.QuarantineEventsTotal,
		m.ZombieFencingRejectionsTotal,
	)

	defaultMetrics = m
	return m
}

// GetMetrics returns the default global metrics registry instance.
// If not initialized, it automatically initializes one with a new Prometheus registry.
func GetMetrics() *Metrics {
	if defaultMetrics == nil {
		defaultMetrics = InitMetrics(nil)
	}
	return defaultMetrics
}

// HTTPHandler returns an http.Handler that serves Prometheus metrics on /metrics.
func (m *Metrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

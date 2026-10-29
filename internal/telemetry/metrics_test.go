package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_RegistrationAndScraping(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := InitMetrics(reg)

	// Simulate 50 messages workload as required by UDS-220 criteria
	tenantID := "tenant_test_123"
	channel := "chatwoot"

	for i := 0; i < 50; i++ {
		m.MessagesReceivedTotal.WithLabelValues(tenantID, channel).Inc()
		m.IngestDurationSeconds.WithLabelValues(tenantID, channel).Observe(0.015)

		m.OutboxEventsPublishedTotal.WithLabelValues(tenantID, "conversation.message.received").Inc()
		m.OutboxPublishLatencySeconds.WithLabelValues(tenantID, "conversation.message.received").Observe(0.08)
		m.KafkaMessagesConsumedTotal.WithLabelValues("conversation-messages", "engine-workers", "success").Inc()

		if i%5 == 0 {
			m.MessagesRejectedTotal.WithLabelValues(tenantID, channel, "duplicate_event").Inc()
			m.RetriesTotal.WithLabelValues(tenantID, "outbox_dispatch").Inc()
			m.QuarantineEventsTotal.WithLabelValues(tenantID, "invalid_json").Inc()
			m.ZombieFencingRejectionsTotal.WithLabelValues(tenantID, "stale_generation").Inc()
		}
	}

	// Gauges
	m.OutboxEventsPendingCount.WithLabelValues(tenantID, "conversation.message.received").Set(12)
	m.KafkaConsumerLagRecords.WithLabelValues("conversation-messages", "engine-workers").Set(3)
	m.ActiveWorkersCount.WithLabelValues("execution_worker").Set(8)
	m.SchedulesPendingCount.WithLabelValues(tenantID).Set(2)

	// Worker & Outbound Histograms
	m.ExecutionsTotal.WithLabelValues(tenantID, "succeeded").Add(45)
	m.ExecutionDurationSeconds.WithLabelValues(tenantID, "succeeded").Observe(3.5)
	m.ScheduleDelaySeconds.WithLabelValues(tenantID).Observe(0.5)
	m.OutboundIntentsTotal.WithLabelValues(tenantID, channel, "delivered").Add(45)
	m.OutboundDeliveryDurationSeconds.WithLabelValues(tenantID, channel, "delivered").Observe(0.12)

	// Verify Scraping via HTTP Handler
	ts := httptest.NewServer(m.HTTPHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("failed to scrape metrics endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	body := string(bodyBytes)

	// Validate presence of metrics
	expectedMetrics := []string{
		"conversation_messages_received_total",
		"conversation_messages_rejected_total",
		"conversation_ingest_duration_seconds",
		"outbox_events_pending_count",
		"outbox_events_published_total",
		"outbox_publish_latency_seconds",
		"kafka_consumer_lag_records",
		"kafka_messages_consumed_total",
		"conversation_executions_total",
		"conversation_execution_duration_seconds",
		"active_workers_count",
		"conversation_schedules_pending_count",
		"conversation_schedule_delay_seconds",
		"outbound_intents_total",
		"outbound_delivery_duration_seconds",
		"conversation_retries_total",
		"quarantine_events_total",
		"zombie_fencing_rejections_total",
	}

	for _, metricName := range expectedMetrics {
		if !strings.Contains(body, metricName) {
			t.Errorf("expected scraped output to contain metric %q", metricName)
		}
	}

	// Verify exact count of messages received = 50
	expectedMsgCount := fmt.Sprintf(`conversation_messages_received_total{channel="chatwoot",tenant_id="%s"} 50`, tenantID)
	if !strings.Contains(body, expectedMsgCount) {
		t.Errorf("expected metric value not found in body: %s", expectedMsgCount)
	}

	// Cardinality check: verify no message_id or conversation_id is present as a label
	forbiddenLabels := []string{"message_id=", "conversation_id=", "customer_phone="}
	for _, forbidden := range forbiddenLabels {
		if strings.Contains(body, forbidden) {
			t.Errorf("violation of INV-03: metric label %q found in scraped output", forbidden)
		}
	}
}

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const e2eCompletedMessages = new Counter('engine_e2e_messages_completed_total');
const e2eErrorRate = new Rate('engine_e2e_errors_rate');
const webhookDuration = new Trend('engine_webhook_duration_ms');

export const options = {
  scenarios: {
    // Pipeline E2E Load: Webhooks Chatwoot chegando continuamente
    chatwoot_inbound_stream: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { duration: '1m', target: 50 },  // Ramping até 50 RPS
        { duration: '5m', target: 50 },  // Sustenta 50 RPS
        { duration: '1m', target: 100 }, // Saturação controlada em 100 RPS
        { duration: '3m', target: 100 },
        { duration: '1m', target: 0 },   // Drenagem e resfriamento
      ],
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<250', 'p(99)<600'],
    'http_req_failed': ['rate<0.01'],
  },
};

const BASE_URL = __ENV.ENGINE_API_URL || 'http://localhost:8080';
const TENANT_ID = __ENV.TENANT_ID || 'tenant_load_test';
const WEBHOOK_TOKEN = __ENV.WEBHOOK_TOKEN || 'test-webhook-token';

export default function () {
  const conversationId = Math.floor(Math.random() * 2000) + 1;
  const messageId = Math.floor(Math.random() * 10000000) + 1;
  const senderId = Math.floor(Math.random() * 5000) + 1;

  const payload = JSON.stringify({
    event: 'message_created',
    id: messageId,
    content: 'Olá! Preciso de ajuda com a confirmação da minha reserva para amanhã às 14h.',
    message_type: 'incoming',
    created_at: new Date().toISOString(),
    conversation: {
      id: conversationId,
      status: 'pending',
      inbox_id: 1,
      account_id: 1,
    },
    sender: {
      id: senderId,
      name: `Cliente Teste ${senderId}`,
      type: 'contact',
      phone_number: `+55119999${senderId.toString().padStart(4, '0')}`,
    },
    account: {
      id: 1,
      name: 'Conta Teste de Carga',
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Chatwoot-Webhook-Token': WEBHOOK_TOKEN,
      'X-Request-Id': `e2e-${Date.now()}-${__VU}-${__ITER}`,
    },
    timeout: '5s',
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/integrations/chatwoot/webhooks/${TENANT_ID}`, payload, params);
  const duration = Date.now() - start;
  webhookDuration.add(duration);

  const ok = check(res, {
    'webhook accepted (200/201/202)': (r) => r.status === 200 || r.status === 201 || r.status === 202,
    'webhook p95 latency acceptable': () => duration < 250,
  });

  if (ok) {
    e2eCompletedMessages.add(1);
    e2eErrorRate.add(0);
  } else {
    e2eErrorRate.add(1);
  }
}

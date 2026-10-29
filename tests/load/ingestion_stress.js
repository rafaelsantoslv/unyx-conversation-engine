import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const rejectedMessages = new Counter('engine_rejected_messages_total');
const errorRate = new Rate('engine_errors_rate');
const ingestLatency = new Trend('engine_ingest_latency_ms');

export const options = {
  scenarios: {
    // 1. Baseline Sustentado: 50 RPS por 10 minutos
    sustained_baseline: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '10m',
      preAllocatedVUs: 50,
      maxVUs: 200,
      startTime: '0s',
    },
    // 2. Spike Test: Salto repentino para 200 RPS por 30s
    spike_test: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 500,
      stages: [
        { duration: '30s', target: 200 }, // Ramping rápido até 200 RPS
        { duration: '1m', target: 200 },  // Sustenta 200 RPS
        { duration: '30s', target: 50 },   // Retorna à baseline
      ],
      startTime: '10m',
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<200', 'p(99)<500'], // INV-08: p95 < 200ms
    'http_req_failed': ['rate<0.001'],               // Erros HTTP < 0.1%
    'engine_errors_rate': ['rate<0.001'],
  },
};

const BASE_URL = __ENV.ENGINE_API_URL || 'http://localhost:8080';
const TENANTS = ['tenant_alpha', 'tenant_beta', 'tenant_gamma', 'tenant_delta'];

export default function () {
  const tenantId = TENANTS[Math.floor(Math.random() * TENANTS.length)];
  const customerId = `user_${Math.floor(Math.random() * 10000)}`;
  const messageId = `msg_${Date.now()}_${Math.floor(Math.random() * 1000000)}`;

  const payload = JSON.stringify({
    tenant_id: tenantId,
    customer_id: customerId,
    external_message_id: messageId,
    channel: 'whatsapp',
    content: 'Olá! Gostaria de saber mais sobre os planos e disponibilidade do serviço.',
    sent_at: new Date().toISOString(),
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Request-Id': `req-${Date.now()}-${__VU}-${__ITER}`,
    },
    timeout: '5s',
  };

  const startTime = Date.now();
  const res = http.post(`${BASE_URL}/v1/messages`, payload, params);
  const latency = Date.now() - startTime;
  ingestLatency.add(latency);

  const isSuccess = check(res, {
    'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
    'latency is below 200ms': () => latency < 200,
  });

  if (!isSuccess) {
    errorRate.add(1);
    rejectedMessages.add(1);
  } else {
    errorRate.add(0);
  }
}

// Spike profile: baseline -> 250x in 30s -> hold -> sharp drop, mirroring a
// real drop. Run with:
//   k6 run -e BASE=http://localhost:8080 -e ADMISSION_SECRET=... -e ADMIN_TOKEN=... loadtest/k6/spike.js
// Use PROFILE=soak or PROFILE=stress for the other shapes.
import http from 'k6/http';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8080';
const SECRET = __ENV.ADMISSION_SECRET;
const ADMIN = __ENV.ADMIN_TOKEN;
const SKU = __ENV.SKU || `k6-${Date.now()}`;
const STOCK = parseInt(__ENV.STOCK || '50000', 10);
const SCALE = parseFloat(__ENV.SCALE || '0.01'); // 1.0 = full production numbers

const soldOut = new Counter('sold_out');
const oversell = new Counter('oversell_detected');

const profiles = {
  // 2k rps baseline -> 500k rps within 30s, hold 10m, drop off.
  spike: [
    { target: 2000 * SCALE, duration: '1m' },
    { target: 500000 * SCALE, duration: '30s' },
    { target: 500000 * SCALE, duration: '10m' },
    { target: 2000 * SCALE, duration: '1m' },
  ],
  // Find the knee: step up until SLOs break.
  stress: [50, 100, 200, 400, 800].map((k) => ({ target: k * 1000 * SCALE, duration: '2m' })),
  // Leaks, connection churn, hold expiry correctness over hours.
  soak: [{ target: 20000 * SCALE, duration: '5m' }, { target: 20000 * SCALE, duration: '4h' }],
};

export const options = {
  scenarios: {
    drop: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, 2000 * SCALE),
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 20000,
      stages: profiles[__ENV.PROFILE || 'spike'],
    },
  },
  thresholds: {
    'http_req_duration{op:reserve}': ['p(99)<500'],
    'http_req_duration{op:pay}': ['p(99)<1500'],
    'http_req_failed{op:reserve}': ['rate<0.001'], // 409 sold_out is expected, see responseCallback
    oversell_detected: ['count==0'],
  },
};

http.setResponseCallback(http.expectedStatuses(200, 201, 202, 204, 402, 409));

function token(user) {
  const exp = Math.floor(Date.now() / 1000) + 900;
  const payload = `${user}|${SKU}|${exp}`;
  const sig = crypto.hmac('sha256', SECRET, payload, 'binary');
  return `${encoding.b64encode(payload, 'rawurl')}.${encoding.b64encode(sig, 'rawurl')}`;
}

export function setup() {
  const res = http.post(
    `${BASE}/admin/skus`,
    JSON.stringify({ sku: SKU, total: STOCK, shards: 64, price_minor: 4999, currency: 'USD' }),
    { headers: { 'Content-Type': 'application/json', 'X-Admin-Token': ADMIN } },
  );
  check(res, { seeded: (r) => r.status === 201 });
}

export default function () {
  const user = `k6-${__VU}-${__ITER}`;
  const headers = { 'X-Admission-Token': token(user) };

  // 50:1 read:write - most traffic is availability polling, served by the CDN in prod.
  if (Math.random() < 0.98) {
    http.get(`${BASE}/v1/skus/${SKU}/availability`, { tags: { op: 'availability' } });
    return;
  }

  const r = http.post(`${BASE}/v1/skus/${SKU}/holds`, null, {
    headers: { ...headers, 'Idempotency-Key': `r-${user}` },
    tags: { op: 'reserve' },
  });
  if (r.status === 409) {
    soldOut.add(1);
    return;
  }
  if (!check(r, { reserved: (x) => x.status === 201 })) return;

  const p = http.post(`${BASE}/v1/holds/${r.json('hold_id')}/pay`, null, {
    headers: { ...headers, 'Idempotency-Key': `p-${user}` },
    tags: { op: 'pay' },
  });
  check(p, { paid_or_pending: (x) => x.status === 200 || x.status === 202 || x.status === 402 });
}

export function teardown() {
  const s = http.get(`${BASE}/admin/skus/${SKU}/stats`, { headers: { 'X-Admin-Token': ADMIN } }).json();
  if (s.orders > STOCK) oversell.add(s.orders - STOCK);
  console.log(`stats: ${JSON.stringify(s)}`);
}

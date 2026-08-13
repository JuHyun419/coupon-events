import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const EVENT_ID = __ENV.EVENT_ID || '1';

export const options = {
  scenarios: {
    phase3_10000tps: {
      executor: 'constant-arrival-rate',
      rate: 10000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 500,
      maxVUs: 5000,
    },
  },
};

export default function () {
  const userId = (Date.now() % 1000000000) + __VU * 100000 + __ITER;
  const payload = JSON.stringify({ userId });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const res = http.post(`${BASE_URL}/api/v3/coupon-events/${EVENT_ID}/issue`, payload, params);

  check(res, {
    'status is 200, 409, or 410': (r) => [200, 409, 410].includes(r.status),
  });
}

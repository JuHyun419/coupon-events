# 부하 테스트 (k6)

## Phase 1: MySQL 비관적 락 (~100 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (재고를 넉넉히 잡아 품절이 아닌 락 경합을 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/v1/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase1-load-test","totalQuantity":5000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase1-issue.js`
4. 관찰 포인트: TPS가 100에 가까워질수록 p95/p99 응답시간 증가, HikariCP 커넥션 타임아웃 로그 발생 여부

## 실측 결과 (2026-07-06)

- 목표: `constant-arrival-rate` 100 req/s, 30초
- 실제 처리량: 97.77 req/s (2934 requests), `dropped_iterations`: 67건 (2.23/s) — 사전 할당된 VU가 이전 요청의 락 대기로 묶여 있어 새 반복을 제때 시작하지 못함
- 응답 시간: `avg` 148.78ms, `p90` 678.18ms, `p95` 1.04s, `max` 1.69s (반면 `min`/`med`는 각각 4ms/8.97ms 수준으로 락 경합이 없을 때는 매우 빠름)
- `http_req_failed`: 0% — 에러 없이 모든 요청이 200/409/410 중 하나로 응답했지만, 지연 시간 분포가 매우 넓게 벌어짐(락 대기 시간이 그대로 응답 지연으로 전가됨)

**결론:** `SELECT ... FOR UPDATE` 비관적 락은 동일 이벤트 row에 대한 모든 발급 요청을 직렬화하므로, ~100 TPS 수준에서 이미 p95/p99 지연 시간이 초 단위로 벌어지고 일부 요청이 처리 큐에서 밀려 드롭된다. 이것이 Phase 1의 예상된 병목이며, Redis 기반 원자적 재고 차감(Phase 2)으로 전환할 근거가 된다.

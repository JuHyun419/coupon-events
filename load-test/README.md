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

## Phase 2: Redis Lua Script (~1,000 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (재고를 넉넉히 잡아 품절이 아닌 처리량 자체를 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase2-load-test","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase2-issue.js`
4. 관찰 포인트: Phase 1(p95 1.04s, dropped_iterations 67건) 대비 p95/p99 응답시간과 dropped_iterations가 얼마나 줄어드는지, 실제 처리량이 1,000 req/s에 얼마나 근접하는지

## 실측 결과 (2026-07-12)

- 목표: `constant-arrival-rate` 1,000 req/s, 30초 (preAllocatedVUs 200, maxVUs 2000)
- 실제 처리량: 578.70 req/s (18,936 requests), `dropped_iterations`: 11,065건 (338.16/s) — 30초 구간 내내 `maxVUs`(2000)에 도달해 더 이상 새 반복(iteration)을 시작할 VU를 확보하지 못함 (`"Insufficient VUs, reached 2000 active VUs and cannot initialize more"` 경고 발생)
- 응답 시간: `avg` 2.89s, `min` 20.98ms, `med` 2.9s, `p90` 4.53s, `p95` 4.8s, `max` 5.3s
- `http_req_failed`: 0.00% (18,936건 중 1건) — `checks`는 100.00% (200/409/410 중 하나로만 응답), 재고 부족(409/410) 없이 사실상 전량이 200으로 발급됨(재고 50,000 대비 발급 시도 약 19,000건 수준)

**결론:** Redis Lua 스크립트 기반 원자적 재고 차감 자체는 MySQL row 락 경합을 제거했지만, 이번 실측에서는 요청마다 이벤트 조회(`SELECT`)와 발급 이력 저장(`INSERT`)이 여전히 동기적으로 MySQL을 거치고, 애플리케이션이 Tomcat/HikariCP 기본 설정(스레드 풀 200, 커넥션 풀 10)인 단일 로컬 인스턴스로 구동되어 1,000 TPS 목표에는 미달했다(실측 처리량 578.70 req/s, 목표 대비 약 58%). 그럼에도 Phase 1(p95 1.04s, dropped_iterations 67건, 목표 100 req/s 대비 실제 97.77 req/s) 대비 절대 처리량은 약 5.9배(97.77 → 578.70 req/s) 높아졌고, `http_req_failed`도 여전히 0%에 가까워 에러 없이 처리되었다. 다만 이번 실측의 p95(4.8s)와 dropped_iterations(11,065건)는 Phase 1보다 절대값 기준으로는 더 크게 나타났는데, 이는 락 경합이 아니라 더 높은 목표 부하(1,000 req/s) 자체와 애플리케이션 스레드/커넥션 풀 한도에 의한 큐잉이 원인이며, MySQL 동기 저장 경로 비동기화나 Tomcat/HikariCP 풀 튜닝이 다음 개선 지점으로 남는다.

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

**결론:** Redis Lua 스크립트 기반 원자적 재고 차감 자체는 MySQL row 락 경합을 제거했지만, 1,000 TPS 목표에는 미달했다(실측 처리량 578.70 req/s, 목표 대비 약 58%). 그럼에도 Phase 1(p95 1.04s, dropped_iterations 67건, 목표 100 req/s 대비 실제 97.77 req/s) 대비 절대 처리량은 약 5.9배(97.77 → 578.70 req/s) 높아졌고, `http_req_failed`도 여전히 0%에 가까워 에러 없이 처리되었다. 다만 이번 실측의 p95(4.8s)와 dropped_iterations(11,065건)는 Phase 1보다 절대값 기준으로는 더 크게 나타났다 — `checks` 100%·`http_req_failed` 0%로 서버가 에러 없이 전량 응답했다는 점에서 이는 Redis Lua 자체의 병목(락 경합)이 아니라 더 높은 목표 부하(1,000 req/s) 자체가 원인일 가능성이 높다. 요청마다 이벤트 조회(`SELECT`)와 발급 이력 저장(`INSERT`)이 여전히 동기적으로 MySQL을 거치고 애플리케이션이 Tomcat/HikariCP 기본 설정(스레드 풀 200, 커넥션 풀 10)인 단일 로컬 인스턴스로 구동된 점이 유력한 원인으로 추정되지만, 이번 실측에서 스레드 풀/커넥션 풀 사용률을 직접 측정하지는 않았으므로 확인된 사실이 아니라 가설이다. MySQL 동기 저장 경로 비동기화나 Tomcat/HikariCP 풀 튜닝, 그리고 실제 풀 사용률 측정이 다음 개선 지점으로 남는다.

**모니터링으로 실측 검증 (2026-08-16)**: Prometheus + Grafana(`pool-bottleneck` 대시보드, uid `pool-bottleneck`)로 HikariCP/Tomcat 풀 사용률을 직접 관찰하며 위 k6 시나리오(`EVENT_ID=97 k6 run load-test/k6/phase2-issue.js`, 컨테이너화된 `app` 서비스 대상 `localhost:8080`)를 재실행하고, 대시보드가 쓰는 것과 동일한 PromQL(`hikaricp_connections_active/idle/pending/max`, `tomcat_threads_busy_threads`/`config_max_threads`, `sum(rate(http_server_requests_seconds_count[1m]))`)을 2~3초 간격으로 폴링했다. 결과: 테스트 시작 직후부터 약 12초간 `hikaricp_connections_active`가 풀 크기(`hikaricp_connections_max`=10)에 정확히 붙고 `idle`이 0으로 떨어지면서 `hikaricp_connections_pending`(대기 중인 요청)이 187~189까지 치솟았고, 같은 구간에서 `tomcat_threads_busy_threads`도 설정 최댓값(`tomcat_threads_config_max_threads`=200)에 정확히 도달했다. 이후 짧은 소강(active 4/10, busy 5/200) 후 두 번째 포화 파동(active 9/10, pending 68, busy 80/200)이 있었고, 테스트 종료 후에는 active 0/idle 10/pending 0/busy 1의 유휴 상태로 복귀했다. 이 포화 구간은 k6가 보고한 실측치(처리량 956.6 req/s, 28,707 requests, `http_req_failed` 0%, `checks` 100%, `avg` 478ms, `med` 301ms, `p90` 1.14s, `p95` 1.24s, `max` 1.96s)의 지연 시간 분포와 시간상 겹친다 — 즉 실패는 없었지만 커넥션/스레드 풀 대기 때문에 일부 요청의 응답이 초 단위로 늘어졌다는 정황과 일치한다. 이로써 위 문단의 가설은 **확인됨**: 이번 실측 수준(약 1,000 req/s 목표, 실제 ~957 req/s)에서 HikariCP 커넥션 풀(10)과 Tomcat 스레드 풀(200) 모두 실제로 포화되었고, HikariCP의 pending 대기열이 0보다 커지는 시점이 직접 관찰됐다. 다만 이번 실측 처리량(~957 req/s)은 원래 위 문단이 설명하려던 Phase 2 기준 실측치(578.70 req/s)와 상당히 다른데, 두 실행이 서로 다른 환경(컨테이너화된 app vs. 원래의 bootRun 로컬 실행)이고 측정 시점도 다르므로 처리량을 1:1로 비교할 수 없으며(이 차이의 구체적인 원인은 이번 실측에서 직접 측정하지 않았다), 이번 검증의 목적은 처리량 자체를 재현하는 것이 아니라 "풀이 실제로 포화되는가"라는 가설을 확인하는 것이었다는 점에서 결론에 영향을 주지 않는다 — HikariCP 10개, Tomcat 200개라는 풀 크기는 실행 환경과 무관하게 고정된 상한이므로, ~957 req/s에서 그 상한에 실제로 도달했다는 사실은 더 낮은 578.70 req/s 구간에서도 동일한 풀 크기가 처리량을 제약하는 요인이었을 것이라는 원래 가설을 뒷받침하는 근거가 된다.

## Phase 3: Redis + Kafka 비동기 영속화 (~10,000 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (v2 admin API 재사용, 재고를 넉넉히 잡아 품절이 아닌 처리량 자체를 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase3-load-test","totalQuantity":500000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase3-issue.js`
4. 관찰 포인트: Phase 2(578.70 req/s, p95 4.8s) 대비 응답 처리량/지연이 얼마나 개선되는지, 실제 처리량이 10,000 req/s에 얼마나 근접하는지, 그리고 k6 종료 후 `issued_coupon` DB row 수가 Redis 기준 확정 발급 수와 같아질 때까지 걸리는 시간(consumer lag)

## 실측 결과 (2026-08-14)

- 목표: `constant-arrival-rate` 10,000 req/s, 30초 (preAllocatedVUs 500, maxVUs 5000)
- 재고 소진에 의한 왜곡을 배제하기 위해 매회 신규 이벤트(재고 500,000)로 동일 조건 3회 반복 실측:
  - 1회차(event 73): 694.26 req/s (24,200 requests), `http_req_duration` avg 6.35s / p90 8.99s / p95 9.98s / max 11.4s
  - 2회차(event 74): 1,147.05 req/s (39,367 requests), avg 3.90s / p90 4.92s / p95 4.95s / max 5.32s
  - 3회차(event 75): 1,167.63 req/s (38,916 requests), avg 3.89s / p90 4.96s / p95 5.09s / max 5.49s
  - 1회차는 애플리케이션/Kafka 프로듀서 커넥션의 콜드 스타트 구간으로 보이며(직전에 `bootRun`을 막 띄운 직후의 첫 실행), 2·3회차는 서로 근접한 값으로 수렴해 이후 반복 실행에서는 안정적인 처리량임을 시사한다.
  - 매회 `dropped_iterations`가 약 26만 건(목표 30만 iteration의 약 87%) 발생 — 30초 내내 `maxVUs`(5000)에 도달해 `"Insufficient VUs, reached 5000 active VUs and cannot initialize more"` 경고가 반복 발생했으며, 응답이 수 초 걸리는 동안 VU가 묶여 새 반복을 시작하지 못한 것이 Phase 2와 동일한 패턴으로 나타남
- `http_req_failed`: 3회 모두 0.00%, `checks` 100.00%(200/409/410 중 하나로만 응답). 재고 500,000 대비 여유가 충분해 재고 소진(409/410) 없이 사실상 전량이 200으로 발급됨
- consumer lag(최종 정합성 도달 시간): k6 실행 종료 직후부터 `curl http://localhost:8080/api/v2/admin/coupon-events/{eventId}`의 `issuedQuantity`(Redis 기준 확정 발급 수)와 `SELECT COUNT(*) FROM issued_coupon WHERE coupon_event_id = {eventId}`(DB에 실제 반영된 건수)를 1초 간격으로 비교했다. 2·3회차 모두 k6 종료 후 첫 폴링 시점(k6 종료 약 10ms 후)에 이미 두 값이 일치했다(2회차: 39,367 = 39,367, 3회차: 38,916 = 38,916). `docker compose exec kafka kafka-consumer-groups.sh --describe --group coupon-issue-consumer`로 확인한 컨슈머 그룹 LAG도 3개 파티션(0/1/2) 모두 0이었다. 이번 실측 범위에서는 관찰 가능한 컨슈머 랙이 사실상 0초 수준이었다.

**결론:** Kafka 비동기 발급 경로는 응답 경로에서 MySQL INSERT를 제거했음에도 목표(10,000 req/s)에는 크게 미달했다(2·3회차 기준 실측 처리량 약 1,147~1,168 req/s, 목표 대비 약 11~12%). Phase 2(578.70 req/s, p95 4.8s) 대비 절대 처리량은 약 2배(578.70 → ~1,150 req/s) 개선되었지만, p95 지연 시간은 오히려 소폭 악화됐다(4.8s → 약 4.95~5.09s) — DB insert를 응답 경로에서 제거하면 지연이 줄어들 것이라는 사전 기대(브리프의 관찰 포인트)와 반대되는 결과다. `http_req_failed` 0%·`checks` 100%로 서버가 에러 없이 전량 응답했고, `dropped_iterations` 패턴이 Phase 2(`maxVUs` 도달로 인한 iteration 드롭)와 동일하게 나타난다는 점에서, 이는 Kafka 비동기 처리 자체의 병목이라기보다 Phase 2 결론에서 지목한 것과 같은 원인(단일 로컬 인스턴스의 Tomcat 기본 스레드 풀 200·HikariCP 커넥션 풀 10)에, 응답 경로에 여전히 남아있는 Redis Lua 호출과 Kafka 프로듀서의 동기 `send().get(3, TimeUnit.SECONDS)` ack 대기가 더해진 결과일 가능성이 높다. 이번 실측에서도 스레드 풀·커넥션 풀·Kafka 프로듀서 ack 대기 시간을 직접 측정하지는 않았지만, 위 Phase 2 절(`모니터링으로 실측 검증 (2026-08-16)`)에서 이 동일한 원인이 이후 직접 측정으로 **확인됨**으로 갱신되었으므로, Phase 3의 미달 역시 (별도로 재측정하지는 않았지만) 같은 확인된 원인을 공유할 가능성이 높다.

반면 컨슈머 랙(최종 정합성 도달 시간)은 매우 우수했다 — k6 종료 시점에 이미 `issued_coupon` row 수가 Redis 기준 확정 발급 수와 일치했고 컨슈머 그룹 LAG도 0이었다. 다만 이는 배치 컨슈머(concurrency 3, max-poll-records 500, fetch-max-wait 200ms)가 매우 효율적이어서라기보다, 실측 처리량 자체가 목표(10,000 req/s)에 크게 못 미쳐(약 1,150 req/s) 컨슈머가 실시간으로 따라잡지 못할 만큼의 백로그가 애초에 Kafka 토픽에 쌓이지 않았기 때문일 가능성이 크다. 즉 이번 실측만으로는 "컨슈머가 실제 10,000 req/s 부하에서도 랙 없이 따라잡는다"는 것을 확인했다고 볼 수 없으며, 프로듀서 측 처리량이 개선되어 실제로 초당 수천 건 이상이 토픽에 유입될 때의 컨슈머 랙은 별도로 재측정이 필요하다.

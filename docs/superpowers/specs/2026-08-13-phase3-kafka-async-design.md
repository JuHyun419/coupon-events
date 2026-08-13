# Phase 3 (Redis 동기 차감 + Kafka 비동기 영속화) 설계

- 작성일: 2026-08-13
- 상위 문서: `docs/superpowers/specs/2026-07-04-coupon-event-design.md` (2장 아키텍처 로드맵, 3장 Phase 3 섹션)
- 관련 문서: `docs/superpowers/specs/2026-07-11-phase2-redis-lua-design.md` (Phase 2 설계 — 이 문서가 재사용하는 Redis Lua 재고 판정 로직의 출처)
- 이 문서는 상위 설계 문서의 Phase 3 개요("무엇을 왜 하는지")를 그대로 따르되, 실제 구현에 필요한 세부 결정을 브레인스토밍을 통해 확정한 내용이다.

## 1. Phase 2와의 관계 — 재고 판정 로직은 재사용, 영속화 경로만 교체

Phase 1(`v1`), Phase 2(`v2`)는 그대로 둔다. v3(`/api/v3/...`)를 병렬로 추가한다.

- **관리자 API는 v2를 그대로 재사용한다.** 이벤트 생성 시 MySQL insert + Redis 시딩(`SET stock`, `DEL issued_users`) 로직이 v2와 100% 동일하므로, 별도 v3 admin 컨트롤러/서비스를 만들지 않는다. `POST /api/v2/admin/coupon-events`로 생성한 이벤트를 `/api/v3/...` 경로로도 그대로 발급할 수 있다.
- v3는 **발급(`/issue`)과 상태 조회(`/status`) 엔드포인트만** 신설한다.
- 재고 차감/중복체크는 Phase 2의 `CouponRedisIssuer`(`jh.couponevent.coupon.redis.application`)를 그대로 재사용한다. 패키지 재배치나 복제 없이 import해서 쓴다.
- 새 코드는 `jh.couponevent.coupon.kafka` 패키지(`api`, `application`, `dto`, `consumer` 하위 패키지) 아래에 작성한다.

## 2. Kafka 토픽 설계

- 토픽명: `coupon-issue-events`
- 파티션 3개, replication-factor 1 (로컬 단일 브로커, KRaft 모드)
- 파티션 키: `userId` — 설계 원칙대로 `eventId`를 키로 잡으면 이벤트 하나가 파티션 하나로 몰려 컨슈머 병렬성이 1로 제한되므로 반드시 `userId`를 키로 사용한다.
- 메시지 값: JSON `{ eventId, userId, issuedAt }`
- `docker-compose.yml`에 KRaft 모드 Kafka 컨테이너(Zookeeper 불필요)를 추가하고, 토픽은 기동 스크립트 또는 admin client로 파티션 3개짜리를 명시적으로 생성한다(auto-create에 의존하지 않음 — 파티션 수를 확정적으로 보장하기 위함).

## 3. 발급 흐름과 정합성

```
POST /api/v3/coupon-events/{eventId}/issue
1. 이벤트 오픈 시각(startAt) 확인 → 아직이면 403 (v1/v2와 동일)
2. Lua 스크립트로 재고 차감 + 중복체크 원자적 수행 (CouponRedisIssuer.tryIssue 재사용)
3. DUPLICATE/SOLD_OUT이면 각각 409/410 응답 (Redis만 관여, Kafka는 아직 관여하지 않음)
4. SUCCESS면 KafkaTemplate.send(topic, key=userId, value={eventId,userId,issuedAt}).get(timeout)로
   "동기식" 발행 — producer가 브로커에 실제로 쓰는 것까지 확인하고 기다린다
   - 발행 성공 → 200 SUCCESS 즉시 응답 (DB row 반영은 이 시점 이후 컨슈머가 비동기로 처리)
   - 발행 실패(타임아웃/브로커 연결 끊김 등) → Redis를 보상 처리(INCR stock + SREM issued_users)하고
     새 예외 CouponIssuePublishFailedException을 던져 500 응답
```

**동기식 send를 선택한 이유**: Phase 2의 "DB insert 실패 시 Redis 롤백" 패턴과 일관성을 유지하기 위함이다. Fire-and-forget으로 발행하면 "응답은 SUCCESS인데 메시지는 유실"되는 상황이 생길 수 있고, 이 경우 Redis 차감은 이미 확정되어 정합성이 깨진다. 동기 발행은 응답 지연을 수 ms 늘리는 대신, "SUCCESS 응답 = 최소한 Kafka에 안전하게 적재됨"을 보장한다.

`GlobalExceptionHandler`에 `CouponIssuePublishFailedException` 처리 핸들러를 하나 추가한다(Phase 2의 `handlePersistenceFailure`와 나란히, 새 핸들러 클래스는 만들지 않는다).

**알려진 한계 — 발행 타임아웃과 롤백 사이의 레이스**: `.get(timeout)`이 타임아웃으로 실패해도 이는 애플리케이션 스레드가 기다리기를 포기했다는 뜻일 뿐, Kafka 브로커에 대한 실제 프로듀서 요청 자체를 취소하지는 않는다. 즉 타임아웃 이후에 브로커가 실제로는 메시지를 정상 적재할 수 있다 — 이 경우 애플리케이션은 이미 500 응답과 함께 Redis를 롤백(`INCR`+`SREM`)했지만, 컨슈머는 그 메시지를 정상적으로 읽어 DB에 row를 반영한다. 결과적으로 "Redis 기준으로는 발급되지 않았는데 DB에는 발급 기록이 존재"하는 상태가 영구적으로 남을 수 있다(사용자 입장에서는 500을 받았지만 실제로는 쿠폰을 보유). 이는 "동기 발행 + 타임아웃 기반 롤백" 패턴이 근본적으로 안고 있는 at-least-once/타임아웃 레이스이며, 이 프로젝트 범위(로컬 학습 환경, 8장 "범위 밖" 참고)에서는 코드로 완전히 막지 않고 알려진 한계로 문서화하는 것으로 대응한다. 프로덕션이라면 프로듀서 idempotence 설정과 별도의 정합성 배치 검증(Redis-DB 대사) 등이 추가로 필요하다.

## 4. Consumer — 배치 영속화

- Spring Kafka 배치 리스너를 사용한다: `spring.kafka.listener.type=batch`, `concurrency=3`(파티션 수와 매칭해 파티션당 컨슈머 스레드 1개)
- `spring.kafka.consumer.max-poll-records=500`, `fetch.max.wait.ms=200`으로 상위 설계문서의 "500건 또는 200ms 윈도우" 배치 조건을 Kafka 표준 파라미터로 근사한다.
- 리스너는 poll로 받은 `List<ConsumerRecord<String, String>>`을 `IssuedCoupon` 엔티티 리스트로 변환해 `issuedCouponRepository.saveAll(...)`을 호출한 뒤, 성공 시 수동 `Acknowledgment.acknowledge()`를 호출한다(자동 커밋 사용 안 함 — insert 실패 시 재처리되도록). **정정**: `IssuedCoupon`이 `GenerationType.IDENTITY`를 쓰기 때문에 Hibernate는 `saveAll(...)` 안에서도 각 row를 즉시 개별 INSERT해야 하고(ID를 미리 알 수 없어 JDBC 배치를 비활성화함), `hibernate.jdbc.batch_size` 설정도 없다 — 따라서 실제로는 "배치 insert"가 아니라 "한 트랜잭션 안에서 여러 건을 순차 INSERT"하는 것에 가깝다. 이번 실측(부하테스트 결과 참고)에서는 컨슈머가 병목이 아니었으므로 성능에 영향은 없었지만, 용어는 정확히 해둔다.
- **at-least-once 재처리 대비**: 컨슈머 재시작/리밸런스로 동일 메시지가 재처리될 수 있다. `issued_coupon.uk_event_user` UNIQUE 제약이 최종 방어선이며, 배치 insert 중 일부 row가 이미 존재해 실패하면 해당 배치는 1건씩 재시도(save)하여 나머지 정상 row는 반영되도록 한다(전체 배치 롤백 방지).

## 5. 상태 조회 API — 비동기 지연을 직접 관찰하는 용도

```
GET /api/v3/coupon-events/{eventId}/users/{userId}/status
→ { eventId, userId, status: NOT_ISSUED | PENDING | COMPLETED, issuedAt }
```

판정 로직:
1. Redis `issued_users` Set에 `userId`가 없음 → `NOT_ISSUED`
2. Redis에는 있지만 `issued_coupon` 테이블에 아직 해당 row가 없음 → `PENDING` (Kafka 발행까지는 끝났고, 컨슈머의 배치 반영을 기다리는 중)
3. `issued_coupon` 테이블에 row 존재 → `COMPLETED` (`issuedAt`은 DB의 `issued_at` 값)

이 엔드포인트는 Phase 3가 도입하는 "응답과 영속화 완료 사이의 시간차(최종 정합성)"를 사용자가 직접 눈으로 확인할 수 있게 하는 것이 유일한 목적이다.

## 6. 테스트 전략

Phase 1/2와 동일하게 Testcontainers는 쓰지 않는다. 로컬 `docker-compose`로 띄운 MySQL/Redis/Kafka에 실제로 연결해서 검증한다.

- **의존성 추가**: `testImplementation("org.awaitility:awaitility-kotlin:4.2.2")` — Kafka 발행 이후 컨슈머가 DB에 반영할 때까지의 비동기 대기를 `await atMost Duration.ofSeconds(N) untilAsserted { ... }` 형태로 선언적으로 표현한다. `Thread.sleep` 폴링보다 빠르고 덜 플레이키하다.
- **동시성 테스트**: Phase 2의 `CouponIssueConcurrencyTest`와 대응 — 재고 100 / 동시 요청 200 → 정확히 100건만 SUCCESS 응답을 받는지 확인(이건 Redis Lua 단계에서 이미 보장되므로 즉시 검증 가능). 이어서 Awaitility로 "DB row 수가 100이 될 때까지" 대기한 뒤 최종 일치 여부를 확인한다.
- **발행 실패 → 롤백 테스트**: `KafkaTemplate`을 mock으로 대체해 `send(...).get(...)`이 예외를 던지도록 만든 뒤, Redis stock/issued_users가 원래 상태로 복구되는지, `CouponIssuePublishFailedException`이 500으로 매핑되는지 확인한다.
- **상태 전이 테스트**: 발급 직후 `PENDING`, 컨슈머 반영 후 `COMPLETED`로 전이하는지 Awaitility로 확인한다.
- **배치 리스너 단위 테스트**: 여러 건의 `ConsumerRecord`를 한 번에 넘겼을 때 `saveAll`이 호출되는지, 일부 row가 UNIQUE 제약으로 실패해도 나머지가 반영되는지 검증한다.

## 7. 부하테스트

- `load-test/k6/phase3-issue.js` 신설, `load-test/README.md`에 "Phase 3" 섹션 추가. 목표 TPS 10,000.
- 관찰 포인트:
  - Phase 2 대비 API 응답 처리량/지연(p95/p99) — Kafka 발행이 DB insert보다 빠르므로 개선을 기대
  - **Consumer lag**: 부하 종료 직후 시점에 `PENDING` 상태로 남아있는 건수를 세어("최종 정합성까지 걸리는 시간") 유입 속도와 적재 속도의 격차를 실측한다. Phase 1/2 보고서(`docs/superpowers/reports/.../task-8-load-test.md`)에서 다뤘던 "병목 지점에 대기열이 쌓인다"는 개념을 Phase 3에서는 컨슈머 랙이라는 형태로 직접 관찰하게 되는 것이 핵심 산출물이다.

## 8. 범위 밖

- 정교한 컨슈머 랙 모니터링(Grafana 등 대시보드 구축) — 로그/카운트 수준까지만 다룬다.
- Kafka 브로커 장애/재시작 시나리오, 멀티 브로커 복제(replication-factor > 1).
- Dead-letter 토픽/고도화된 재시도 정책 — 배치 insert 실패 시 1건씩 재시도하는 수준까지만 다루고, 별도 DLQ나 재시도 백오프 전략은 다루지 않는다(YAGNI).
- `coupon_event.issued_quantity` 컬럼 동기화 — Phase 2와 동일하게 v3도 이 컬럼을 건드리지 않는다. 관리자 조회의 `issuedQuantity`는 v2와 동일한 방식(`totalQuantity - Redis stock`)으로 역산한다.

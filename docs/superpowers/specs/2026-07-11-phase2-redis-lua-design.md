# Phase 2 (Redis Lua Script 원자적 차감) 설계

- 작성일: 2026-07-11
- 상위 문서: `docs/superpowers/specs/2026-07-04-coupon-event-design.md` (2장 아키텍처 로드맵, 3장 Phase 2 섹션)
- 이 문서는 상위 설계 문서의 Phase 2 개요("무엇을 왜 하는지")를 그대로 따르되, 실제 구현에 필요한 세부 결정("어떻게 나눌지, 어떤 경로/패키지를 쓸지, 정합성을 어떻게 지킬지")을 브레인스토밍을 통해 확정한 내용이다.

## 1. Phase 1과의 관계 — 교체가 아니라 병존

Phase 1의 `CouponIssueService`(MySQL `SELECT ... FOR UPDATE`)는 그대로 둔다. 같은 코드베이스 안에 두 가지 구현을 나란히 두고 비교할 수 있게 하는 것이 학습 목적에 맞다.

- 기존 경로를 버전 접두사로 이동: `/api/coupon-events/...`, `/api/admin/coupon-events/...` → `/api/v1/coupon-events/...`, `/api/v1/admin/coupon-events/...` (컨트롤러의 `@RequestMapping` 경로만 변경, 패키지/클래스명은 그대로 유지)
- 새 Redis 기반 구현은 `/api/v2/...` 경로, 새 패키지 `jh.couponevent.coupon.redis`(`api`, `application`, `dto` 하위 패키지) 아래에 작성
- `CouponEvent`, `IssuedCoupon` 엔티티와 `CouponEventRepository`, `IssuedCouponRepository`는 v1/v2가 공유한다. 데이터 모델은 바뀌지 않고, "재고 판정을 어디서 하는가"만 바뀐다.

## 2. Redis 키 설계

기존 설계 문서의 Lua 스크립트를 그대로 사용한다.

- `coupon:{eventId}:stock` — 문자열, 남은 재고 수 (정수)
- `coupon:{eventId}:issued_users` — Set, 발급받은 `userId` 목록

```lua
-- KEYS[1] = stock key, KEYS[2] = issued_users set key, ARGV[1] = userId
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return 'DUPLICATE'
end
local stock = tonumber(redis.call('GET', KEYS[1]))
if stock <= 0 then
    return 'SOLD_OUT'
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
return 'SUCCESS'
```

Spring 쪽에서는 `StringRedisTemplate` + `DefaultRedisScript<String>`으로 이 스크립트를 실행한다(Spring Data Redis, Lettuce 클라이언트 — `spring-boot-starter-data-redis` 표준 조합).

## 3. 관리자 API — 이벤트 생성 시 Redis 자동 시딩

`POST /api/v2/admin/coupon-events`는 하나의 요청 안에서:

1. MySQL에 `coupon_event` insert (Phase 1과 동일한 엔티티/리포지토리 재사용)
2. `SET coupon:{id}:stock {totalQuantity}`
3. `DEL coupon:{id}:issued_users` (혹시 남아있는 이전 데이터 정리)

별도의 "활성화" 단계 없이, 이벤트 생성 즉시 v2 발급 API를 호출할 수 있는 상태가 된다. 오픈 시각 이전 차단은 Phase 1과 동일하게 애플리케이션 레벨(`startAt` 비교)에서 처리하며, Redis 접근 이전에 걸러낸다.

`GET /api/v2/admin/coupon-events/{id}`의 `issuedQuantity`는 DB 컬럼이 아니라 Redis에서 역산한다: `totalQuantity - GET coupon:{id}:stock`. `coupon_event.issued_quantity` 컬럼은 v2 경로에서 건드리지 않는다(아래 4번 참고).

## 4. 발급 흐름과 정합성

`POST /api/v2/coupon-events/{eventId}/issue`:

1. Lua 스크립트로 중복 체크 + 재고 차감을 원자적으로 수행
2. `DUPLICATE`/`SOLD_OUT`이면 각각 409/410 응답 (Lua 실행에는 MySQL 트랜잭션이 전혀 관여하지 않는다)
3. `SUCCESS`면 `issued_coupon` insert (Phase 1과 같은 테이블, 같은 UNIQUE 제약이 최종 방어선)
4. insert가 실패하면 Redis를 보상 처리한다: `INCR coupon:{id}:stock` + `SREM coupon:{id}:issued_users {userId}`, 그리고 새 예외 `CouponIssuePersistenceFailedException`을 던져 500으로 응답 (Phase 1의 `GlobalExceptionHandler`에 핸들러 메서드 하나를 추가하는 형태로, 새 핸들러 클래스를 만들지 않는다)

**`coupon_event.issued_quantity`를 갱신하지 않기로 한 이유**: "SSOT는 Redis"라는 설계 원칙을 코드로도 지키기 위함이다. 만약 insert와 같은 트랜잭션에서 `issued_quantity += 1`도 같이 갱신한다면, v1 admin 조회 API와 값이 같아져서 편리해 보이지만, MySQL 카운터와 Redis 카운터라는 두 개의 카운터를 유지하게 되어 서로 어긋날 가능성(예: Redis 롤백은 했는데 이 갱신은 롤백 못한 경우)이 생긴다. 하나의 카운터만 두는 게 더 단순하고 정합성 문제의 여지가 적다.

## 5. 테스트 전략

Phase 1과 동일한 방침을 유지한다 — Testcontainers를 쓰지 않고 로컬 `docker-compose` Redis에 실제로 접속해서 검증한다.

- `docker-compose.yml`에 `redis:7` 서비스 추가 (포트 6379)
- Lua 스크립트 자체의 동작(중복/품절/성공 판정)은 실제 Redis에 대한 통합 테스트로 검증
- Phase 1의 `CouponIssueConcurrencyTest`와 대응하는 동시성 테스트를 작성 — 이번엔 MySQL 락이 아니라 Redis Lua의 원자성이 초과 발급을 막는지 확인 (재고 100 / 동시 요청 200 → 정확히 100 성공)
- DB insert 실패 시 보상 처리(4번) 경로도 별도 테스트로 검증 — 예를 들어 리포지토리를 실패하도록 만든 상태에서 Redis 재고가 원래대로 복구되는지 확인

## 6. 부하테스트

Phase 1의 `load-test/k6/phase1-issue.js`와 대응하는 `load-test/k6/phase2-issue.js`를 추가하고, 목표 TPS를 100 → 1,000으로 올려 실행한다. 관찰 포인트는 Phase 1과 대비해서 p95/p99 지연 시간과 `dropped_iterations`가 확연히 줄어드는지, 그리고 실제 처리량이 1,000 req/s에 얼마나 근접하는지다.

## 7. 범위 밖

- Phase 3(Kafka 비동기 영속화)은 이 문서의 범위 밖이다.
- `coupon_event.issued_quantity`를 v2에서도 실시간으로 맞추는 등의 이중 카운터 동기화는 다루지 않는다(4번 참고, YAGNI).
- Redis 장애/재시작 시나리오(예: Redis 다운 상태에서의 폴백)는 다루지 않는다 — 로컬 학습 환경에서는 Redis가 항상 떠 있다고 가정한다.

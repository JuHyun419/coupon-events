# 선착순 쿠폰 발급 이벤트 설계

- 작성일: 2026-07-04
- 목적: 학습/포트폴리오용 심화 학습. 선착순 쿠폰 발급 시나리오를 통해 동시성 제어, 분산 락, 비동기 처리 기술을 단계적으로 경험하고 트레이드오프를 문서화한다.

## 1. 요구사항 요약

- 10:00 정각 오픈, 총 10,000장 한정 쿠폰
- 트래픽은 100 TPS → 1,000 TPS → 10,000 TPS로 점진적으로 증가
- 1인 1매 제한 (동일 사용자의 중복 발급 방지)
- 논리적으로는 멀티 인스턴스(로드밸런서 뒤 여러 대) 환경을 전제로 설계하되, 로컬 검증은 단일 인스턴스로도 가능하게 하고 필요 시 docker-compose로 인스턴스를 여러 개 띄워 분산 상황을 재현한다
- DB: MySQL / ORM: JPA (+ 필요 시 QueryDSL) / 캐시·분산 제어: Redis / 비동기 처리: Kafka
- 부하 테스트 도구: k6

## 2. 아키텍처 로드맵

같은 문제(재고 경합)를 난이도를 높여가며 세 번 풀어본다. 각 Phase는 이전 Phase의 병목을 실측으로 확인한 뒤 다음 기술로 넘어가는 방식으로 진행한다.

| Phase | 목표 TPS | 핵심 기술 | 재고 SSOT(단일 진실 공급원) | 배우는 것 |
|---|---|---|---|---|
| Phase 1 | ~100 | MySQL 비관적 락 (`SELECT ... FOR UPDATE`) | MySQL `coupon_event.issued_quantity` | 락 경합, 커넥션 풀 고갈 |
| Phase 2 | ~1,000 | Redis + Lua Script 원자적 차감 | Redis `stock` 키 | 분산 원자성 확보, Redis-DB 정합성 문제 |
| Phase 3 | ~10,000 | Redis(동기 차감) + Kafka(비동기 영속화) | Redis `stock` 키 | 응답/영속화 분리, 최종 정합성, consumer lag |

설계 원칙: Phase가 올라가도 "재고 차감의 원자성"은 항상 Redis Lua 스크립트가 책임지고, Kafka는 DB 기록 작업을 비동기로 미루는 역할만 한다. Phase 3에서도 사용자는 API 호출 즉시 당첨 여부를 알 수 있다.

## 3. Phase별 상세 설계

### Phase 1 — MySQL 비관적 락 (~100 TPS)

```
POST /api/coupon-events/{eventId}/issue
1. 이벤트 오픈 시각(start_at) 확인 → 아직이면 403
2. BEGIN TRANSACTION
3. SELECT * FROM coupon_event WHERE id=? FOR UPDATE   -- 재고 행에 락
4. issued_quantity >= total_quantity ?  → 실패(품절, 410)
5. issued_coupon(event_id, user_id) UNIQUE 제약 확인   -- 중복 발급 방지 (409)
6. INSERT INTO issued_coupon (...)
7. UPDATE coupon_event SET issued_quantity = issued_quantity + 1
8. COMMIT
```

**관찰 포인트**: 모든 요청이 재고 행 하나를 두고 `FOR UPDATE`로 직렬화된다. 트랜잭션 처리 시간(수 ms)에 비례해 처리량 상한이 생기고, TPS가 올라갈수록 커넥션 풀이 락 대기로 가득 차 타임아웃이 연쇄적으로 발생한다. 이를 부하테스트로 실측하는 것이 Phase 1의 핵심 산출물이다.

부가 실험(선택): 비관적 락 대신 낙관적 락(`@Version` + 재시도)으로 바꿔서 핫로우 경합 시 재시도 폭풍이 왜 더 나쁜 결과를 내는지 비교 기록한다.

### Phase 2 — Redis Lua Script 원자적 차감 (~1,000 TPS)

이벤트 오픈 전에 Redis에 재고 수량(`coupon:{eventId}:stock`)과 발급자 Set(`coupon:{eventId}:issued_users`)을 미리 세팅한다.

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

`SUCCESS`를 받은 요청만 동기적으로 DB에 `issued_coupon`을 insert한다. DB에는 더 이상 경합할 "재고 행"이 없으므로(서로 다른 row에 insert) 락 경합이 사라진다.

**트레이드오프**: Redis 차감은 성공했는데 뒤이은 DB insert가 실패하면 "Redis는 발급됐다고 하는데 DB엔 없는" 정합성 문제가 생긴다. Phase 2는 아직 동기 구조이므로, insert 실패 시 Redis를 롤백(`INCR` + `SREM`)하고 사용자에게 실패를 응답하는 보상 처리로 방어한다.

### Phase 3 — Redis(동기) + Kafka(비동기 영속화) (~10,000 TPS)

Redis Lua 스크립트로 재고 차감/중복 체크는 Phase 2와 동일하게 유지한다(사용자는 여전히 API 응답으로 당첨 여부를 실시간으로 안다). `SUCCESS` 이후 DB에 직접 쓰는 대신 Kafka로 발행한다.

```
Redis Lua: SUCCESS
  → Kafka Producer: publish("coupon-issue-events", key=userId, value={eventId, userId, ts})
  → 사용자에게 즉시 SUCCESS 응답 반환 (DB 반영은 비동기)

Kafka Consumer Group
  → 배치 단위(예: 500건 또는 200ms 윈도우)로 모아 batch insert
  → DB unique(event_id, user_id) 제약으로 중복 insert 방지 (at-least-once 재처리 대비)
```

**핵심 트레이드오프**: "API 응답"과 "DB 영속화 완료" 사이에 시간차가 생긴다(최종 정합성). 파티션 키는 `eventId`가 아니라 `userId`로 잡아야 컨슈머가 병렬로 확장 가능하다(eventId로 잡으면 이벤트 하나가 파티션 하나로 몰려 병목이 생긴다).

**검토했으나 채택하지 않은 대안**: Redis List(`BRPOP`/`LPUSH`)로 큐를 직접 구현하는 방법. Consumer가 꺼내는 즉시 항목이 삭제되어 처리 중 장애 시 메시지가 유실되고, Kafka 같은 consumer group/offset 기반 재처리가 없다. Redis Streams는 이 약점을 보완하지만, Kafka 학습이라는 원래 목표와 실무에서의 범용성을 고려해 Kafka로 확정했다.

## 4. 데이터 모델

```sql
CREATE TABLE coupon_event (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    total_quantity INT NOT NULL,
    issued_quantity INT NOT NULL DEFAULT 0,  -- Phase 1의 SSOT. Phase 2/3부터는 참고용(SSOT는 Redis)
    start_at DATETIME NOT NULL,
    end_at DATETIME,
    status VARCHAR(20) NOT NULL,             -- READY, OPEN, CLOSED
    version BIGINT NOT NULL DEFAULT 0,        -- 낙관적 락 실험용
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

CREATE TABLE issued_coupon (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    coupon_event_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    issued_at DATETIME NOT NULL,
    status VARCHAR(20) NOT NULL,             -- ISSUED, USED, CANCELLED
    UNIQUE KEY uk_event_user (coupon_event_id, user_id)
);
```

`issued_coupon.uk_event_user`는 모든 Phase에서 "동일 사용자 중복 발급"을 막는 최종 방어선 역할을 한다 (Redis 판정이 뚫리거나 Kafka가 메시지를 재처리해도 DB 레벨에서 막힘).

## 5. API 스펙 (초안)

| Method | Path | 설명 |
|---|---|---|
| POST | `/api/admin/coupon-events` | 이벤트 생성 (`name`, `totalQuantity`, `startAt`) |
| GET | `/api/admin/coupon-events` | 이벤트 목록 조회 (QueryDSL 동적 검색: 상태, 기간 등) |
| POST | `/api/coupon-events/{eventId}/issue` | 쿠폰 발급 요청 (`userId`) → `200 SUCCESS` / `409 DUPLICATE` / `410 SOLD_OUT` / `403 NOT_OPEN_YET` |
| GET | `/api/coupon-events/{eventId}/users/{userId}/status` | 발급 상태 조회 (Phase 3에서 비동기 반영 지연을 확인하는 용도) |

인증/인가는 범위 밖으로 두고 `userId`는 요청 바디로 직접 받는다(실제 서비스라면 인증 컨텍스트에서 추출).

## 6. 정각 오픈 처리

- 각 애플리케이션 인스턴스가 로컬 시계로 `now >= start_at`을 판단한다(중앙 스케줄러에 대한 단일 장애점을 피하기 위함). 인스턴스 간 시계 오차를 줄이기 위해 NTP 동기화를 전제로 한다.
- Redis 재고 값은 이벤트 오픈 이전에 미리 세팅해두고, 오픈 시각 이전 요청은 API 레이어에서 즉시 차단한다(Redis/DB 접근 이전에 필터링해 불필요한 부하를 막음).

## 7. 로컬 개발/검증 환경

- `docker-compose`로 MySQL, Redis, Kafka(KRaft 모드로 Zookeeper 생략)를 구성한다.
- 분산 상황(멀티 인스턴스) 재현이 필요할 때는 앱 컨테이너를 2~3개 띄우고 간단한 로드밸런서(nginx)를 앞단에 둔다. 평소 개발 중에는 단일 인스턴스로 충분하다.

## 8. 기술적 난제 & 트레이드오프 정리

1. **Redis-DB 이중 쓰기 정합성** (Phase 2/3 공통): Redis 차감 성공 후 DB 반영(또는 Kafka 발행)이 실패하면 Redis를 롤백하는 보상 처리가 필요하다.
2. **Kafka at-least-once 재처리**: 컨슈머 재시작/리밸런스 시 메시지가 중복 처리될 수 있다 → DB unique 제약으로 멱등성을 확보한다.
3. **파티션 키 선택**: `eventId`로 잡으면 컨슈머 병렬성이 파티션 1개로 제한된다. `userId`로 잡아야 여러 컨슈머로 분산된다.
4. **Consumer lag**: 10,000 TPS 유입 대비 DB batch insert 처리량이 못 따라가면 랙이 쌓인다. 본 프로젝트에서는 로그/간단한 메트릭 수준까지만 다루고, 정교한 모니터링 대시보드 구축은 범위 밖으로 둔다.
5. **정각 트래픽 폭주**: 인스턴스별 로컬 시계 판단 + NTP, Redis 재고 사전 세팅으로 대응한다.
6. **멀티 인스턴스 로컬 재현**: docker-compose로 앱 컨테이너를 다중화해 "로컬 락이 왜 안 통하는지"를 직접 재현한다.

## 9. 테스트 전략

- **단위 테스트**: Redis Lua 스크립트 로직(재고 소진/중복 판정 케이스), 재고 계산 로직. Testcontainers Redis 사용.
- **동시성/통합 테스트**: Testcontainers(MySQL + Redis + Kafka)를 띄우고 `ExecutorService`로 N개 동시 요청을 발생시켜 "정확히 `total_quantity`개만 성공"하는지, 사용자당 중복 미발급인지를 검증한다.
- **부하 테스트**: k6로 100 → 1,000 → 10,000 VU 단계별 시나리오를 작성한다. 응답시간(p95/p99), 에러율을 측정하고, 테스트 종료 후 "최종 DB row 수 == 재고 수량"이 일치하는지 사후 검증 스크립트로 확인한다.

## 10. 범위 밖 (Out of Scope)

- 결제 연동, 실제 알림(푸시/카카오톡 등) 발송
- 정교한 모니터링 대시보드(Grafana 등, 후속 과제로 남김)
- 인증/인가 체계 (userId를 요청으로 직접 받는 것으로 대체)

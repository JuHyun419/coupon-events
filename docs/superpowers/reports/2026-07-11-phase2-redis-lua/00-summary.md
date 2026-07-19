# Phase 2 (Redis Lua Script) 구현 결과 요약

- 계획: `docs/superpowers/plans/2026-07-11-phase2-redis-lua.md`
- 스펙: `docs/superpowers/specs/2026-07-11-phase2-redis-lua-design.md`
- 브랜치: `phase2-redis-lua`
- PR: [#2 Phase 2: Redis Lua Script 기반 원자적 쿠폰 발급 (~1,000 TPS)](https://github.com/JuHyun419/coupon-events/pull/2)
- 손으로 직접 확인해보고 싶다면: [manual-testing-guide.md](manual-testing-guide.md)

## 전체 아키텍처

Phase 1(v1, MySQL 비관적 락)은 그대로 두고, Phase 2(v2, Redis Lua Script)를 병렬로 추가했다. 엔티티/리포지토리/예외 핸들러는 v1·v2가 공유하고, "재고를 어떻게 지키는가"만 다르다.

```mermaid
flowchart TB
    Client(["클라이언트 / k6"])

    subgraph V1["v1 — MySQL 비관적 락 (Phase 1)"]
        direction TB
        AdminCtrl1["CouponEventAdminController<br/>/api/v1/admin/coupon-events"]
        IssueCtrl1["CouponIssueController<br/>/api/v1/coupon-events/{eventId}"]
        AdminSvc1["CouponEventAdminService"]
        IssueSvc1["CouponIssueService<br/>(SELECT ... FOR UPDATE)"]
    end

    subgraph V2["v2 — Redis Lua Script (Phase 2)"]
        direction TB
        AdminCtrl2["CouponEventAdminControllerV2<br/>/api/v2/admin/coupon-events"]
        IssueCtrl2["CouponIssueControllerV2<br/>/api/v2/coupon-events/{eventId}"]
        AdminSvc2["CouponEventAdminServiceV2"]
        IssueSvc2["CouponIssueServiceV2"]
        RedisIssuer["CouponRedisIssuer<br/>(Lua script)"]
    end

    subgraph SHARED["공유 도메인 계층"]
        Domain["CouponEvent / IssuedCoupon<br/>CouponEventRepository / IssuedCouponRepository"]
        GEH["GlobalExceptionHandler"]
    end

    MySQL[("MySQL")]
    Redis[("Redis")]

    Client --> AdminCtrl1 --> AdminSvc1 --> Domain
    Client --> IssueCtrl1 --> IssueSvc1 --> Domain
    Client --> AdminCtrl2 --> AdminSvc2 --> Domain
    AdminSvc2 --> RedisIssuer
    Client --> IssueCtrl2 --> IssueSvc2 --> Domain
    IssueSvc2 --> RedisIssuer
    Domain --> MySQL
    RedisIssuer --> Redis
    IssueSvc1 -. "throws" .-> GEH
    IssueSvc2 -. "throws" .-> GEH
```

v1과 v2 클래스가 처음엔 같은 단순 클래스명(`CouponEventAdminController` 등)을 썼는데, Spring의 기본 빈 이름이 패키지와 무관하게 단순 클래스명만 본다는 걸 놓쳐서 실제로 앱이 기동조차 안 되는 버그가 났었다 — 자세한 경위는 [계획 문서 Task 7 amendment](../../plans/2026-07-11-phase2-redis-lua.md) 참고, 요약 다이어그램은 아래 "겪은 문제" 절 참고.

## Redis 키 설계 & Lua 스크립트 원자성

재고 확인·중복 체크·차감을 하나의 Lua 스크립트로 묶어서 "확인 후 차감" 사이에 다른 요청이 끼어들 수 없게 만들었다(Redis는 스크립트 실행 중 단일 스레드로 동작).

```mermaid
flowchart TD
    Start(["tryIssue(eventId, userId)"]) --> Lua["Lua 스크립트 실행 (원자적)"]
    Lua --> Dup{"SISMEMBER issued_users userId?"}
    Dup -->|"1 (이미 있음)"| RetDup["'DUPLICATE' 반환"]
    Dup -->|0| Stock{"GET stock &lt;= 0 ?"}
    Stock -->|예| RetSold["'SOLD_OUT' 반환"]
    Stock -->|아니오| Decr["DECR stock<br/>SADD issued_users userId"]
    Decr --> RetSucc["'SUCCESS' 반환"]
```

키: `coupon:{eventId}:stock`(잔여 재고), `coupon:{eventId}:issued_users`(발급자 Set).

## 이벤트 생성 흐름 (Redis 자동 시딩)

관리자가 이벤트를 만드는 순간 MySQL insert와 Redis 시딩이 한 요청 안에서 함께 일어난다 — 별도 "활성화" 단계가 없다.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as CouponEventAdminControllerV2
    participant Svc as CouponEventAdminServiceV2
    participant DB as MySQL
    participant R as Redis

    C->>Ctrl: POST /api/v2/admin/coupon-events
    Ctrl->>Svc: create(request)
    Svc->>DB: INSERT coupon_event
    DB-->>Svc: eventId
    Svc->>R: SET coupon:{eventId}:stock = totalQuantity
    Svc->>R: DEL coupon:{eventId}:issued_users
    Svc-->>Ctrl: CouponEventResponse(issuedQuantity=0)
    Ctrl-->>C: 201 Created
```

관리자 조회(`GET`)의 `issuedQuantity`는 DB 컬럼이 아니라 매번 `totalQuantity - Redis stock`으로 역산한다 — `coupon_event.issued_quantity`는 v2 경로에서 절대 쓰지 않는다.

## 발급 요청 흐름 (성공 / 실패 / 영속화 실패 보상 처리)

Lua 스크립트가 이미 `SUCCESS`를 반환한 뒤(=Redis 재고는 이미 차감된 뒤) MySQL insert가 실패하면, Redis를 롤백(`INCR`+`SREM`)해서 재고가 "먹튀"되지 않게 막는다.

```mermaid
sequenceDiagram
    participant C as Client
    participant Ctrl as CouponIssueControllerV2
    participant Svc as CouponIssueServiceV2
    participant DB as MySQL
    participant R as Redis (Lua)
    participant GEH as GlobalExceptionHandler

    C->>Ctrl: POST /api/v2/coupon-events/{id}/issue
    Ctrl->>Svc: issue(eventId, userId)
    Svc->>DB: findById(eventId)
    alt 이벤트 없음 / 오픈 전
        Svc-->>GEH: NotFoundException / NotOpenException
        GEH-->>C: 404 / 403
    else 이벤트 존재 & 오픈됨
        Svc->>R: tryIssue(eventId, userId)
        alt DUPLICATE
            R-->>Svc: DUPLICATE
            Svc-->>GEH: DuplicateCouponIssueException
            GEH-->>C: 409
        else SOLD_OUT
            R-->>Svc: SOLD_OUT
            Svc-->>GEH: CouponSoldOutException
            GEH-->>C: 410
        else SUCCESS (재고 이미 차감됨)
            R-->>Svc: SUCCESS
            Svc->>DB: INSERT issued_coupon
            alt insert 성공
                DB-->>Svc: OK
                Svc-->>Ctrl: CouponIssueResult
                Ctrl-->>C: 200 SUCCESS
            else insert 실패
                DB-->>Svc: Exception
                Svc->>R: rollback (INCR stock, SREM issued_users)
                Svc-->>GEH: CouponIssuePersistenceFailedException
                GEH-->>C: 500
            end
        end
    end
```

## 동시성 검증 (실제 Redis 대상)

MySQL 락 대신 Redis Lua의 원자성이 초과 발급을 막는다는 걸 실제 Redis로 검증했다 — 재고 100개 / 동시 요청 200건 → 정확히 100건 성공.

```mermaid
sequenceDiagram
    participant Pool as ExecutorService(32 threads)
    participant Lua as Redis Lua Script
    participant DB as issued_coupon

    Note over Pool: userId 1..200, 200건 동시 제출

    loop 200건 모두
        Pool->>Lua: tryIssue(eventId, userId) — 원자적 실행
        alt 재고 남음 (최대 100건)
            Lua-->>Pool: SUCCESS
            Pool->>DB: INSERT issued_coupon
        else 재고 소진
            Lua-->>Pool: SOLD_OUT
        end
    end

    Note over Lua: MySQL 락 대기 없이 Redis가 단일 스레드로 원자성 보장 →<br/>정확히 100건만 SUCCESS, DB row도 100건
```

## 겪은 문제: v1/v2 클래스명 충돌

구현 도중 전체 테스트 스위트를 돌렸다가 앱 컨텍스트 자체가 안 뜨는 버그를 발견했다. Spring은 `@Component` 계열 빈의 기본 이름을 패키지 없이 단순 클래스명으로만 정하는데, v1/v2가 대칭성을 위해 같은 클래스명(`CouponEventAdminController` 등)을 쓰고 있었던 게 원인이었다.

```mermaid
flowchart LR
    subgraph Before["문제: 클래스명 충돌"]
        B1["jh.couponevent.coupon.api<br/>CouponEventAdminController"]
        B2["jh.couponevent.coupon.redis.api<br/>CouponEventAdminController"]
        B1 -."동일한 단순 클래스명<br/>→ Spring 기본 빈 이름도 동일".-> B2
        B2 --> BErr["ConflictingBeanDefinitionException<br/>(컨텍스트 시작 자체가 실패)"]
    end
    subgraph After["해결: v2에 V2 접미사"]
        A1["CouponEventAdminController (v1)"]
        A2["CouponEventAdminControllerV2 (v2)"]
        A1 -."빈 이름이 서로 다름".-> A2
        A2 --> AOk["정상 기동"]
    end
```

v1은 그대로 두고 v2의 `@Service`/`@RestController` 클래스만 `V2` 접미사로 리네임해서 해결했다(`CouponEventAdminControllerV2`, `CouponEventAdminServiceV2`, `CouponIssueServiceV2`, `CouponIssueControllerV2`). 계획서에도 이 사고 경위를 amendment로 남겨뒀다.

## 부하테스트: Phase 1 대비 어디가 나아지고 어디가 병목인지

```mermaid
flowchart LR
    subgraph P1["Phase 1 (목표 ~100 TPS)"]
        direction TB
        P1Bot["병목: MySQL row 락 대기열"]
        P1Num["실측 97.77 req/s<br/>p95 1.04s<br/>dropped 67건"]
    end
    subgraph P2["Phase 2 (목표 ~1,000 TPS)"]
        direction TB
        P2Bot["병목(가설): Tomcat/HikariCP<br/>기본 풀 크기"]
        P2Num["실측 578.70 req/s<br/>p95 4.8s<br/>dropped 11,065건"]
    end
    P1Num -. "절대 처리량 약 5.9배 개선<br/>(락 경합은 사라짐)" .-> P2Num
```

Redis Lua 자체는 MySQL row 락 경합을 없앴지만, 요청마다 여전히 동기적으로 MySQL SELECT+INSERT를 거치고 앱이 Tomcat/HikariCP 기본 풀 크기(스레드 200, 커넥션 10)인 단일 인스턴스로 떠 있어서 1,000 TPS 목표에는 못 미쳤다(실측 578.70 req/s, 목표 대비 약 58%). 이 원인 분석은 실제로 풀 사용률을 측정해 확인한 게 아니라 정황(에러율 0%, 높은 지연, VU 부족 경고)에 기반한 가설임을 `load-test/README.md`에 명시해뒀다. 자세한 수치는 `load-test/README.md`의 "Phase 2: Redis Lua Script (~1,000 TPS)" 절 참고.

## 최종 검증 결과

- `./gradlew test` 전체 스위트 통과 (단위 + Redis/MySQL 대상 통합 테스트 포함), `--rerun`으로 독립 재검증
- 동시성 통합 테스트: 재고 100개 / 동시 요청 200건 → 정확히 100건 성공, Redis 잔여 재고 0
- k6 부하테스트 (~1,000 TPS 목표, 30초): 실제 처리량 578.70 req/s, p95 4.8s, dropped_iterations 11,065건 — Phase 1 대비 절대 처리량 약 5.9배, 목표 대비는 약 58%
- 10개 Task 각각 구현→리뷰(spec+quality) 통과, 최종 전체 브랜치 리뷰(opus)에서 Critical/Important 0건, "Ready to merge: Yes"

## 다음 단계

Phase 3(Redis 동기 차감 + Kafka 비동기 영속화, ~10,000 TPS)는 이 보고서의 범위 밖이며, Phase 2에서 드러난 "동기 MySQL insert + 기본 풀 크기" 한계를 근거로 별도 계획 문서로 착수한다.

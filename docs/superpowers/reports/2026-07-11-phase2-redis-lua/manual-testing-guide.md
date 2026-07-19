# 수동 테스트 가이드 (Phase 2 — Redis Lua Script)

Phase 1(`/api/v1/...`, MySQL 락)과 Phase 2(`/api/v2/...`, Redis Lua)를 나란히 호출해서 차이를 직접 비교해볼 수 있는 가이드다.

## 사전 준비

```bash
docker compose up -d       # MySQL + Redis 기동
./gradlew bootRun          # 별도 터미널에서 앱 기동 (포트 8080)
```

## 1. v2 이벤트 생성 (재고 3개)

```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"v2-hands-on-test","totalQuantity":3,"startAt":"2020-01-01T00:00:00"}'
```

응답의 `id`를 `EVENT_ID`로 기억해둔다.

## 2. 정상 발급 → 200 SUCCESS

```bash
curl -i -X POST http://localhost:8080/api/v2/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'
```

## 3. 같은 유저로 재요청 → 409 DUPLICATE

```bash
curl -i -X POST http://localhost:8080/api/v2/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'
```

## 4. 재고 소진 → 410 SOLD_OUT

```bash
curl -s -X POST http://localhost:8080/api/v2/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":2}'
curl -s -X POST http://localhost:8080/api/v2/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":3}'
curl -i -X POST http://localhost:8080/api/v2/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":4}'
```

## 5. Redis 안의 실제 키 값을 직접 들여다보기

```bash
docker compose exec redis redis-cli GET coupon:EVENT_ID:stock
docker compose exec redis redis-cli SMEMBERS coupon:EVENT_ID:issued_users
```

발급할 때마다 `stock`이 줄어들고 `issued_users` Set에 userId가 쌓이는 걸 직접 확인할 수 있다.

## 6. 관리자 조회로 issuedQuantity 역산 확인

```bash
curl -s http://localhost:8080/api/v2/admin/coupon-events/EVENT_ID
```

`issuedQuantity`가 3(재고 3개 모두 소진)으로 나오는지 확인한다 — 이 값은 DB 컬럼이 아니라 `totalQuantity - Redis stock`으로 매번 계산된 것이다.

## 7. v1과 v2를 나란히 비교

v1(MySQL 락)으로도 똑같은 순서(1~4단계)를 `/api/v1/...` 경로로 반복해보고, 관리자 조회(`GET /api/v1/admin/coupon-events/{id}`)의 `issuedQuantity`가 DB 컬럼 값 그대로인 것과 v2의 역산 값을 비교해본다.

## 8. 자동화된 테스트 직접 돌려보기

```bash
./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponIssueServiceTest" --info
./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponIssueConcurrencyTest" --info
```

## 9. k6 부하테스트로 Phase 1과 처리량 비교

```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"my-phase2-load-test","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
# 반환된 id로:
EVENT_ID=<위에서 받은 id> k6 run load-test/k6/phase2-issue.js
```

`p95`, `dropped_iterations`가 Phase 1 실측치(p95 1.04s, dropped_iterations 67건)보다 얼마나 개선됐는지 직접 비교해본다.

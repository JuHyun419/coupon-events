# 수동 테스트 가이드 (직접 손으로 확인해보기)

자동화된 테스트(Task 5~8)가 이미 로직을 검증하지만, 실제로 API를 호출하고 락이 걸리는 걸 눈으로 보면 이해가 훨씬 빠르다. 이 문서는 로컬에서 직접 따라 해볼 수 있는 순서를 정리한 것이다.

## 사전 준비

```bash
cd .claude/worktrees/phase1-mysql-lock   # 저장소 루트 기준
docker compose up -d                     # MySQL 기동
./gradlew bootRun                        # 별도 터미널에서 앱 기동 (포트 8080)
```

앱이 뜬 걸 확인:
```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/admin/coupon-events
```

## 1. 재고 3개짜리 테스트 이벤트 만들기

재고를 작게 잡아야 품절(410)까지 금방 확인할 수 있다.

```bash
curl -s -X POST http://localhost:8080/api/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"hands-on-test","totalQuantity":3,"startAt":"2020-01-01T00:00:00"}'
```

응답의 `id`를 `EVENT_ID`로 기억해둔다.

## 2. 정상 발급 → 200 SUCCESS

```bash
curl -i -X POST http://localhost:8080/api/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'
```

`CouponIssueService.issue`의 판정 흐름도([task-5](task-5-issue-service.md))에서 맨 아래 `Save` 분기를 타는 케이스.

## 3. 같은 유저로 재요청 → 409 DUPLICATE

```bash
curl -i -X POST http://localhost:8080/api/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'
```

`existsByCouponEventIdAndUserId` 체크에 걸리는 케이스.

## 4. 재고 소진 → 410 SOLD_OUT

userId 2, 3으로 두 번 더 발급(성공)한 뒤 userId 4로 재요청:

```bash
curl -s -X POST http://localhost:8080/api/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":2}'
curl -s -X POST http://localhost:8080/api/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":3}'
curl -i -X POST http://localhost:8080/api/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":4}'
```

마지막 요청만 `410 SOLD_OUT`.

## 5. 오픈 전 이벤트 → 403 NOT_OPEN_YET

```bash
curl -s -X POST http://localhost:8080/api/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"not-open-yet","totalQuantity":10,"startAt":"2099-01-01T00:00:00"}'
# 반환된 id로:
curl -i -X POST http://localhost:8080/api/coupon-events/NEW_EVENT_ID/issue \
  -H "Content-Type: application/json" -d '{"userId":1}'
```

## 6. 상태 조회 + DB 직접 들여다보기

```bash
curl -s http://localhost:8080/api/coupon-events/EVENT_ID/users/1/status

docker compose exec mysql mysql -ucoupon -pcoupon coupon_event \
  -e "SELECT * FROM coupon_event; SELECT * FROM issued_coupon;"
```

## 7. 락 자체를 직접 느껴보기

애플리케이션 코드 없이 **MySQL 세션 두 개**로 `FOR UPDATE`가 다른 트랜잭션을 어떻게 막는지 직접 체감할 수 있다.

터미널 A:
```bash
docker compose exec mysql mysql -ucoupon -pcoupon coupon_event
```
```sql
START TRANSACTION;
SELECT * FROM coupon_event WHERE id = 1 FOR UPDATE;
-- COMMIT 하지 말고 잠깐 멈춰있기
```

터미널 B (새 탭에서 동시에):
```bash
docker compose exec mysql mysql -ucoupon -pcoupon coupon_event
```
```sql
START TRANSACTION;
SELECT * FROM coupon_event WHERE id = 1 FOR UPDATE;
-- 터미널 A가 COMMIT 하기 전까지 여기서 그대로 멈춰있음(hang)
```

터미널 A에서 `COMMIT;`을 치는 순간 터미널 B가 풀려난다. 이것이 [task-7 동시성 다이어그램](task-7-concurrency-test.md)의 "락 대기열"의 실체다.

## 8. 자동화된 테스트 직접 돌려보기

```bash
./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueServiceTest" --info
./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueConcurrencyTest" --info
```

동시성 테스트는 `--info`로 돌리면 200개 요청 중 성공/실패가 갈리는 걸 로그로 확인할 수 있다.

## 9. k6 부하테스트 직접 돌리고 숫자로 느끼기

```bash
curl -s -X POST http://localhost:8080/api/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"my-load-test","totalQuantity":5000,"startAt":"2020-01-01T00:00:00"}'
# 반환된 id로:
EVENT_ID=<위에서 받은 id> k6 run load-test/k6/phase1-issue.js
```

`p95`, `max`, `dropped_iterations` 숫자가 [task-8 리포트](task-8-load-test.md)와 비슷한 범위로 나오는지 직접 비교해본다.

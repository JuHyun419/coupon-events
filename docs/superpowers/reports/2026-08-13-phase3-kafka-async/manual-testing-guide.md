# 수동 테스트 가이드 (Phase 3 — Redis + Kafka 비동기 영속화)

Phase 2(`/api/v2/...`, Redis 동기 차감 + 동기 DB insert)와 Phase 3(`/api/v3/...`, Redis 동기 차감 + Kafka 비동기 DB 반영)를 나란히 호출해서, "응답은 즉시 오지만 DB 반영은 잠시 후"라는 최종 정합성을 직접 눈으로 확인해보는 가이드다.

## 사전 준비

```bash
docker compose up -d       # MySQL + Redis + Kafka 기동
sleep 5
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic coupon-issue-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092   # 이미 만들어져 있다면 "already exists" 에러가 나는데 무시해도 된다
./gradlew bootRun          # 별도 터미널에서 앱 기동 (포트 8080)
```

앱이 뜬 걸 확인:
```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/v2/admin/coupon-events
```

## 1. v3용 이벤트 생성 (v2 admin API 재사용, 재고 3개)

```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"v3-hands-on-test","totalQuantity":3,"startAt":"2020-01-01T00:00:00"}'
```

응답의 `id`를 `EVENT_ID`로 기억해둔다. v3는 별도 admin API가 없다 — v2로 만든 이벤트를 그대로 v3 경로로 발급한다.

## 2. 발급 요청 직후 상태를 바로 조회해본다 — PENDING을 잡을 수도 있다

```bash
curl -i -X POST http://localhost:8080/api/v3/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'

curl -s http://localhost:8080/api/v3/coupon-events/EVENT_ID/users/1/status
```

로컬 환경은 컨슈머 반영이 매우 빨라서(수십 ms) 이미 `COMPLETED`가 찍혀 있을 수도 있다 — 그 자체가 "동기 응답과 비동기 반영 사이의 시간차가 아주 짧을 뿐 여전히 존재한다"는 걸 보여준다. `PENDING`을 안정적으로 관찰하려면 3번처럼 앱을 잠시 멈춰본다.

## 3. 컨슈머를 잠시 멈춰서 PENDING을 확실히 관찰하기

별도 터미널에서 앱을 잠깐 멈춘 뒤(`Ctrl+C`) 요청을 보내면, Redis 차감/Kafka 발행은 이미 끝났는데 컨슈머가 없어 DB 반영만 멈춘 상태를 관찰할 수 있다. 앱을 다시 켜면(`./gradlew bootRun`) 컨슈머가 재기동되면서 밀려있던 메시지를 마저 처리해 `COMPLETED`로 바뀐다.

## 4. 같은 유저로 재요청 → 409 DUPLICATE

```bash
curl -i -X POST http://localhost:8080/api/v3/coupon-events/EVENT_ID/issue \
  -H "Content-Type: application/json" \
  -d '{"userId":1}'
```

## 5. 재고 소진 → 410 SOLD_OUT

```bash
curl -s -X POST http://localhost:8080/api/v3/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":2}'
curl -s -X POST http://localhost:8080/api/v3/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":3}'
curl -i -X POST http://localhost:8080/api/v3/coupon-events/EVENT_ID/issue -H "Content-Type: application/json" -d '{"userId":4}'
```

## 6. Kafka 토픽에 실제로 쌓인 메시지를 눈으로 확인하기

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic coupon-issue-events --from-beginning --bootstrap-server localhost:9092 --max-messages 5
```

`{eventId, userId, issuedAt}` 형태의 JSON 메시지가 파티션 키(`userId`)와 함께 쌓여 있는 걸 확인할 수 있다.

## 7. v2와 v3를 나란히 비교

v2(`/api/v2/...`)로 동일한 이벤트를 만들어 발급 직후 관리자 조회(`issuedQuantity`)와 v3의 상태 조회(`status`)가 반영되는 타이밍 차이를 비교해본다 — v2는 응답이 온 시점에 이미 DB에도 반영되어 있지만, v3는 응답은 왔어도 DB 반영은 컨슈머가 배치를 처리할 때까지 지연될 수 있다.

## 8. 자동화된 테스트 직접 돌려보기

```bash
./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueServiceV3Test" --info
./gradlew test --tests "jh.couponevent.coupon.kafka.consumer.IssuedCouponBatchConsumerTest" --info
./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueConcurrencyV3Test" --info
```

## 9. k6 부하테스트로 Phase 2와 처리량/consumer lag 비교

```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"my-phase3-load-test","totalQuantity":500000,"startAt":"2020-01-01T00:00:00"}'
# 반환된 id로:
EVENT_ID=<위에서 받은 id> k6 run load-test/k6/phase3-issue.js
```

k6 실행이 끝난 직후 `curl -s http://localhost:8080/api/v2/admin/coupon-events/EVENT_ID`로 확인한 `issuedQuantity`(Redis 기준 확정치)와, MySQL의 `SELECT COUNT(*) FROM issued_coupon WHERE coupon_event_id = EVENT_ID`(실제 DB 반영치)가 같아질 때까지 몇 초나 걸리는지 직접 재본다 — 이게 consumer lag이다.

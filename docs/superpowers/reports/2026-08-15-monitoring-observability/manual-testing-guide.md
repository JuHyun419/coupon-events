# 수동 테스트 가이드 (모니터링 옵저버빌리티)

Prometheus + Grafana로 앱의 내부 상태(HikariCP/Tomcat 풀, HTTP 처리량, Kafka lag, JVM)를 직접 들여다보는 가이드다.

## 1. 전체 스택 기동

```bash
docker compose up -d --build
sleep 30
docker compose ps
```

`mysql`/`redis`/`kafka`가 `healthy`, `app`/`prometheus`/`kafka-exporter`/`grafana`가 `running`인지 확인한다.

## 2. Grafana 접속

브라우저로 `http://localhost:3000` 접속(로그인 없이 바로 들어가짐 — 로컬 전용 익명 Admin 설정). 좌측 메뉴에서 `coupon-event` 폴더를 열면 대시보드 3개가 보인다:

- **풀 병목 검증**: HikariCP 커넥션 풀, Tomcat 스레드 풀, 전체 HTTP req/s
- **API 버전 비교**: v1/v2/v3별 처리량/p95 지연
- **Kafka Lag & JVM**: 컨슈머 랙, CPU, 힙 메모리

## 3. Prometheus에서 직접 쿼리해보기

브라우저로 `http://localhost:9090` 접속 후 쿼리창에 다음을 입력해본다:

```promql
hikaricp_connections_active
tomcat_threads_busy_threads
kafka_consumergroup_lag
rate(http_server_requests_seconds_count[1m])
```

## 4. 부하를 걸면서 대시보드 변화 관찰

```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"manual-monitoring-test","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
# 반환된 id로:
EVENT_ID=<위에서 받은 id> k6 run load-test/k6/phase2-issue.js
```

k6가 돌아가는 동안 "풀 병목 검증" 대시보드를 계속 보면서 HikariCP `pending`이나 Tomcat `busy`가 튀는 순간이 있는지 직접 확인한다. `load-test/k6/phase3-issue.js`로도 동일하게 반복해서 Phase 2와 Phase 3의 풀 사용 패턴을 비교해볼 수 있다.

## 5. Kafka lag이 실제로 쌓였다 빠지는 걸 관찰하기

Phase 3 부하테스트(`phase3-issue.js`) 실행 중 "Kafka Lag & JVM" 대시보드를 보면, 유입 속도가 컨슈머 처리 속도를 앞지르는 순간 lag이 잠깐 쌓였다가 부하가 끝나면 다시 0으로 떨어지는 걸 확인할 수 있다(Phase 3 부하테스트에서 관찰된 "처리량이 낮아 lag이 거의 없었다"는 가설을 이 대시보드로 직접 반박하거나 재확인해볼 수 있다).

## 6. 정리

```bash
docker compose down
```

(`docker-compose.yml`에 `mysql-data` 볼륨만 정의되어 있어 DB 데이터는 유지되고, Prometheus/Grafana 데이터는 컨테이너와 함께 사라진다 — 로컬 학습 환경이므로 의도된 동작이다.)

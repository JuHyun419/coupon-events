# 모니터링 옵저버빌리티 (Prometheus + Grafana) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spring Boot Actuator + Micrometer + Prometheus + Grafana로 옵저버빌리티 스택을 구축해, Phase 2/3 부하테스트 보고서에 "가설"로만 남아있던 Tomcat/HikariCP 풀 병목 원인을 실측으로 검증한다.

**Architecture:** 앱에 Actuator/Micrometer를 붙여 `/actuator/prometheus`로 메트릭을 노출하고, 앱 자체도 Dockerfile로 컨테이너화해 `docker-compose`에 합류시킨다(기존 `./gradlew bootRun` 워크플로는 그대로 유지 — Spring profile로 분리). Prometheus가 앱과 kafka_exporter를 스크레이핑하고, Grafana가 provisioning으로 대시보드 3개를 자동 로드한다. 마지막 태스크에서 실제 k6 부하테스트를 돌리며 대시보드를 관찰해 병목 가설을 검증하고 결과를 문서화한다.

**Tech Stack:** Spring Boot Actuator, Micrometer(`micrometer-registry-prometheus`), Prometheus, Grafana(provisioning), `danielqsj/kafka-exporter`, Docker(멀티스테이지 빌드), docker-compose.

**Spec:** `docs/superpowers/specs/2026-08-15-monitoring-observability-design.md`

## Global Constraints

- 참조 스펙: `docs/superpowers/specs/2026-08-15-monitoring-observability-design.md`
- nGrinder 등 부하테스트 도구는 이 계획의 범위 밖이다 — 관찰용 인프라(Prometheus/Grafana)만 다룬다.
- 기존 `./gradlew bootRun` 개발 워크플로는 그대로 유지한다 — `application.yml`의 `localhost` 접속 정보를 바꾸지 않는다. 컨테이너 전용 설정은 새 `application-docker.yml` profile로 분리한다.
- 새 docker-compose 서비스: `app`, `prometheus`, `grafana`, `kafka-exporter`. 기존 `mysql`/`redis`/`kafka` 서비스는 헬스체크 추가 외에는 건드리지 않는다.
- Grafana 대시보드는 UI에서 수동으로 만들지 않는다 — JSON을 리포에 커밋하고 provisioning으로 자동 로드되게 한다(`docker compose up`만으로 재현 가능해야 함).
- 클라우드 전용 배포 자동화(Terraform, K8s manifest)는 다루지 않는다 — docker-compose로 로컬/클라우드 VM 어디서든 동일하게 뜨는 것으로 충분하다.
- Alertmanager, MySQL/Redis exporter는 다루지 않는다(YAGNI, 스펙 7장 "범위 밖" 참고).

---

### Task 1: Actuator + Micrometer Prometheus 노출 (HikariCP/HTTP/JVM 자동 계측)

**Files:**
- Modify: `build.gradle.kts`
- Modify: `src/main/resources/application.yml`

**Interfaces:**
- Consumes: 없음 (최초 작업)
- Produces: `/actuator/prometheus` 엔드포인트(이후 모든 Task가 이걸 스크레이핑 대상으로 사용), `http_server_requests_seconds_bucket`(히스토그램, Task 9의 p95/p99 쿼리가 사용)

- [ ] **Step 1: `build.gradle.kts`에 Actuator/Micrometer 의존성을 추가한다**

`dependencies` 블록의 `implementation("org.springframework.boot:spring-boot-starter-validation")` 다음 줄에 추가:

```kotlin
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
```

- [ ] **Step 2: `application.yml`에 management/metrics 설정을 추가한다**

파일 끝(`kafka:` 블록 다음, 최상위 `spring:` 블록이 끝난 뒤)에 추가:

```yaml

management:
  endpoints:
    web:
      exposure:
        include: prometheus,health
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        http.server.requests: true
```

(`percentiles-histogram`을 켜야 `http_server_requests_seconds_bucket`이 노출되어 Task 9에서 `histogram_quantile`로 p95/p99를 계산할 수 있다.)

- [ ] **Step 3: 앱을 띄우고 메트릭이 실제로 노출되는지 확인한다**

Run:
```bash
docker compose up -d
./gradlew bootRun &
sleep 15
curl -s http://localhost:8080/actuator/prometheus | grep -E "^hikaricp_connections_active|^process_cpu_usage|^jvm_memory_used_bytes"
kill %1
```
Expected: 세 메트릭 모두 최소 한 줄씩 출력된다(HikariCP는 앱이 뜨면서 커넥션 풀을 초기화하므로 값이 잡힘).

- [ ] **Step 4: 커밋**

```bash
git add build.gradle.kts src/main/resources/application.yml
git commit -m "Add Actuator + Micrometer Prometheus metrics exposure"
```

---

### Task 2: Tomcat 스레드 풀 메트릭 노출

**Files:**
- Modify: `src/main/resources/application.yml`

**Interfaces:**
- Consumes: Task 1의 `/actuator/prometheus` 엔드포인트
- Produces: `tomcat_threads_busy_threads`, `tomcat_threads_config_max_threads` 메트릭(Task 8의 풀 병목 대시보드가 사용)

- [ ] **Step 1: `application.yml`에 Tomcat MBean 레지스트리 설정을 추가한다**

`management:` 블록 앞(최상위 레벨)에 추가:

```yaml
server:
  tomcat:
    mbeanregistry:
      enabled: true
```

- [ ] **Step 2: 앱을 띄우고 Tomcat 스레드 메트릭이 노출되는지 확인한다**

Run:
```bash
./gradlew bootRun &
sleep 15
curl -s http://localhost:8080/actuator/prometheus | grep -E "^tomcat_threads_busy_threads|^tomcat_threads_config_max_threads"
kill %1
```
Expected: 두 메트릭 모두 출력된다(`tomcat_threads_config_max_threads`는 기본값 200).

- [ ] **Step 3: 커밋**

```bash
git add src/main/resources/application.yml
git commit -m "Expose Tomcat thread pool metrics via MBean registry"
```

---

### Task 3: Docker profile 분리 (`application-docker.yml`)

**Files:**
- Create: `src/main/resources/application-docker.yml`

**Interfaces:**
- Consumes: 없음
- Produces: `docker` Spring profile — Task 4의 `app` 컨테이너가 `SPRING_PROFILES_ACTIVE=docker`로 이 설정을 오버라이드해서 씀

- [ ] **Step 1: 컨테이너 전용 접속 정보 파일을 작성한다**

```yaml
spring:
  datasource:
    url: jdbc:mysql://mysql:3306/coupon_event?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
  data:
    redis:
      host: redis
  kafka:
    bootstrap-servers: kafka:9092
```

(`application.yml`의 `localhost`/`3306`/`6379`/`9092`를 docker-compose 서비스명으로만 오버라이드한다. 나머지 설정 — Kafka producer/consumer 옵션, JPA, Actuator 등 — 은 `application.yml`에서 그대로 상속된다.)

- [ ] **Step 2: YAML 문법이 올바른지 확인한다**

Run: `./gradlew compileKotlin` (리소스 파일 문법 오류는 부팅 시점에나 드러나므로, 최종 검증은 Task 4에서 실제 컨테이너로 기동해서 확인한다)
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: 커밋**

```bash
git add src/main/resources/application-docker.yml
git commit -m "Add docker Spring profile for container-internal hostnames"
```

---

### Task 4: 앱 컨테이너화 (Dockerfile + docker-compose app 서비스)

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 3의 `docker` profile
- Produces: docker-compose `app` 서비스(호스트 포트 8080) — Task 5의 Prometheus가 컨테이너 네트워크 안에서 `app:8080`으로 스크레이핑

- [ ] **Step 1: 멀티스테이지 `Dockerfile`을 작성한다**

```dockerfile
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app
COPY gradlew gradlew.bat ./
COPY gradle gradle
COPY build.gradle.kts settings.gradle.kts ./
COPY src src
RUN chmod +x gradlew && ./gradlew bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 2: `.dockerignore`를 작성해 빌드 컨텍스트를 줄인다**

```
.git
.gradle
build
.superpowers
docs
load-test
*.md
```

- [ ] **Step 3: `docker-compose.yml`의 `mysql`/`redis`/`kafka` 서비스에 헬스체크를 추가한다**

`mysql:` 서비스의 `volumes:` 다음 줄에 추가:

```yaml
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-proot"]
      interval: 5s
      timeout: 5s
      retries: 10
```

`redis:` 서비스의 `ports:` 다음 줄에 추가:

```yaml
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10
```

`kafka:` 서비스의 `environment:` 블록 마지막 줄(`CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw`) 다음, 즉 `kafka:` 서비스 블록이 끝나는 지점(다음 서비스가 시작되기 전 빈 줄 앞)에 추가:

```yaml
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
```

- [ ] **Step 4: `docker-compose.yml`에 `app` 서비스를 추가한다**

`volumes:` 최상위 블록 앞에 추가:

```yaml
  app:
    build: .
    container_name: coupon-event-app
    environment:
      SPRING_PROFILES_ACTIVE: docker
    ports:
      - "8080:8080"
    depends_on:
      mysql:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
```

- [ ] **Step 5: 전체 스택을 빌드하고 기동해 앱이 정상 응답하는지 확인한다**

Run:
```bash
docker compose up -d --build
sleep 30
docker compose ps
curl -s http://localhost:8080/actuator/health
```
Expected: `docker compose ps`에서 `mysql`/`redis`/`kafka`가 `healthy`, `app`이 `running`. `curl`은 `{"status":"UP"}` 반환.

만약 `app`이 재시작을 반복하면 `docker compose logs app`으로 원인을 확인한다(주로 healthcheck 대기 전에 접속을 시도하는 타이밍 문제이며, healthcheck가 `condition: service_healthy`로 걸려있으므로 정상적으로는 재현되지 않아야 한다).

- [ ] **Step 6: 커밋**

```bash
git add Dockerfile .dockerignore docker-compose.yml
git commit -m "Containerize the app and add healthchecks to infra services"
```

---

### Task 5: Prometheus 서비스 + 스크레이핑 설정

**Files:**
- Create: `docker/prometheus/prometheus.yml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 4의 `app` 서비스(`app:8080/actuator/prometheus`)
- Produces: Prometheus 서비스(호스트 포트 9090) — Task 7의 Grafana가 datasource로 사용

- [ ] **Step 1: Prometheus 스크레이핑 설정 파일을 작성한다**

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "coupon-event-app"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["app:8080"]

  - job_name: "kafka-exporter"
    static_configs:
      - targets: ["kafka-exporter:9308"]
```

(`kafka-exporter` 타깃은 Task 6에서 서비스가 추가되기 전까지는 `DOWN`으로 표시되는데, 이는 정상이다 — Prometheus는 존재하지 않는 타깃도 설정에 있으면 그냥 `DOWN`으로 표시할 뿐 에러를 내지 않는다.)

- [ ] **Step 2: `docker-compose.yml`에 `prometheus` 서비스를 추가한다**

`app:` 서비스 블록 다음에 추가:

```yaml
  prometheus:
    image: prom/prometheus:latest
    container_name: coupon-event-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      - app
```

- [ ] **Step 3: Prometheus를 기동하고 app 타깃이 UP인지 확인한다**

Run:
```bash
docker compose up -d prometheus
sleep 10
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[a-z]*"'
curl -s 'http://localhost:9090/api/v1/query?query=hikaricp_connections_active' | grep -o '"status":"success"'
```
Expected: `coupon-event-app` 타깃이 `"health":"up"`, `hikaricp_connections_active` 쿼리가 `"status":"success"`와 함께 값을 반환.

- [ ] **Step 4: 커밋**

```bash
git add docker/prometheus/prometheus.yml docker-compose.yml
git commit -m "Add Prometheus service scraping app metrics"
```

---

### Task 6: kafka-exporter 서비스 (Kafka consumer lag)

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 4에서 이미 healthcheck가 붙은 기존 `kafka` 서비스
- Produces: kafka-exporter 서비스(호스트 포트 9308), Task 5의 Prometheus 설정에 이미 등록된 `kafka-exporter:9308` 타깃이 이제 실제로 UP됨. `kafka_consumergroup_lag` 메트릭(Task 10의 대시보드가 사용)

- [ ] **Step 1: `docker-compose.yml`에 `kafka-exporter` 서비스를 추가한다**

`prometheus:` 서비스 블록 다음에 추가:

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: coupon-event-kafka-exporter
    command: ["--kafka.server=kafka:9092"]
    ports:
      - "9308:9308"
    depends_on:
      kafka:
        condition: service_healthy
```

- [ ] **Step 2: 기동하고 Prometheus 타깃이 UP으로 바뀌는지, lag 메트릭이 노출되는지 확인한다**

Run:
```bash
docker compose up -d kafka-exporter
sleep 15
curl -s http://localhost:9308/metrics | grep "^kafka_consumergroup_lag" | head -5
curl -s http://localhost:9090/api/v1/targets | python3 -c "import json,sys; d=json.load(sys.stdin); print([t['health'] for t in d['data']['activeTargets'] if t['labels']['job']=='kafka-exporter'])"
```
Expected: `kafka_consumergroup_lag` 라인이 최소 몇 개 출력(`coupon-issue-consumer` 그룹의 파티션별 lag). Prometheus 타깃 상태는 `['up']`.

- [ ] **Step 3: 커밋**

```bash
git add docker-compose.yml
git commit -m "Add kafka-exporter service for consumer lag metrics"
```

---

### Task 7: Grafana 서비스 + Datasource Provisioning

**Files:**
- Create: `docker/grafana/provisioning/datasources/prometheus.yml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 5의 Prometheus 서비스
- Produces: Grafana 서비스(호스트 포트 3000), uid가 `prometheus`로 고정된 datasource — Task 8/9/10의 대시보드 JSON이 이 uid를 참조

- [ ] **Step 1: Datasource provisioning 파일을 작성한다**

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 2: `docker-compose.yml`에 `grafana` 서비스를 추가한다**

`kafka-exporter:` 서비스 블록 다음에 추가:

```yaml
  grafana:
    image: grafana/grafana:latest
    container_name: coupon-event-grafana
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: "Admin"
    volumes:
      - ./docker/grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - prometheus
```

(`GF_AUTH_ANONYMOUS_ENABLED`는 로컬 학습 환경에서 로그인 없이 바로 대시보드를 보기 위한 설정이다 — 운영 환경이라면 절대 쓰면 안 되지만, 이 프로젝트는 로컬 전용이므로 편의를 우선한다.)

- [ ] **Step 3: 기동하고 datasource가 자동 등록됐는지 확인한다**

Run:
```bash
docker compose up -d grafana
sleep 15
curl -s http://localhost:3000/api/datasources | grep -o '"uid":"prometheus"'
```
Expected: `"uid":"prometheus"` 출력(익명 Admin 권한으로 API 호출 가능).

- [ ] **Step 4: 커밋**

```bash
git add docker/grafana/provisioning/datasources/prometheus.yml docker-compose.yml
git commit -m "Add Grafana service with Prometheus datasource provisioning"
```

---

### Task 8: 대시보드 1 — 풀 병목 검증 (HikariCP + Tomcat + HTTP 처리량)

**Files:**
- Create: `docker/grafana/provisioning/dashboards/dashboards.yml`
- Create: `docker/grafana/provisioning/dashboards/pool-bottleneck.json`

**Interfaces:**
- Consumes: Task 1의 `hikaricp_connections_*`, Task 2의 `tomcat_threads_*`, Task 1의 `http_server_requests_seconds_count`, Task 7의 datasource uid `prometheus`
- Produces: `docker/grafana/provisioning/dashboards/` 디렉토리(Task 9/10이 같은 디렉토리에 JSON을 추가)

- [ ] **Step 1: 대시보드 provider 설정 파일을 작성한다 (한 번만 필요, Task 9/10도 이 파일을 공유)**

```yaml
apiVersion: 1

providers:
  - name: coupon-event
    orgId: 1
    folder: "coupon-event"
    type: file
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/provisioning/dashboards
```

- [ ] **Step 2: 풀 병목 검증 대시보드 JSON을 작성한다**

```json
{
  "uid": "pool-bottleneck",
  "title": "coupon-event: 풀 병목 검증",
  "tags": ["coupon-event"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "HikariCP Connections",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "hikaricp_connections_active", "legendFormat": "active", "refId": "A" },
        { "expr": "hikaricp_connections_idle", "legendFormat": "idle", "refId": "B" },
        { "expr": "hikaricp_connections_pending", "legendFormat": "pending (대기 중인 요청)", "refId": "C" },
        { "expr": "hikaricp_connections_max", "legendFormat": "max (풀 크기)", "refId": "D" }
      ]
    },
    {
      "id": 2,
      "title": "Tomcat Threads",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "tomcat_threads_busy_threads", "legendFormat": "busy", "refId": "A" },
        { "expr": "tomcat_threads_config_max_threads", "legendFormat": "max (풀 크기)", "refId": "B" }
      ]
    },
    {
      "id": 3,
      "title": "전체 HTTP 처리량 (req/s)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "sum(rate(http_server_requests_seconds_count[1m]))", "legendFormat": "req/s", "refId": "A" }
      ]
    }
  ]
}
```

- [ ] **Step 3: Grafana를 재기동해 대시보드가 로드됐는지 확인한다**

Run:
```bash
docker compose restart grafana
sleep 15
curl -s http://localhost:3000/api/search?query=풀%20병목 | grep -o '"uid":"pool-bottleneck"'
```
Expected: `"uid":"pool-bottleneck"` 출력. 브라우저로 `http://localhost:3000/d/pool-bottleneck`에 접속하면 패널 3개가 보인다(이 시점엔 트래픽이 없어 그래프는 평평하다 — 정상. Task 11에서 실제 부하를 걸어 값 변화를 관찰한다).

- [ ] **Step 4: 커밋**

```bash
git add docker/grafana/provisioning/dashboards/dashboards.yml docker/grafana/provisioning/dashboards/pool-bottleneck.json
git commit -m "Add pool-bottleneck Grafana dashboard (HikariCP + Tomcat + throughput)"
```

---

### Task 9: 대시보드 2 — API 버전 비교 (v1/v2/v3 처리량 & 지연)

**Files:**
- Create: `docker/grafana/provisioning/dashboards/api-version-comparison.json`

**Interfaces:**
- Consumes: Task 1의 `http_server_requests_seconds_count`/`http_server_requests_seconds_bucket`(uri 태그로 v1/v2/v3 구분), Task 7의 datasource uid `prometheus`, Task 8의 `dashboards.yml` provider(재사용, 수정 불필요)
- Produces: 없음 (대시보드 산출물)

- [ ] **Step 1: API 버전 비교 대시보드 JSON을 작성한다**

```json
{
  "uid": "api-version-comparison",
  "title": "coupon-event: API 버전 비교 (v1/v2/v3)",
  "tags": ["coupon-event"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "버전별 처리량 (req/s)",
      "type": "timeseries",
      "gridPos": { "h": 9, "w": 24, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (version) (label_replace(rate(http_server_requests_seconds_count{uri=~\"/api/v[0-9]/.*\"}[1m]), \"version\", \"$1\", \"uri\", \"/api/(v[0-9])/.*\"))",
          "legendFormat": "{{version}}",
          "refId": "A"
        }
      ]
    },
    {
      "id": 2,
      "title": "버전별 p95 지연 (초)",
      "type": "timeseries",
      "gridPos": { "h": 9, "w": 24, "x": 0, "y": 9 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by (le, version) (label_replace(rate(http_server_requests_seconds_bucket{uri=~\"/api/v[0-9]/.*\"}[1m]), \"version\", \"$1\", \"uri\", \"/api/(v[0-9])/.*\")))",
          "legendFormat": "p95 {{version}}",
          "refId": "A"
        }
      ]
    }
  ]
}
```

(`label_replace`로 `uri` 태그(예: `/api/v2/coupon-events/{eventId}/issue`)에서 `version` 태그(`v2`)를 뽑아내 `sum by (version)`으로 묶는다 — v1/v2/v3 엔드포인트 경로가 이미 `/api/v{n}/...`로 구분되어 있어 코드 변경 없이 쿼리만으로 그룹화된다.)

- [ ] **Step 2: Grafana를 재기동해 대시보드가 로드됐는지 확인한다**

Run:
```bash
docker compose restart grafana
sleep 15
curl -s http://localhost:3000/api/search?query=버전%20비교 | grep -o '"uid":"api-version-comparison"'
```
Expected: `"uid":"api-version-comparison"` 출력.

- [ ] **Step 3: 커밋**

```bash
git add docker/grafana/provisioning/dashboards/api-version-comparison.json
git commit -m "Add API version comparison Grafana dashboard"
```

---

### Task 10: 대시보드 3 — Kafka & JVM 리소스

**Files:**
- Create: `docker/grafana/provisioning/dashboards/kafka-jvm.json`

**Interfaces:**
- Consumes: Task 6의 `kafka_consumergroup_lag`, Task 1의 `process_cpu_usage`/`jvm_memory_used_bytes`/`jvm_gc_pause_seconds_count`, Task 7의 datasource uid `prometheus`, Task 8의 `dashboards.yml` provider(재사용, 수정 불필요)
- Produces: 없음 (대시보드 산출물)

- [ ] **Step 1: Kafka & JVM 대시보드 JSON을 작성한다**

```json
{
  "uid": "kafka-jvm",
  "title": "coupon-event: Kafka Lag & JVM",
  "tags": ["coupon-event"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "Kafka Consumer Lag (파티션별)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "kafka_consumergroup_lag{consumergroup=\"coupon-issue-consumer\"}",
          "legendFormat": "partition {{partition}}",
          "refId": "A"
        }
      ]
    },
    {
      "id": 2,
      "title": "CPU 사용률",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "process_cpu_usage", "legendFormat": "cpu usage (0-1)", "refId": "A" }
      ]
    },
    {
      "id": 3,
      "title": "JVM Heap 메모리",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "sum(jvm_memory_used_bytes{area=\"heap\"})", "legendFormat": "heap used", "refId": "A" }
      ]
    }
  ]
}
```

- [ ] **Step 2: Grafana를 재기동해 대시보드가 로드됐는지 확인한다**

Run:
```bash
docker compose restart grafana
sleep 15
curl -s http://localhost:3000/api/search?query=Kafka | grep -o '"uid":"kafka-jvm"'
```
Expected: `"uid":"kafka-jvm"` 출력.

- [ ] **Step 3: 커밋**

```bash
git add docker/grafana/provisioning/dashboards/kafka-jvm.json
git commit -m "Add Kafka lag and JVM resource Grafana dashboard"
```

---

### Task 11: 실제 부하테스트로 병목 가설 검증

**Files:**
- Modify: `load-test/README.md`

**Interfaces:**
- Consumes: Task 8의 풀 병목 검증 대시보드, 기존 `load-test/k6/phase2-issue.js`/`phase3-issue.js`
- Produces: 없음 (실측/관찰용 산출물)

- [ ] **Step 1: 전체 스택이 떠 있는지 확인한다**

Run:
```bash
docker compose up -d --build
sleep 30
docker compose ps
```
Expected: 모든 서비스(`mysql`, `redis`, `kafka`, `app`, `prometheus`, `kafka-exporter`, `grafana`) `running`/`healthy`.

- [ ] **Step 2: 부하테스트용 v2 이벤트를 생성한다**

Run:
```bash
curl -s -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"monitoring-verification","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
```
Expected: `201 Created`, 응답의 `id`를 `EVENT_ID`로 기억해둔다.

- [ ] **Step 3: 브라우저로 `http://localhost:3000/d/pool-bottleneck`을 열어둔다**

k6 실행 전에 미리 대시보드를 띄워둬야 실행 중 변화를 실시간으로 관찰할 수 있다.

- [ ] **Step 4: k6 부하테스트를 실행하면서 대시보드를 관찰한다**

Run: `EVENT_ID=<Step 2에서 확인한 id> k6 run load-test/k6/phase2-issue.js`

실행되는 30초 동안 대시보드에서 다음을 관찰하고 기록한다:
- HikariCP `pending`(대기 중인 요청)이 0보다 커지는 시점이 있는지, `active`가 `max`에 붙어있는 시간이 있는지
- Tomcat `busy`가 `max`(200)에 근접하거나 도달하는지
- 이 시점들이 k6의 응답 지연 급증 구간과 겹치는지

- [ ] **Step 5: 관찰 결과를 `load-test/README.md`에 반영한다**

`load-test/README.md`의 "Phase 2: Redis Lua Script (~1,000 TPS)" 절의 결론 문단 끝(`...다음 개선 지점으로 남는다.` 다음)에 추가:

```markdown

**모니터링으로 실측 검증 (2026-08-15)**: Prometheus + Grafana로 HikariCP/Tomcat 풀 사용률을 직접 관찰하며 위 k6 시나리오를 재실행했다. [Step 4에서 실제로 관찰한 내용을 여기 기록 — 예: "HikariCP active가 커넥션 풀 크기(10)에 붙어 pending이 0보다 커지는 구간이 관찰됐다" 또는 반대로 "풀은 여유가 있었고 Tomcat busy threads가 병목이었다" 등, 실제로 본 그대로를 적는다]. 이로써 위 문단의 가설은 [확인됨 / 다른 원인으로 정정됨 — 관찰한 그대로 기술].
```

(이 Step은 실제로 관찰한 내용을 그대로 옮겨 적는 것이 핵심이다 — 미리 정해진 결론을 쓰지 않는다. Phase 1/2/3 보고서 전체의 원칙과 동일하게, 실측되지 않은 인과관계는 가설로 하지 확정된 사실로 쓰지 않는다.)

- [ ] **Step 6: 커밋**

```bash
git add load-test/README.md
git commit -m "Record monitoring-verified pool bottleneck observations"
```

---

### Task 12: 손으로 확인하는 수동 가이드 작성

**Files:**
- Create: `docs/superpowers/reports/2026-08-15-monitoring-observability/manual-testing-guide.md`

**Interfaces:**
- Consumes: 앞선 모든 Task의 서비스/대시보드
- Produces: 없음 (문서 산출물)

- [ ] **Step 1: 가이드 문서를 작성한다**

```markdown
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
```

- [ ] **Step 2: 커밋**

```bash
git add docs/superpowers/reports/2026-08-15-monitoring-observability/manual-testing-guide.md
git commit -m "Add manual testing guide for monitoring observability stack"
```

---

## 완료 후 다음 단계

이 계획이 끝나면 Prometheus + Grafana로 Phase 2/3의 풀 병목 가설을 실측으로 검증한 결과가 `load-test/README.md`에 남는다. nGrinder 등 부하테스트 도구 자체를 다루는 작업은 이 계획의 범위 밖이며, 필요해지면 별도로 브레인스토밍해서 시작한다.

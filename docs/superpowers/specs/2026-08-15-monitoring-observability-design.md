# 모니터링 옵저버빌리티 (Prometheus + Grafana) 설계

- 작성일: 2026-08-15
- 상위 문서: `docs/superpowers/specs/2026-07-04-coupon-event-design.md` (10장 "범위 밖" — "정교한 모니터링 대시보드(Grafana 등, 후속 과제로 남김)"가 이 문서의 출발점)
- 관련 문서: `load-test/README.md`(Phase 2/3 실측 결과 — 여기 남긴 "가설"을 이 작업으로 검증한다)

## 0. 이 작업의 목적

Phase 2/3 부하테스트 보고서에는 반복해서 같은 문구가 등장한다: *"Tomcat/HikariCP 기본 풀 크기가 병목의 유력한 원인으로 추정되지만, 이번 실측에서 스레드 풀/커넥션 풀 사용률을 직접 측정하지는 않았으므로 확인된 사실이 아니라 가설이다."* 이 작업의 1차 목적은 이 가설을 실측으로 검증하는 것이다. Prometheus + Grafana로 HikariCP/Tomcat 풀 사용률, HTTP 요청률/지연(v1/v2/v3별), Kafka consumer lag, JVM 리소스를 시계열로 관찰 가능하게 만들고, k6 부하테스트를 돌리면서 실제로 풀이 고갈되는 시점과 지연이 급증하는 시점이 일치하는지 직접 눈으로 확인한다.

**범위 분리**: 부하테스트 도구(nGrinder 등)는 이 문서의 범위 밖이다. Prometheus/Grafana 관찰용 인프라만 다룬다.

## 1. 아키텍처

```
docker-compose 서비스:
  기존: mysql, redis, kafka
  신규: app(컨테이너화), prometheus, grafana, kafka_exporter

app → /actuator/prometheus 노출
prometheus → app, kafka_exporter를 스크레이핑
grafana → prometheus를 datasource로, provisioning으로 대시보드 3개 자동 로드
```

앱 실행은 두 가지 방식을 모두 지원한다:
- **`./gradlew bootRun`** — 기존 개발 워크플로 그대로(빠른 재시작). MySQL/Redis/Kafka가 호스트에 포트를 열어두므로 `localhost`로 접속 가능.
- **`docker compose up`** — 앱까지 포함해 전체 스택을 컨테이너로 띄움(모니터링/부하테스트 세션용). 이미지 빌드가 필요해 재시작이 느리지만 환경이 하나로 통일된다.

두 모드를 가르기 위해 Spring profile을 분리한다: 기존 `application.yml`은 `localhost` 기준 그대로 두고, `application-docker.yml`을 새로 추가해 컨테이너 내부 호스트명(`mysql`, `redis`, `kafka`)으로 오버라이드한다. `docker-compose.yml`의 `app` 서비스에 `SPRING_PROFILES_ACTIVE=docker`를 지정한다.

## 2. 메트릭 노출 (Spring Boot Actuator + Micrometer)

`build.gradle.kts`에 추가:
```kotlin
implementation("org.springframework.boot:spring-boot-starter-actuator")
implementation("io.micrometer:micrometer-registry-prometheus")
```

`application.yml`(모든 profile 공통)에 추가:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: prometheus,health
  metrics:
    tags:
      application: ${spring.application.name}
server:
  tomcat:
    mbeanregistry:
      enabled: true   # Tomcat 스레드 풀 메트릭을 Micrometer로 노출하려면 필요
```

| 메트릭 | 노출 방식 | 비고 |
|---|---|---|
| HikariCP 풀 (`hikaricp.connections.active/idle/pending`) | 의존성 추가만으로 자동 | Phase 2/3 가설의 핵심 검증 대상 |
| Tomcat 스레드 풀 (`tomcat.threads.busy/config.max`) | `server.tomcat.mbeanregistry.enabled: true` 필요 | 위와 함께 병목 가설의 핵심 검증 대상 |
| HTTP 요청률/지연 (`http.server.requests`) | 자동 계측, `uri` 태그로 이미 v1/v2/v3 경로 구분됨 | Grafana에서 `/api/(v\d)/...` 정규식으로 그룹화, 코드 변경 없음 |
| CPU/메모리/GC (`process.cpu.usage`, `jvm.memory.used`, `jvm.gc.*`) | 의존성 추가만으로 자동 | |
| Kafka consumer lag | kafka_exporter(별도 컨테이너, 3장 참고)가 브로커에서 직접 읽어 노출 | 앱 코드 변경 없음 |

## 3. docker-compose 추가 서비스

```yaml
  app:
    build: .
    container_name: coupon-event-app
    environment:
      SPRING_PROFILES_ACTIVE: docker
    ports:
      - "8080:8080"
    depends_on:
      - mysql
      - redis
      - kafka

  prometheus:
    image: prom/prometheus:latest
    container_name: coupon-event-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml

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

  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: coupon-event-kafka-exporter
    command: ["--kafka.server=kafka:29092"]
    ports:
      - "9308:9308"
    depends_on:
      - kafka
```

`docker/prometheus/prometheus.yml`은 `app:8080/actuator/prometheus`와 `kafka-exporter:9308/metrics`를 스크레이핑 타깃으로 등록한다(컨테이너 네트워크 안에서는 서비스명으로 서로를 찾으므로 `host.docker.internal`이 필요 없다 — app도 같은 docker-compose 네트워크 안에 있기 때문).

Kafka에는 리스너가 두 개다: 호스트 프로세스(`./gradlew bootRun`)용 `PLAINTEXT`(광고 주소 `localhost:9092`)와, compose 네트워크 안의 컨테이너 클라이언트(컨테이너화된 app, kafka-exporter)용 `DOCKER`(광고 주소 `kafka:29092`)다. 위 `kafka-exporter`의 `--kafka.server`가 `kafka:29092`인 이유가 이것이다 — `kafka:9092`로 쓰면 호스트 전용 리스너로 잘못 연결을 시도하게 되어 동작하지 않는다.

`GF_AUTH_ANONYMOUS_ENABLED`는 로컬 학습 환경에서 로그인 없이 바로 대시보드를 보기 위한 설정이다 — 운영 환경이라면 절대 쓰면 안 되지만, 이 프로젝트는 로컬 전용이므로 편의를 우선한다. **단, 7장에서 확인했듯 이 스택은 "로컬이든 클라우드 VM이든" 동일하게 띄울 수 있어야 한다는 요구사항도 있다 — 만약 실제로 클라우드 VM에 배포한다면, 익명 Grafana 관리자 접근·인증 없는 Prometheus(전체 쿼리 + admin API)·인증 없는 `/actuator/*` 엔드포인트가 그대로 공인 인터넷에 노출되므로, 배포 전에 반드시 이를 잠가야 한다(해당 포트를 제한하는 방화벽 규칙 적용, 또는 실제 인증 활성화 등) — 현재 설정은 이를 처리하지 않으며, 로컬 전제가 깨지면 실질적인 노출 위험이 된다.**

## 4. Dockerfile (앱 컨테이너화)

멀티스테이지 빌드:
```dockerfile
FROM gradle:8-jdk21 AS build
WORKDIR /app
COPY . .
RUN gradle bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

(정확한 베이스 이미지 태그는 구현 시점에 사용 가능한 최신 안정 버전으로 확정한다.)

## 5. Grafana 대시보드 (provisioning으로 코드화)

`docker/grafana/provisioning/datasources/prometheus.yml`로 Prometheus datasource를 자동 등록하고, `docker/grafana/provisioning/dashboards/`에 대시보드 JSON 3개를 커밋해 `docker compose up`만으로 그대로 재현되게 한다(수동으로 Grafana UI에서 패널을 만들지 않는다).

1. **풀 병목 검증** — HikariCP active/idle/pending, Tomcat busy/idle threads, 전체 HTTP req/s를 한 화면에. k6 부하테스트 중 풀이 실제로 고갈되는 시점과 지연 급증 시점이 겹치는지 직접 확인하는 것이 목적.
2. **API 버전 비교** — v1/v2/v3별 req/s, p95/p99 latency를 나란히.
3. **Kafka & JVM** — 파티션별 consumer lag, CPU, JVM 메모리/GC.

## 6. 검증 방법

기존 `load-test/k6/phase2-issue.js` 또는 `phase3-issue.js`를 실행하면서 대시보드 1을 관찰한다. 관찰 결과(풀 고갈 시점과 지연 급증 시점의 일치 여부, 실제 병목이 무엇이었는지)를 `load-test/README.md`의 기존 "가설" 문구를 실측으로 업데이트하거나, 별도 보고서(`docs/superpowers/reports/`)로 남긴다 — 정확한 형식은 실측 결과를 본 뒤 결정한다.

## 7. 범위 밖 (YAGNI)

- Alertmanager 등 알림 체계 — 관찰/검증용이지 운영 알림 체계가 아님
- MySQL/Redis 자체 exporter — 이번엔 앱 풀/스레드/Kafka lag/JVM에 집중
- 클라우드 전용 배포 자동화(Terraform, K8s manifest 등) — docker-compose로 로컬이든 클라우드 VM이든 동일하게 띄울 수 있으면 충분하다는 게 확인된 요구사항. 클라우드 특화 IaC는 다루지 않는다.
- 장기 메트릭 보관/스케일 아웃 — 로컬 학습 환경이므로 Prometheus 기본 설정(로컬 디스크, 기본 보관 기간)으로 충분

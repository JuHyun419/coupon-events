# Phase 3 (Redis 동기 차감 + Kafka 비동기 영속화) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redis Lua 스크립트로 재고 차감/중복체크를 원자적으로 유지하되, DB 영속화는 Kafka를 통해 비동기로 처리하는 세 번째 쿠폰 발급 API(`/api/v3/...`)를 v1/v2와 나란히 추가하고, ~10,000 TPS 부하테스트로 Phase 2 대비 개선과 consumer lag(최종 정합성까지 걸리는 시간)을 실측한다.

**Architecture:** Phase 2의 `CouponRedisIssuer`(재고 차감 원자성)를 그대로 재사용한다. v3 전용 코드는 새 패키지 `jh.couponevent.coupon.kafka`(`api`/`application`/`consumer`)에 작성한다. `POST /issue`는 Redis 차감 성공 후 `KafkaTemplate.send(...).get(timeout)`으로 동기 발행하고, 발행 성공 시 즉시 200 응답한다(DB insert는 하지 않는다). 발행 실패 시 Redis를 롤백(`INCR`+`SREM`)하고 500 응답한다. 별도의 Kafka 배치 리스너(`IssuedCouponBatchConsumer`)가 컨슈머 그룹으로 메시지를 모아 `issuedCouponRepository.saveAll(...)`로 비동기 배치 insert한다. 관리자 API(이벤트 생성)는 v2의 것을 그대로 재사용하며 v3 전용 admin 컨트롤러는 만들지 않는다. `GET /status`는 Redis(발급 여부)와 DB(영속화 완료 여부)를 함께 조회해 `NOT_ISSUED`/`PENDING`/`COMPLETED`를 구분한다.

**Tech Stack:** Kotlin 2.3.21, Spring Boot 4.1.0, Spring Kafka(spring-kafka), Spring Data Redis(Lettuce, Phase 2에서 이미 추가됨), MySQL 8.0 + Redis 7 + Kafka(KRaft, docker-compose), Awaitility(비동기 대기 검증), Mockito + mockito-kotlin, k6.

**Spec:** `docs/superpowers/specs/2026-08-13-phase3-kafka-async-design.md` (상위 로드맵: `docs/superpowers/specs/2026-07-04-coupon-event-design.md`)

## Global Constraints

- v1/v2 코드는 수정하지 않는다(관리자 API 경로 공유 목적의 재사용 제외) — v3는 순수 추가.
- 새 코드는 `jh.couponevent.coupon.kafka` 패키지 아래에 작성한다. 엔티티/JPA 리포지토리, `CouponRedisIssuer`(Phase 2)는 그대로 재사용한다.
- v3 관리자 API는 만들지 않는다 — `POST /api/v2/admin/coupon-events`로 생성한 이벤트를 `/api/v3/...` 경로로 발급한다.
- Kafka 토픽: `coupon-issue-events`, 파티션 3개, 파티션 키 = `userId`.
- Kafka 발행은 **동기식**(`KafkaTemplate.send(...).get(timeout)`)이며, 발행 실패 시 Redis를 보상 처리(`INCR`+`SREM`)하고 `CouponIssuePublishFailedException`으로 500 응답한다.
- Consumer는 Spring Kafka 배치 리스너(`spring.kafka.listener.type=batch`, `concurrency=3`)를 사용하고, `max-poll-records=500` / `fetch-max-wait=200ms`로 배치 조건을 근사한다.
- `issued_coupon.uk_event_user` UNIQUE 제약이 at-least-once 재처리의 최종 방어선이다 — 배치 insert가 실패하면 1건씩 재시도해 나머지 정상 row는 반영되게 한다.
- `coupon_event.issued_quantity` 컬럼은 v3에서도 갱신하지 않는다 — 관리자 조회는 v2와 동일하게 `totalQuantity - Redis stock`으로 역산한다.
- Testcontainers는 쓰지 않는다 — Kafka도 로컬 docker-compose로 띄운 실제 브로커에 직접 연결해서 검증한다(Phase 1/2와 동일한 방침).
- Jackson은 `tools.jackson.databind.ObjectMapper`(Jackson 3, Spring Boot 4.1 기본) — `com.fasterxml.jackson.*`을 쓰지 않는다.

---

### Task 1: Kafka 의존성 + 로컬 docker-compose Kafka(KRaft) 인프라 구성

**Files:**
- Modify: `build.gradle.kts`
- Modify: `docker-compose.yml`
- Modify: `src/main/resources/application.yml`

**Interfaces:**
- Consumes: 없음 (최초 작업)
- Produces: `spring.kafka.*` 설정(이후 Task들이 `KafkaTemplate<String, String>` 빈, `@KafkaListener` 배치 리스너로 사용), docker-compose Kafka 서비스(호스트 `localhost:9092`), 토픽 `coupon-issue-events`(파티션 3개), `org.awaitility:awaitility-kotlin` 테스트 의존성

- [ ] **Step 1: `build.gradle.kts`에 Kafka/Awaitility 의존성을 추가한다**

`dependencies` 블록의 `implementation("org.springframework.boot:spring-boot-starter-data-redis")` 다음 줄에 추가:

```kotlin
    implementation("org.springframework.kafka:spring-kafka")
```

`testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")` 다음 줄에 추가:

```kotlin
    testImplementation("org.awaitility:awaitility-kotlin:4.2.2")
```

- [ ] **Step 2: `docker-compose.yml`에 Kafka(KRaft 모드, 단일 브로커) 서비스를 추가한다**

```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: coupon-event-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: coupon_event
      MYSQL_USER: coupon
      MYSQL_PASSWORD: coupon
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql

  redis:
    image: redis:7
    container_name: coupon-event-redis
    ports:
      - "6379:6379"

  kafka:
    image: apache/kafka:3.7.0
    container_name: coupon-event-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw

volumes:
  mysql-data:
```

(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`로 둔 이유: 파티션 수를 3개로 확정적으로 보장하기 위해 — auto-create에 맡기면 기본 파티션 1개로 생성될 수 있다. 토픽은 Step 4에서 명시적으로 만든다.)

- [ ] **Step 3: `application.yml`에 Kafka 연결/컨슈머 설정을 추가한다**

`spring.data.redis` 블록 다음(같은 `spring:` 들여쓰기 레벨)에 추가:

```yaml
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: coupon-issue-consumer
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      fetch-max-wait: 200ms
    listener:
      type: batch
      concurrency: 3
      ack-mode: manual
```

- [ ] **Step 4: Kafka 컨테이너를 띄우고 토픽을 명시적으로 생성한다**

Run:
```bash
docker compose up -d
sleep 5
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic coupon-issue-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic coupon-issue-events --bootstrap-server localhost:9092
```
Expected: `Created topic coupon-issue-events.`, describe 출력에서 `PartitionCount: 3`

- [ ] **Step 5: 빌드가 새 의존성으로 성공하는지 확인한다**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 커밋**

```bash
git add build.gradle.kts docker-compose.yml src/main/resources/application.yml
git commit -m "Add Kafka dependency and local docker-compose KRaft broker for Phase 3"
```

---

### Task 2: 발행 실패 예외 + 전역 예외 핸들러 확장

**Files:**
- Modify: `src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt`
- Modify: `src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt`
- Modify: `src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt`

**Interfaces:**
- Consumes: 없음
- Produces: `CouponIssuePublishFailedException(eventId: Long, userId: Long, cause: Throwable? = null)` — Task 4가 Kafka 발행 실패 시 던짐. `GlobalExceptionHandler`가 500(`ISSUE_PUBLISH_FAILED`)으로 매핑.

- [ ] **Step 1: 예외 클래스를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt` 끝에 추가:

```kotlin

class CouponIssuePublishFailedException(eventId: Long, userId: Long, cause: Throwable? = null) :
    RuntimeException(
        "Failed to publish issued coupon event for event $eventId, user $userId after Redis stock was decremented",
        cause
    )
```

- [ ] **Step 2: 실패하는 테스트를 먼저 작성한다**

`src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
```

기존 클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @Test
    fun `발행 실패 예외는 500 INTERNAL_SERVER_ERROR, ISSUE_PUBLISH_FAILED 상태로 매핑된다`() {
        val response = handler.handlePublishFailure(CouponIssuePublishFailedException(1L, 100L))

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("ISSUE_PUBLISH_FAILED", response.body?.status)
    }
```

- [ ] **Step 3: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"`
Expected: FAIL (`handlePublishFailure`가 아직 없어서 컴파일 에러)

- [ ] **Step 4: `GlobalExceptionHandler`에 핸들러를 추가한다**

`src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
```

클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @ExceptionHandler(CouponIssuePublishFailedException::class)
    fun handlePublishFailure(e: CouponIssuePublishFailedException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ErrorResponse("ISSUE_PUBLISH_FAILED", e.message.orEmpty()))
```

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"`
Expected: `BUILD SUCCESSFUL`, 4 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt \
        src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt \
        src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt
git commit -m "Add CouponIssuePublishFailedException mapped to 500"
```

---

### Task 3: Kafka Producer 래퍼 (CouponIssueEventPublisher)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueEventPublisher.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueEventPublisherTest.kt`

**Interfaces:**
- Consumes: Task 1의 `KafkaTemplate<String, String>` 자동 설정(Spring Boot가 `spring-kafka` + `application.yml`의 `spring.kafka.producer.*`만으로 빈을 자동 등록), Task 2의 `CouponIssuePublishFailedException`
- Produces:
  - `const val COUPON_ISSUE_TOPIC = "coupon-issue-events"`
  - `data class CouponIssueEvent(eventId: Long, userId: Long, issuedAt: LocalDateTime)`
  - `CouponIssueEventPublisher.publish(eventId: Long, userId: Long, issuedAt: LocalDateTime)` — 성공 시 정상 반환, 실패 시 `CouponIssuePublishFailedException` throw

- [ ] **Step 1: 먼저 실패하는 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.KafkaException
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture

class CouponIssueEventPublisherTest {

    private val kafkaTemplate: KafkaTemplate<String, String> = mock()
    private val publisher = CouponIssueEventPublisher(kafkaTemplate)

    @Test
    fun `발행에 성공하면 예외 없이 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val sendResult: CompletableFuture<SendResult<String, String>> =
            CompletableFuture.completedFuture(mock())
        whenever(kafkaTemplate.send(eq(COUPON_ISSUE_TOPIC), eq("100"), any())).thenReturn(sendResult)

        publisher.publish(eventId = 1L, userId = 100L, issuedAt = issuedAt)
    }

    @Test
    fun `발행이 실패하면 CouponIssuePublishFailedException을 던진다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val failedFuture = CompletableFuture<SendResult<String, String>>()
        failedFuture.completeExceptionally(KafkaException("broker unreachable"))
        whenever(kafkaTemplate.send(eq(COUPON_ISSUE_TOPIC), eq("100"), any())).thenReturn(failedFuture)

        assertThrows<CouponIssuePublishFailedException> {
            publisher.publish(eventId = 1L, userId = 100L, issuedAt = issuedAt)
        }
    }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패(클래스 없음) 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueEventPublisherTest"`
Expected: FAIL (`CouponIssueEventPublisher`, `COUPON_ISSUE_TOPIC`이 아직 없어서 컴파일 에러)

- [ ] **Step 3: `CouponIssueEventPublisher`를 구현한다**

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

const val COUPON_ISSUE_TOPIC = "coupon-issue-events"

data class CouponIssueEvent(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

@Component
class CouponIssueEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val objectMapper = ObjectMapper()

    fun publish(eventId: Long, userId: Long, issuedAt: LocalDateTime) {
        val payload = objectMapper.writeValueAsString(CouponIssueEvent(eventId, userId, issuedAt))
        try {
            kafkaTemplate.send(COUPON_ISSUE_TOPIC, userId.toString(), payload)
                .get(3, TimeUnit.SECONDS)
        } catch (e: Exception) {
            throw CouponIssuePublishFailedException(eventId, userId, cause = e)
        }
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueEventPublisherTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueEventPublisher.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueEventPublisherTest.kt
git commit -m "Add CouponIssueEventPublisher wrapping synchronous Kafka send"
```

---

### Task 4: v3 쿠폰 발급 핵심 로직 (CouponIssueServiceV3)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3Test.kt`

**Interfaces:**
- Consumes: `CouponEventRepository`(공유), Phase 2의 `CouponRedisIssuer`/`CouponIssueLuaResult`(`jh.couponevent.coupon.redis.application`, 재사용), Task 3의 `CouponIssueEventPublisher.publish(...)`, Task 2의 `CouponIssuePublishFailedException`, `jh.couponevent.common.ClockConfig`의 `Clock` 빈(공유)
- Produces:
  - `data class CouponIssueAcceptedResult(eventId: Long, userId: Long, issuedAt: LocalDateTime)`
  - `CouponIssueServiceV3.issue(eventId: Long, userId: Long): CouponIssueAcceptedResult`

- [ ] **Step 1: 실패하는 테스트를 먼저 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.redis.application.CouponIssueLuaResult
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Optional
import kotlin.test.assertEquals

class CouponIssueServiceV3Test {

    private val couponEventRepository: CouponEventRepository = mock()
    private val couponRedisIssuer: CouponRedisIssuer = mock()
    private val couponIssueEventPublisher: CouponIssueEventPublisher = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-08-13T10:00:00Z"), ZoneOffset.UTC)
    private val service =
        CouponIssueServiceV3(couponEventRepository, couponRedisIssuer, couponIssueEventPublisher, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        startAt: LocalDateTime = LocalDateTime.of(2026, 8, 13, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(name = "test-event", totalQuantity = 10, startAt = startAt)
        event.id = id
        return event
    }

    @Test
    fun `Redis가 SUCCESS를 반환하고 발행에 성공하면 발급이 접수된다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)

        val result = service.issue(1L, 100L)

        assertEquals(1L, result.eventId)
        assertEquals(100L, result.userId)
    }

    @Test
    fun `이벤트가 존재하지 않으면 CouponEventNotFoundException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.empty())

        assertThrows<CouponEventNotFoundException> { service.issue(1L, 100L) }
    }

    @Test
    fun `오픈 시각 이전이면 CouponEventNotOpenException`() {
        whenever(couponEventRepository.findById(1L))
            .thenReturn(Optional.of(eventWithId(startAt = LocalDateTime.of(2026, 8, 13, 11, 0))))

        assertThrows<CouponEventNotOpenException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Redis가 SOLD_OUT을 반환하면 CouponSoldOutException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SOLD_OUT)

        assertThrows<CouponSoldOutException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Redis가 DUPLICATE를 반환하면 DuplicateCouponIssueException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.DUPLICATE)

        assertThrows<DuplicateCouponIssueException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Kafka 발행이 실패하면 Redis를 롤백하고 예외를 다시 던진다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)
        whenever(couponIssueEventPublisher.publish(any(), any(), any()))
            .thenThrow(CouponIssuePublishFailedException(1L, 100L))

        assertThrows<CouponIssuePublishFailedException> { service.issue(1L, 100L) }

        verify(couponRedisIssuer).rollback(1L, 100L)
    }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueServiceV3Test"`
Expected: FAIL (`CouponIssueServiceV3`가 아직 없어서 컴파일 에러)

- [ ] **Step 3: `CouponIssueServiceV3`를 구현한다**

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.redis.application.CouponIssueLuaResult
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.LocalDateTime

data class CouponIssueAcceptedResult(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

@Service
class CouponIssueServiceV3(
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val couponIssueEventPublisher: CouponIssueEventPublisher,
    private val clock: Clock
) {
    fun issue(eventId: Long, userId: Long): CouponIssueAcceptedResult {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }

        val now = LocalDateTime.now(clock)
        if (now.isBefore(event.startAt)) {
            throw CouponEventNotOpenException(eventId)
        }

        when (couponRedisIssuer.tryIssue(eventId, userId)) {
            CouponIssueLuaResult.DUPLICATE -> throw DuplicateCouponIssueException(eventId, userId)
            CouponIssueLuaResult.SOLD_OUT -> throw CouponSoldOutException(eventId)
            CouponIssueLuaResult.SUCCESS -> Unit
        }

        try {
            couponIssueEventPublisher.publish(eventId, userId, now)
        } catch (e: CouponIssuePublishFailedException) {
            couponRedisIssuer.rollback(eventId, userId)
            throw e
        }

        return CouponIssueAcceptedResult(eventId, userId, now)
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueServiceV3Test"`
Expected: `BUILD SUCCESSFUL`, 6 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3Test.kt
git commit -m "Add v3 CouponIssueServiceV3 publishing to Kafka after Redis decrement"
```

---

### Task 5: v3 쿠폰 발급 API (CouponIssueControllerV3)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/kafka/api/dto/CouponIssueDtoV3.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3Test.kt`

**Interfaces:**
- Consumes: Task 4의 `CouponIssueServiceV3.issue(...)`, `jh.couponevent.common.GlobalExceptionHandler`(공유, Task 2에서 확장됨)
- Produces:
  - `POST /api/v3/coupon-events/{eventId}/issue` → 200 `CouponIssueAcceptedResponse` / 403 / 404 / 409 / 410 / 500
  - `data class CouponIssueRequestV3(userId: Long)`, `data class CouponIssueAcceptedResponse(status: String, issuedAt: LocalDateTime)`

- [ ] **Step 1: DTO를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.api.dto

import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CouponIssueRequestV3(
    @field:Positive
    val userId: Long
)

data class CouponIssueAcceptedResponse(
    val status: String,
    val issuedAt: LocalDateTime
)
```

- [ ] **Step 2: 컨트롤러를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.api

import jakarta.validation.Valid
import jh.couponevent.coupon.kafka.api.dto.CouponIssueAcceptedResponse
import jh.couponevent.coupon.kafka.api.dto.CouponIssueRequestV3
import jh.couponevent.coupon.kafka.application.CouponIssueServiceV3
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v3/coupon-events/{eventId}")
class CouponIssueControllerV3(
    private val couponIssueService: CouponIssueServiceV3
) {
    @PostMapping("/issue")
    fun issue(
        @PathVariable eventId: Long,
        @Valid @RequestBody request: CouponIssueRequestV3
    ): CouponIssueAcceptedResponse {
        val result = couponIssueService.issue(eventId, request.userId)
        return CouponIssueAcceptedResponse(status = "SUCCESS", issuedAt = result.issuedAt)
    }
}
```

- [ ] **Step 3: `@WebMvcTest`로 예외 → HTTP 상태 매핑까지 검증하는 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.api

import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.kafka.api.dto.CouponIssueRequestV3
import jh.couponevent.coupon.kafka.application.CouponIssueAcceptedResult
import jh.couponevent.coupon.kafka.application.CouponIssueServiceV3
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime

@WebMvcTest(CouponIssueControllerV3::class)
class CouponIssueControllerV3Test @Autowired constructor(
    private val mockMvc: MockMvc
) {
    @MockitoBean
    private lateinit var couponIssueService: CouponIssueServiceV3

    private val objectMapper = ObjectMapper()

    @Test
    fun `재고가 소진되면 410 GONE과 SOLD_OUT 상태를 반환한다`() {
        whenever(couponIssueService.issue(any(), any())).thenThrow(CouponSoldOutException(1L))

        mockMvc.post("/api/v3/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequestV3(userId = 100L))
        }.andExpect {
            status { isGone() }
            jsonPath("$.status") { value("SOLD_OUT") }
        }
    }

    @Test
    fun `발급이 접수되면 200과 SUCCESS 상태를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        whenever(couponIssueService.issue(any(), any()))
            .thenReturn(CouponIssueAcceptedResult(eventId = 1L, userId = 100L, issuedAt = issuedAt))

        mockMvc.post("/api/v3/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequestV3(userId = 100L))
        }.andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SUCCESS") }
        }
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.api.CouponIssueControllerV3Test"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/kafka/api/dto/CouponIssueDtoV3.kt \
        src/main/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3Test.kt
git commit -m "Add v3 coupon issue API endpoint publishing to Kafka"
```

---

### Task 6: Kafka 배치 컨슈머 (IssuedCouponBatchConsumer)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/kafka/consumer/IssuedCouponBatchConsumer.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/kafka/consumer/IssuedCouponBatchConsumerTest.kt`

**Interfaces:**
- Consumes: `IssuedCouponRepository`(공유), Task 3의 `CouponIssueEvent`/`COUPON_ISSUE_TOPIC`
- Produces: `IssuedCouponBatchConsumer.consume(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment)` — `@KafkaListener(topics = [COUPON_ISSUE_TOPIC], groupId = "coupon-issue-consumer")`로 등록되어 Task 1의 `spring.kafka.listener.type=batch` 설정에 따라 Spring이 자동 호출

- [ ] **Step 1: 먼저 실패하는 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.consumer

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.kafka.application.COUPON_ISSUE_TOPIC
import jh.couponevent.coupon.kafka.application.CouponIssueEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.support.Acknowledgment
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime
import kotlin.test.assertEquals

class IssuedCouponBatchConsumerTest {

    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val objectMapper = ObjectMapper()
    private val consumer = IssuedCouponBatchConsumer(issuedCouponRepository)
    private val ack: Acknowledgment = mock()

    private fun record(eventId: Long, userId: Long, issuedAt: LocalDateTime): ConsumerRecord<String, String> =
        ConsumerRecord(
            COUPON_ISSUE_TOPIC, 0, 0L, userId.toString(),
            objectMapper.writeValueAsString(CouponIssueEvent(eventId, userId, issuedAt))
        )

    @Test
    fun `배치로 받은 레코드를 saveAll로 한 번에 저장하고 ack한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val records = listOf(record(1L, 100L, issuedAt), record(1L, 101L, issuedAt))

        consumer.consume(records, ack)

        val captor = argumentCaptor<List<IssuedCoupon>>()
        verify(issuedCouponRepository).saveAll(captor.capture())
        val saved = captor.firstValue
        assertEquals(2, saved.size)
        assertEquals(setOf(100L, 101L), saved.map { it.userId }.toSet())
        verify(ack).acknowledge()
    }

    @Test
    fun `saveAll이 UNIQUE 제약 위반으로 실패하면 1건씩 재시도해 나머지는 반영하고 ack한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val records = listOf(record(1L, 100L, issuedAt), record(1L, 101L, issuedAt))
        whenever(issuedCouponRepository.saveAll(any<List<IssuedCoupon>>()))
            .thenThrow(DataIntegrityViolationException("duplicate"))
        whenever(issuedCouponRepository.save(argThat { it.userId == 100L }))
            .thenThrow(DataIntegrityViolationException("duplicate"))
        whenever(issuedCouponRepository.save(argThat { it.userId == 101L }))
            .thenAnswer { invocation -> invocation.arguments[0] }

        consumer.consume(records, ack)

        verify(issuedCouponRepository).save(argThat { it.userId == 101L })
        verify(ack).acknowledge()
    }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.consumer.IssuedCouponBatchConsumerTest"`
Expected: FAIL (`IssuedCouponBatchConsumer`가 아직 없어서 컴파일 에러)

- [ ] **Step 3: `IssuedCouponBatchConsumer`를 구현한다**

```kotlin
package jh.couponevent.coupon.kafka.consumer

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.kafka.application.COUPON_ISSUE_TOPIC
import jh.couponevent.coupon.kafka.application.CouponIssueEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class IssuedCouponBatchConsumer(
    private val issuedCouponRepository: IssuedCouponRepository
) {
    private val objectMapper = ObjectMapper()

    @KafkaListener(topics = [COUPON_ISSUE_TOPIC], groupId = "coupon-issue-consumer")
    fun consume(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
        val coupons = records.map { it.toIssuedCoupon() }
        try {
            issuedCouponRepository.saveAll(coupons)
        } catch (e: DataIntegrityViolationException) {
            coupons.forEach { coupon ->
                runCatching { issuedCouponRepository.save(coupon) }
            }
        }
        ack.acknowledge()
    }

    private fun ConsumerRecord<String, String>.toIssuedCoupon(): IssuedCoupon {
        val event = objectMapper.readValue(value(), CouponIssueEvent::class.java)
        return IssuedCoupon(couponEventId = event.eventId, userId = event.userId, issuedAt = event.issuedAt)
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.consumer.IssuedCouponBatchConsumerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/kafka/consumer/IssuedCouponBatchConsumer.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/consumer/IssuedCouponBatchConsumerTest.kt
git commit -m "Add Kafka batch listener persisting issued coupons asynchronously"
```

---

### Task 7: 상태 조회 API (NOT_ISSUED / PENDING / COMPLETED)

**Files:**
- Modify: `src/main/kotlin/jh/couponevent/coupon/domain/IssuedCouponRepository.kt`
- Modify: `src/main/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuer.kt`
- Modify: `src/test/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuerTest.kt`
- Modify: `src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3.kt`
- Modify: `src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3Test.kt`
- Modify: `src/main/kotlin/jh/couponevent/coupon/kafka/api/dto/CouponIssueDtoV3.kt`
- Modify: `src/main/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3.kt`
- Modify: `src/test/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3Test.kt`

**Interfaces:**
- Consumes: Task 4의 `CouponIssueServiceV3`(수정 대상), Task 6이 채우는 `issued_coupon` 테이블
- Produces:
  - `IssuedCouponRepository.findByCouponEventIdAndUserId(couponEventId: Long, userId: Long): IssuedCoupon?`
  - `CouponRedisIssuer.isIssuedInRedis(eventId: Long, userId: Long): Boolean`
  - `enum class CouponIssueStatus { NOT_ISSUED, PENDING, COMPLETED }`
  - `data class CouponStatusResponseV3(eventId: Long, userId: Long, status: CouponIssueStatus, issuedAt: LocalDateTime?)`
  - `CouponIssueServiceV3.status(eventId: Long, userId: Long): CouponStatusResponseV3`
  - `GET /api/v3/coupon-events/{eventId}/users/{userId}/status` → 200 `CouponStatusResponseV3`

- [ ] **Step 1: `IssuedCouponRepository`에 조회 메서드를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/domain/IssuedCouponRepository.kt`를 다음으로 교체:

```kotlin
package jh.couponevent.coupon.domain

import org.springframework.data.jpa.repository.JpaRepository

interface IssuedCouponRepository : JpaRepository<IssuedCoupon, Long> {
    fun existsByCouponEventIdAndUserId(couponEventId: Long, userId: Long): Boolean
    fun countByCouponEventId(couponEventId: Long): Long
    fun findByCouponEventIdAndUserId(couponEventId: Long, userId: Long): IssuedCoupon?
}
```

- [ ] **Step 2: `CouponRedisIssuer`에 Redis 발급 여부 확인 메서드를 추가하는 실패 테스트를 먼저 작성한다**

`src/test/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuerTest.kt`의 마지막 `@Test` 메서드(`rollback하면...`) 다음, 클래스 본문 끝(`companion object` 앞)에 추가:

```kotlin

    @Test
    fun `isIssuedInRedis는 발급자 Set에 있는 사용자만 true를 반환한다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)

        assertEquals(true, issuer.isIssuedInRedis(TEST_EVENT_ID, 1L))
        assertEquals(false, issuer.isIssuedInRedis(TEST_EVENT_ID, 2L))
    }
```

- [ ] **Step 3: 테스트 실행 → 컴파일 실패 확인**

Run: `docker compose up -d` (Redis가 안 떠 있다면 먼저 기동)
Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponRedisIssuerTest"`
Expected: FAIL (`isIssuedInRedis`가 아직 없어서 컴파일 에러)

- [ ] **Step 4: `CouponRedisIssuer`에 메서드를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuer.kt`의 `remainingStock` 메서드 다음에 추가:

```kotlin

    fun isIssuedInRedis(eventId: Long, userId: Long): Boolean =
        redisTemplate.opsForSet().isMember(issuedUsersKey(eventId), userId.toString()) ?: false
```

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponRedisIssuerTest"`
Expected: `BUILD SUCCESSFUL`, 5 tests passed

- [ ] **Step 6: `CouponIssueServiceV3Test`에 `status` 실패 테스트를 먼저 추가한다**

`src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3Test.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
```

`private val service = ...` 줄을 다음으로 교체(생성자에 리포지토리 추가):

```kotlin
    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val service = CouponIssueServiceV3(
        couponEventRepository, couponRedisIssuer, couponIssueEventPublisher, issuedCouponRepository, fixedClock
    )
```

클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @Test
    fun `DB에 row가 있으면 COMPLETED를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val coupon = IssuedCoupon(couponEventId = 1L, userId = 100L, issuedAt = issuedAt)
        whenever(issuedCouponRepository.findByCouponEventIdAndUserId(1L, 100L)).thenReturn(coupon)

        val result = service.status(1L, 100L)

        assertEquals(CouponIssueStatus.COMPLETED, result.status)
        assertEquals(issuedAt, result.issuedAt)
    }

    @Test
    fun `DB에는 없지만 Redis에는 있으면 PENDING을 반환한다`() {
        whenever(issuedCouponRepository.findByCouponEventIdAndUserId(1L, 100L)).thenReturn(null)
        whenever(couponRedisIssuer.isIssuedInRedis(1L, 100L)).thenReturn(true)

        val result = service.status(1L, 100L)

        assertEquals(CouponIssueStatus.PENDING, result.status)
        assertEquals(null, result.issuedAt)
    }

    @Test
    fun `DB와 Redis 어디에도 없으면 NOT_ISSUED를 반환한다`() {
        whenever(issuedCouponRepository.findByCouponEventIdAndUserId(1L, 100L)).thenReturn(null)
        whenever(couponRedisIssuer.isIssuedInRedis(1L, 100L)).thenReturn(false)

        val result = service.status(1L, 100L)

        assertEquals(CouponIssueStatus.NOT_ISSUED, result.status)
    }
```

- [ ] **Step 7: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueServiceV3Test"`
Expected: FAIL (생성자 시그니처 불일치, `status` 메서드 없음, `CouponIssueStatus` 없음으로 컴파일 에러)

- [ ] **Step 8: `CouponIssueServiceV3`를 수정해 `status`를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3.kt` 전체를 다음으로 교체:

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.redis.application.CouponIssueLuaResult
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.LocalDateTime

data class CouponIssueAcceptedResult(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

enum class CouponIssueStatus {
    NOT_ISSUED, PENDING, COMPLETED
}

data class CouponStatusResponseV3(
    val eventId: Long,
    val userId: Long,
    val status: CouponIssueStatus,
    val issuedAt: LocalDateTime?
)

@Service
class CouponIssueServiceV3(
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val couponIssueEventPublisher: CouponIssueEventPublisher,
    private val issuedCouponRepository: IssuedCouponRepository,
    private val clock: Clock
) {
    fun issue(eventId: Long, userId: Long): CouponIssueAcceptedResult {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }

        val now = LocalDateTime.now(clock)
        if (now.isBefore(event.startAt)) {
            throw CouponEventNotOpenException(eventId)
        }

        when (couponRedisIssuer.tryIssue(eventId, userId)) {
            CouponIssueLuaResult.DUPLICATE -> throw DuplicateCouponIssueException(eventId, userId)
            CouponIssueLuaResult.SOLD_OUT -> throw CouponSoldOutException(eventId)
            CouponIssueLuaResult.SUCCESS -> Unit
        }

        try {
            couponIssueEventPublisher.publish(eventId, userId, now)
        } catch (e: CouponIssuePublishFailedException) {
            couponRedisIssuer.rollback(eventId, userId)
            throw e
        }

        return CouponIssueAcceptedResult(eventId, userId, now)
    }

    fun status(eventId: Long, userId: Long): CouponStatusResponseV3 {
        val persisted = issuedCouponRepository.findByCouponEventIdAndUserId(eventId, userId)
        if (persisted != null) {
            return CouponStatusResponseV3(eventId, userId, CouponIssueStatus.COMPLETED, persisted.issuedAt)
        }

        val status = if (couponRedisIssuer.isIssuedInRedis(eventId, userId)) {
            CouponIssueStatus.PENDING
        } else {
            CouponIssueStatus.NOT_ISSUED
        }
        return CouponStatusResponseV3(eventId, userId, status, issuedAt = null)
    }
}
```

- [ ] **Step 9: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueServiceV3Test"`
Expected: `BUILD SUCCESSFUL`, 9 tests passed

- [ ] **Step 10: 컨트롤러에 상태 조회 엔드포인트를 추가하는 실패 테스트를 먼저 작성한다**

`src/test/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3Test.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.kafka.application.CouponIssueStatus
import jh.couponevent.coupon.kafka.application.CouponStatusResponseV3
import org.springframework.test.web.servlet.get
```

클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @Test
    fun `상태 조회는 서비스 결과를 그대로 응답한다`() {
        whenever(couponIssueService.status(1L, 100L)).thenReturn(
            CouponStatusResponseV3(eventId = 1L, userId = 100L, status = CouponIssueStatus.PENDING, issuedAt = null)
        )

        mockMvc.get("/api/v3/coupon-events/1/users/100/status").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("PENDING") }
        }
    }
```

- [ ] **Step 11: 테스트 실행 → 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.api.CouponIssueControllerV3Test"`
Expected: FAIL (컨트롤러에 `status` 엔드포인트가 없어 404)

- [ ] **Step 12: DTO와 컨트롤러에 상태 조회 엔드포인트를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/kafka/api/dto/CouponIssueDtoV3.kt`는 수정하지 않는다(상태 응답은 서비스의 `CouponStatusResponseV3`를 그대로 반환하므로 별도 DTO 불필요).

`src/main/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.kafka.application.CouponStatusResponseV3
import org.springframework.web.bind.annotation.GetMapping
```

클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @GetMapping("/users/{userId}/status")
    fun status(@PathVariable eventId: Long, @PathVariable userId: Long): CouponStatusResponseV3 =
        couponIssueService.status(eventId, userId)
```

- [ ] **Step 13: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.api.CouponIssueControllerV3Test"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed

- [ ] **Step 14: 전체 테스트 스위트 실행 (v1/v2/v3 빈 이름 충돌 등 컨텍스트 로딩 문제가 없는지 반드시 확인)**

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`, 모든 테스트 통과 — 특히 `CouponEventApplicationTests.contextLoads()`와 `@SpringBootTest` 기반 테스트가 v3 빈들과 충돌 없이 컨텍스트를 띄우는지 확인 (Phase 2에서 v1/v2 클래스명 충돌로 `ConflictingBeanDefinitionException`이 났던 전례가 있으므로 반드시 스킵 없이 실행할 것)

- [ ] **Step 15: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/domain/IssuedCouponRepository.kt \
        src/main/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuer.kt \
        src/test/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuerTest.kt \
        src/main/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueServiceV3Test.kt \
        src/main/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3.kt \
        src/test/kotlin/jh/couponevent/coupon/kafka/api/CouponIssueControllerV3Test.kt
git commit -m "Add NOT_ISSUED/PENDING/COMPLETED status endpoint for Phase 3"
```

---

### Task 8: 동시성 + 최종 정합성 통합 테스트 (실제 Kafka/Redis/MySQL)

**Files:**
- Test: `src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueConcurrencyV3Test.kt`

**Interfaces:**
- Consumes: Task 4/7의 `CouponIssueServiceV3.issue(...)`/`status(...)`, `CouponEventRepository`(공유), `CouponRedisIssuer`(공유), `IssuedCouponRepository`(공유), Task 6의 `IssuedCouponBatchConsumer`(간접 — 실행 중인 Spring 컨텍스트가 자동 구동) — `@SpringBootTest`로 전체 컨텍스트를 띄워 실제 빈 사용
- Produces: 없음 (검증 전용 테스트)

- [ ] **Step 1: MySQL, Redis, Kafka가 모두 떠 있고 토픽이 존재하는지 확인한다**

Run: `docker compose up -d && docker compose ps`
Expected: `mysql`, `redis`, `kafka` 서비스 모두 `running`

Run: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
Expected: `coupon-issue-events` 포함 (Task 1에서 생성 안 됐다면 Task 1 Step 4를 다시 실행)

- [ ] **Step 2: 동시성 + 최종 정합성 통합 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

@SpringBootTest
class CouponIssueConcurrencyV3Test @Autowired constructor(
    private val couponIssueService: CouponIssueServiceV3,
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동시에 200명이 요청해도 Redis 재고 100개만 정확히 발급되고 결국 DB row 100개로 수렴한다`() {
        val totalQuantity = 100
        val requestCount = 200
        val event = couponEventRepository.save(
            CouponEvent(
                name = "kafka-concurrency-test-${System.currentTimeMillis()}",
                totalQuantity = totalQuantity,
                startAt = LocalDateTime.now().minusMinutes(1)
            )
        )
        val eventId = requireNotNull(event.id)
        couponRedisIssuer.seedStock(eventId, totalQuantity)

        val executor = Executors.newFixedThreadPool(32)
        val latch = CountDownLatch(requestCount)
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)

        (1..requestCount).forEach { userId ->
            executor.submit {
                try {
                    couponIssueService.issue(eventId, userId.toLong())
                    successCount.incrementAndGet()
                } catch (e: Exception) {
                    failCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        // Redis 차감은 동기적이므로 요청이 끝난 시점에 이미 확정되어 있다.
        assertEquals(totalQuantity, successCount.get())
        assertEquals(requestCount - totalQuantity, failCount.get())
        assertEquals(0, couponRedisIssuer.remainingStock(eventId))

        // DB 반영은 Kafka 컨슈머가 비동기로 처리하므로, 완료될 때까지 최대 30초 기다린다.
        await atMost Duration.ofSeconds(30) untilAsserted {
            assertEquals(totalQuantity.toLong(), issuedCouponRepository.countByCouponEventId(eventId))
        }

        val completedStatus = couponIssueService.status(eventId, 1L)
        assertEquals(CouponIssueStatus.COMPLETED, completedStatus.status)
    }
}
```

- [ ] **Step 3: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.kafka.application.CouponIssueConcurrencyV3Test"`
Expected: `BUILD SUCCESSFUL`, 1 test passed (Redis 기준 정확히 100건 성공/100건 실패는 즉시 확인되고, DB row 100개 수렴은 Awaitility가 컨슈머의 비동기 반영을 기다린 뒤 확인)

- [ ] **Step 4: 커밋**

```bash
git add src/test/kotlin/jh/couponevent/coupon/kafka/application/CouponIssueConcurrencyV3Test.kt
git commit -m "Add concurrency + eventual-consistency integration test against real Kafka"
```

---

### Task 9: k6 부하테스트 (~10,000 TPS 실측 + consumer lag 관찰)

**Files:**
- Create: `load-test/k6/phase3-issue.js`
- Modify: `load-test/README.md`

**Interfaces:**
- Consumes: Task 5의 `POST /api/v3/coupon-events/{eventId}/issue`
- Produces: 없음 (실측/관찰용 산출물)

- [ ] **Step 1: 애플리케이션을 실행한다**

Run: `docker compose up -d && ./gradlew bootRun`
Expected: `Started CouponEventApplicationKt` 로그, 포트 8080에서 서비스 중

- [ ] **Step 2: 별도 터미널에서 v3 테스트용 이벤트를 생성한다 (v2 admin API 재사용, 재고를 넉넉히 잡아 품절이 아니라 처리량 자체를 관찰)**

Run:
```bash
curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"phase3-load-test","totalQuantity":500000,"startAt":"2020-01-01T00:00:00"}'
```
Expected: `201 Created`, 응답 JSON에서 생성된 `id` 값을 확인한다 (이후 단계의 `EVENT_ID`로 사용)

- [ ] **Step 3: k6 스크립트를 작성한다**

```javascript
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const EVENT_ID = __ENV.EVENT_ID || '1';

export const options = {
  scenarios: {
    phase3_10000tps: {
      executor: 'constant-arrival-rate',
      rate: 10000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 500,
      maxVUs: 5000,
    },
  },
};

export default function () {
  const userId = (Date.now() % 1000000000) + __VU * 100000 + __ITER;
  const payload = JSON.stringify({ userId });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const res = http.post(`${BASE_URL}/api/v3/coupon-events/${EVENT_ID}/issue`, payload, params);

  check(res, {
    'status is 200, 409, or 410': (r) => [200, 409, 410].includes(r.status),
  });
}
```

- [ ] **Step 4: k6를 실행한다**

Run: `EVENT_ID=<Step 2에서 확인한 id> k6 run load-test/k6/phase3-issue.js`
Expected: 콘솔에 `http_req_duration`, `checks` 등 요약 통계가 출력된다.

- [ ] **Step 5: consumer lag(최종 정합성까지 걸리는 시간)을 관찰한다**

k6 실행이 끝난 직후, `issued_coupon` 테이블에 아직 반영되지 않은 건수를 반복 조회해서 0이 될 때까지 걸리는 시간을 기록한다:

```bash
curl -s http://localhost:8080/api/v2/admin/coupon-events/EVENT_ID | grep -o '"issuedQuantity":[0-9]*'
```

위 값(= `totalQuantity - Redis stock`, Redis 기준 이미 확정된 발급 수)과, MySQL에 직접 접속해 확인한 `SELECT COUNT(*) FROM issued_coupon WHERE coupon_event_id = EVENT_ID`(DB에 실제로 반영된 수)를 몇 초 간격으로 비교하며 두 값이 같아지는 시점까지의 시간을 재는 것이 consumer lag 실측이다.

- [ ] **Step 6: 실행 결과를 관찰하고 기록한다**

Phase 2(`load-test/README.md`의 실측 결과: 578.70 req/s, p95 4.8s)와 비교해서, p95/p99 지연 시간이 더 낮게 유지되는지(DB insert가 응답 경로에서 빠졌으므로 개선 기대), 실제 처리량이 10,000 req/s에 얼마나 근접하는지, 그리고 Step 5의 consumer lag이 몇 초 수준인지 기록한다.

- [ ] **Step 7: `load-test/README.md`에 Phase 3 섹션을 추가한다**

파일 끝에 추가:

```markdown

## Phase 3: Redis + Kafka 비동기 영속화 (~10,000 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (v2 admin API 재사용, 재고를 넉넉히 잡아 품절이 아닌 처리량 자체를 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase3-load-test","totalQuantity":500000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase3-issue.js`
4. 관찰 포인트: Phase 2(578.70 req/s, p95 4.8s) 대비 응답 처리량/지연이 얼마나 개선되는지, 실제 처리량이 10,000 req/s에 얼마나 근접하는지, 그리고 k6 종료 후 `issued_coupon` DB row 수가 Redis 기준 확정 발급 수와 같아질 때까지 걸리는 시간(consumer lag)
```

- [ ] **Step 8: 커밋**

```bash
git add load-test/k6/phase3-issue.js load-test/README.md
git commit -m "Add k6 load test script for Phase 3 (~10,000 TPS)"
```

---

### Task 10: 손으로 확인하는 수동 테스트 가이드 작성

**Files:**
- Create: `docs/superpowers/reports/2026-08-13-phase3-kafka-async/manual-testing-guide.md`

**Interfaces:**
- Consumes: 앞선 모든 Task의 API 엔드포인트
- Produces: 없음 (문서 산출물)

Phase 2의 `docs/superpowers/reports/2026-07-11-phase2-redis-lua/manual-testing-guide.md`와 같은 형식으로, v2와 v3를 나란히 손으로 호출해 "응답은 즉시 오지만 DB 반영은 잠시 후"라는 Phase 3의 핵심 차이를 직접 관찰할 수 있는 가이드를 만든다.

- [ ] **Step 1: 가이드 문서를 작성한다**

```markdown
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
```

- [ ] **Step 2: 커밋**

```bash
git add docs/superpowers/reports/2026-08-13-phase3-kafka-async/manual-testing-guide.md
git commit -m "Add manual testing guide for hands-on verification of Phase 3"
```

---

## 완료 후 다음 단계

Phase 3가 끝나면 로드맵(`docs/superpowers/specs/2026-07-04-coupon-event-design.md`)의 세 Phase가 모두 완료된다. 이후 작업(모니터링 대시보드, 인증/인가 등)은 로드맵의 "범위 밖"에 명시되어 있으므로, 새 요구사항이 생기면 그때 별도로 브레인스토밍한다.

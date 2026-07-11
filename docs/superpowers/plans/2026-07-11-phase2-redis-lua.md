# Phase 2 (Redis Lua Script 원자적 차감) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redis Lua 스크립트로 재고 차감과 중복 체크를 원자적으로 수행하는 두 번째 쿠폰 발급 API(`/api/v2/...`)를 Phase 1(`/api/v1/...`, MySQL 비관적 락)과 나란히 두고, ~1,000 TPS 부하테스트로 Phase 1 대비 개선을 실측한다.

**Architecture:** 기존 `CouponEvent`/`IssuedCoupon` 엔티티와 JPA 리포지토리는 v1/v2가 공유한다. v2 전용 코드는 새 패키지 `jh.couponevent.coupon.redis`(`api`/`application`/`dto`)에 작성한다. 재고 판정은 `CouponRedisIssuer`가 감싼 Lua 스크립트가 원자적으로 수행하고, `SUCCESS`일 때만 `issued_coupon`을 insert한다. insert 실패 시 Redis를 롤백(`INCR`+`SREM`)한다. `coupon_event.issued_quantity` 컬럼은 v2 경로에서 갱신하지 않고, 관리자 조회는 Redis 값으로 역산한다.

**Tech Stack:** Kotlin 2.3.21, Spring Boot 4.1.0, Spring Data Redis(Lettuce), MySQL 8.0 + Redis 7(docker-compose), Mockito + mockito-kotlin, k6.

## Global Constraints

- 참조 스펙: `docs/superpowers/specs/2026-07-11-phase2-redis-lua-design.md`, `docs/superpowers/specs/2026-07-04-coupon-event-design.md`
- Phase 1 코드는 삭제/교체하지 않고 그대로 유지한다 — 경로만 `/api/v1/...`로 옮긴다.
- 새 코드는 `jh.couponevent.coupon.redis` 패키지 아래에 작성한다. 엔티티/JPA 리포지토리(`CouponEvent`, `IssuedCoupon`, `CouponEventRepository`, `IssuedCouponRepository`)는 기존 `jh.couponevent.coupon.domain` 패키지 것을 그대로 재사용한다.
- Redis 키: `coupon:{eventId}:stock`(문자열, 잔여 재고), `coupon:{eventId}:issued_users`(Set, 발급자 userId).
- `coupon_event.issued_quantity` 컬럼은 v2 경로에서 갱신하지 않는다 — SSOT는 Redis.
- Testcontainers는 쓰지 않는다 — Redis 관련 테스트도 로컬 docker-compose로 띄운 실제 Redis에 직접 연결해서 검증한다(Phase 1과 동일한 방침).
- 이벤트 오픈 이전 차단은 애플리케이션 레벨(`startAt` 비교)에서 Redis 접근 전에 수행한다.

---

### Task 1: Redis 의존성 추가 + 로컬 Redis 인프라 구성

**Files:**
- Modify: `build.gradle.kts`
- Modify: `docker-compose.yml`
- Modify: `src/main/resources/application.yml`

**Interfaces:**
- Consumes: 없음 (최초 작업)
- Produces: `spring.data.redis.*` 설정(다음 Task들이 `StringRedisTemplate` 빈으로 연결), docker-compose Redis 서비스(호스트 `localhost:6379`)

- [ ] **Step 1: `build.gradle.kts`에 Redis 의존성을 추가한다**

`dependencies` 블록의 `implementation("org.springframework.boot:spring-boot-starter-data-jpa")` 다음 줄에 추가:

```kotlin
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
```

- [ ] **Step 2: `docker-compose.yml`에 Redis 서비스를 추가한다**

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

volumes:
  mysql-data:
```

- [ ] **Step 3: `application.yml`에 Redis 연결 설정을 추가한다**

`spring.jpa` 블록 다음에 추가(들여쓰기는 `spring:` 아래 다른 항목들과 동일하게 맞춘다):

```yaml
  data:
    redis:
      host: localhost
      port: 6379
```

- [ ] **Step 4: Redis 컨테이너를 띄우고 정상 기동을 확인한다**

Run: `docker compose up -d && sleep 3 && docker compose ps`
Expected: `redis` 서비스 상태가 `running`, `redis-cli -h localhost ping`으로 확인 가능(`docker compose exec redis redis-cli ping` → `PONG`)

- [ ] **Step 5: 빌드가 새 의존성으로 성공하는지 확인한다**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 커밋**

```bash
git add build.gradle.kts docker-compose.yml src/main/resources/application.yml
git commit -m "Add Redis dependency and local docker-compose infra for Phase 2"
```

---

### Task 2: Phase 1 API 경로를 /api/v1/...로 이동

**Files:**
- Modify: `src/main/kotlin/jh/couponevent/coupon/api/CouponEventAdminController.kt:18`
- Modify: `src/main/kotlin/jh/couponevent/coupon/api/CouponIssueController.kt:17`
- Modify: `src/test/kotlin/jh/couponevent/coupon/api/CouponIssueControllerTest.kt:36,51`
- Modify: `load-test/k6/phase1-issue.js:25`
- Modify: `load-test/README.md`

**Interfaces:**
- Consumes: 없음 (기존 코드 경로만 변경)
- Produces: `/api/v1/admin/coupon-events`, `/api/v1/coupon-events/{eventId}/issue`, `/api/v1/coupon-events/{eventId}/users/{userId}/status` (이후 Task들이 v2와 구분해서 참조)

- [ ] **Step 1: `CouponEventAdminController`의 경로를 변경한다**

`src/main/kotlin/jh/couponevent/coupon/api/CouponEventAdminController.kt:18`에서:

```kotlin
@RequestMapping("/api/admin/coupon-events")
```

를 다음으로 교체:

```kotlin
@RequestMapping("/api/v1/admin/coupon-events")
```

- [ ] **Step 2: `CouponIssueController`의 경로를 변경한다**

`src/main/kotlin/jh/couponevent/coupon/api/CouponIssueController.kt:17`에서:

```kotlin
@RequestMapping("/api/coupon-events/{eventId}")
```

를 다음으로 교체:

```kotlin
@RequestMapping("/api/v1/coupon-events/{eventId}")
```

- [ ] **Step 3: `CouponIssueControllerTest`의 요청 경로 두 곳을 갱신한다**

`src/test/kotlin/jh/couponevent/coupon/api/CouponIssueControllerTest.kt`에서 두 번 나오는:

```kotlin
        mockMvc.post("/api/coupon-events/1/issue") {
```

를 각각:

```kotlin
        mockMvc.post("/api/v1/coupon-events/1/issue") {
```

로 교체.

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.api.CouponIssueControllerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: k6 스크립트와 README의 경로를 갱신한다**

`load-test/k6/phase1-issue.js:25`에서:

```javascript
  const res = http.post(`${BASE_URL}/api/coupon-events/${EVENT_ID}/issue`, payload, params);
```

를:

```javascript
  const res = http.post(`${BASE_URL}/api/v1/coupon-events/${EVENT_ID}/issue`, payload, params);
```

로 교체. `load-test/README.md`의 curl 예시:

```bash
   curl -X POST http://localhost:8080/api/admin/coupon-events \
```

를:

```bash
   curl -X POST http://localhost:8080/api/v1/admin/coupon-events \
```

로 교체.

- [ ] **Step 6: 전체 테스트 스위트가 여전히 통과하는지 확인한다**

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 7: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/api/CouponEventAdminController.kt \
        src/main/kotlin/jh/couponevent/coupon/api/CouponIssueController.kt \
        src/test/kotlin/jh/couponevent/coupon/api/CouponIssueControllerTest.kt \
        load-test/k6/phase1-issue.js load-test/README.md
git commit -m "Move Phase 1 API paths under /api/v1 to make room for Phase 2"
```

---

### Task 3: Redis Lua 스크립트 래퍼 (CouponRedisIssuer)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuer.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuerTest.kt`

**Interfaces:**
- Consumes: Task 1의 `StringRedisTemplate` 자동 설정(Spring Boot가 `spring-boot-starter-data-redis`만으로 빈을 자동 등록)
- Produces:
  - `enum class CouponIssueLuaResult { SUCCESS, DUPLICATE, SOLD_OUT }`
  - `CouponRedisIssuer.tryIssue(eventId: Long, userId: Long): CouponIssueLuaResult`
  - `CouponRedisIssuer.rollback(eventId: Long, userId: Long)`
  - `CouponRedisIssuer.seedStock(eventId: Long, totalQuantity: Int)`
  - `CouponRedisIssuer.remainingStock(eventId: Long): Int`

- [ ] **Step 1: 먼저 실패하는 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.application

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.assertEquals

class CouponRedisIssuerTest {

    private val connectionFactory = LettuceConnectionFactory("localhost", 6379).apply { afterPropertiesSet() }
    private val redisTemplate = StringRedisTemplate(connectionFactory).apply { afterPropertiesSet() }
    private val issuer = CouponRedisIssuer(redisTemplate)

    @BeforeEach
    fun setUp() {
        issuer.seedStock(TEST_EVENT_ID, 2)
    }

    @Test
    fun `재고가 있고 미발급 사용자면 SUCCESS를 반환하고 재고를 차감한다`() {
        val result = issuer.tryIssue(TEST_EVENT_ID, 1L)

        assertEquals(CouponIssueLuaResult.SUCCESS, result)
        assertEquals(1, issuer.remainingStock(TEST_EVENT_ID))
    }

    @Test
    fun `이미 발급받은 사용자가 재요청하면 DUPLICATE를 반환한다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)

        val result = issuer.tryIssue(TEST_EVENT_ID, 1L)

        assertEquals(CouponIssueLuaResult.DUPLICATE, result)
    }

    @Test
    fun `재고가 소진되면 SOLD_OUT을 반환한다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)
        issuer.tryIssue(TEST_EVENT_ID, 2L)

        val result = issuer.tryIssue(TEST_EVENT_ID, 3L)

        assertEquals(CouponIssueLuaResult.SOLD_OUT, result)
    }

    @Test
    fun `rollback하면 재고가 복구되고 발급자 목록에서 제거된다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)

        issuer.rollback(TEST_EVENT_ID, 1L)

        assertEquals(2, issuer.remainingStock(TEST_EVENT_ID))
        assertEquals(CouponIssueLuaResult.SUCCESS, issuer.tryIssue(TEST_EVENT_ID, 1L))
    }

    companion object {
        private const val TEST_EVENT_ID = 999_999L
    }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패(클래스 없음) 확인**

Run: `docker compose up -d` (Redis가 안 떠 있다면 먼저 기동)
Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponRedisIssuerTest"`
Expected: FAIL (`CouponRedisIssuer`가 아직 없어서 컴파일 에러)

- [ ] **Step 3: `CouponRedisIssuer`를 구현한다**

```kotlin
package jh.couponevent.coupon.redis.application

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

enum class CouponIssueLuaResult {
    SUCCESS, DUPLICATE, SOLD_OUT
}

@Component
class CouponRedisIssuer(
    private val redisTemplate: StringRedisTemplate
) {
    private val issueScript = DefaultRedisScript(ISSUE_SCRIPT, String::class.java)

    fun tryIssue(eventId: Long, userId: Long): CouponIssueLuaResult {
        val result = redisTemplate.execute(
            issueScript,
            listOf(stockKey(eventId), issuedUsersKey(eventId)),
            userId.toString()
        )
        return CouponIssueLuaResult.valueOf(requireNotNull(result))
    }

    fun rollback(eventId: Long, userId: Long) {
        redisTemplate.opsForValue().increment(stockKey(eventId))
        redisTemplate.opsForSet().remove(issuedUsersKey(eventId), userId.toString())
    }

    fun seedStock(eventId: Long, totalQuantity: Int) {
        redisTemplate.opsForValue().set(stockKey(eventId), totalQuantity.toString())
        redisTemplate.delete(issuedUsersKey(eventId))
    }

    fun remainingStock(eventId: Long): Int =
        redisTemplate.opsForValue().get(stockKey(eventId))?.toInt() ?: 0

    private fun stockKey(eventId: Long) = "coupon:$eventId:stock"

    private fun issuedUsersKey(eventId: Long) = "coupon:$eventId:issued_users"

    companion object {
        private const val ISSUE_SCRIPT = """
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
        """
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponRedisIssuerTest"`
Expected: `BUILD SUCCESSFUL`, 4 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuer.kt \
        src/test/kotlin/jh/couponevent/coupon/redis/application/CouponRedisIssuerTest.kt
git commit -m "Add CouponRedisIssuer wrapping the atomic issuance Lua script"
```

---

### Task 4: 영속화 실패 예외 + 전역 예외 핸들러 확장

**Files:**
- Modify: `src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt`
- Modify: `src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt`
- Modify: `src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt`

**Interfaces:**
- Consumes: 없음
- Produces: `CouponIssuePersistenceFailedException(eventId: Long, userId: Long)` — Task 6이 Redis 성공 후 DB insert 실패 시 던짐. `GlobalExceptionHandler`가 500으로 매핑.

- [ ] **Step 1: 예외 클래스를 추가한다**

`src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt` 끝에 추가:

```kotlin

class CouponIssuePersistenceFailedException(eventId: Long, userId: Long) :
    RuntimeException(
        "Failed to persist issued coupon for event $eventId, user $userId after Redis stock was decremented"
    )
```

- [ ] **Step 2: 실패하는 테스트를 먼저 작성한다**

`src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt`에 테스트 메서드 추가(기존 import에 `CouponIssuePersistenceFailedException` 추가 필요):

```kotlin
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
```

기존 클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @Test
    fun `영속화 실패 예외는 500 INTERNAL_SERVER_ERROR, ISSUE_PERSISTENCE_FAILED 상태로 매핑된다`() {
        val response = handler.handlePersistenceFailure(CouponIssuePersistenceFailedException(1L, 100L))

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode)
        assertEquals("ISSUE_PERSISTENCE_FAILED", response.body?.status)
    }
```

- [ ] **Step 3: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"`
Expected: FAIL (`handlePersistenceFailure`가 아직 없어서 컴파일 에러)

- [ ] **Step 4: `GlobalExceptionHandler`에 핸들러를 추가한다**

`src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt`의 import에 추가:

```kotlin
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
```

클래스 본문 끝(마지막 `}` 앞)에 추가:

```kotlin

    @ExceptionHandler(CouponIssuePersistenceFailedException::class)
    fun handlePersistenceFailure(e: CouponIssuePersistenceFailedException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ErrorResponse("ISSUE_PERSISTENCE_FAILED", e.message.orEmpty()))
```

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"`
Expected: `BUILD SUCCESSFUL`, 3 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt \
        src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt \
        src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt
git commit -m "Add CouponIssuePersistenceFailedException mapped to 500"
```

---

### Task 5: v2 관리자 API (이벤트 생성 시 Redis 자동 시딩)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/api/dto/CouponEventDto.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/application/CouponEventAdminService.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/api/CouponEventAdminController.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/redis/api/CouponEventAdminControllerTest.kt`

**Interfaces:**
- Consumes: Task 2의 `CouponEventRepository`(공유), Task 3의 `CouponRedisIssuer.seedStock`/`remainingStock`, `jh.couponevent.coupon.exception.CouponEventNotFoundException`(공유)
- Produces:
  - `CreateCouponEventRequest(name: String, totalQuantity: Int, startAt: LocalDateTime)`
  - `CouponEventResponse(id: Long, name: String, totalQuantity: Int, issuedQuantity: Int, startAt: LocalDateTime)`
  - `CouponEventAdminService.create(request: CreateCouponEventRequest): CouponEventResponse`
  - `CouponEventAdminService.findAll(name: String?): List<CouponEventResponse>`
  - `CouponEventAdminService.findById(eventId: Long): CouponEventResponse`

- [ ] **Step 1: DTO를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.api.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CreateCouponEventRequest(
    @field:NotBlank
    val name: String,

    @field:Positive
    val totalQuantity: Int,

    val startAt: LocalDateTime
)

data class CouponEventResponse(
    val id: Long,
    val name: String,
    val totalQuantity: Int,
    val issuedQuantity: Int,
    val startAt: LocalDateTime
)
```

- [ ] **Step 2: 서비스를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CouponEventAdminService(
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer
) {
    @Transactional
    fun create(request: CreateCouponEventRequest): CouponEventResponse {
        val saved = couponEventRepository.save(
            CouponEvent(
                name = request.name,
                totalQuantity = request.totalQuantity,
                startAt = request.startAt
            )
        )
        val eventId = requireNotNull(saved.id)
        couponRedisIssuer.seedStock(eventId, request.totalQuantity)
        return saved.toResponse(issuedQuantity = 0)
    }

    fun findAll(name: String?): List<CouponEventResponse> {
        val events = if (name.isNullOrBlank()) {
            couponEventRepository.findAll()
        } else {
            couponEventRepository.findByNameContaining(name)
        }
        return events.map { it.toResponseWithRedisCount() }
    }

    fun findById(eventId: Long): CouponEventResponse {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }
        return event.toResponseWithRedisCount()
    }

    private fun CouponEvent.toResponseWithRedisCount(): CouponEventResponse {
        val eventId = requireNotNull(id)
        val remaining = couponRedisIssuer.remainingStock(eventId)
        return toResponse(issuedQuantity = totalQuantity - remaining)
    }

    private fun CouponEvent.toResponse(issuedQuantity: Int) = CouponEventResponse(
        id = requireNotNull(id),
        name = name,
        totalQuantity = totalQuantity,
        issuedQuantity = issuedQuantity,
        startAt = startAt
    )
}
```

- [ ] **Step 3: 컨트롤러를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.api

import jakarta.validation.Valid
import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.redis.application.CouponEventAdminService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v2/admin/coupon-events")
class CouponEventAdminController(
    private val couponEventAdminService: CouponEventAdminService
) {
    @PostMapping
    fun create(@Valid @RequestBody request: CreateCouponEventRequest): ResponseEntity<CouponEventResponse> =
        ResponseEntity.status(HttpStatus.CREATED).body(couponEventAdminService.create(request))

    @GetMapping
    fun list(@RequestParam(required = false) name: String?): List<CouponEventResponse> =
        couponEventAdminService.findAll(name)

    @GetMapping("/{eventId}")
    fun get(@PathVariable eventId: Long): CouponEventResponse =
        couponEventAdminService.findById(eventId)
}
```

- [ ] **Step 4: 컨트롤러 단위 테스트를 작성한다 (서비스는 mock)**

```kotlin
package jh.couponevent.coupon.redis.api

import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.redis.application.CouponEventAdminService
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.LocalDateTime
import kotlin.test.assertEquals

class CouponEventAdminControllerTest {

    private val couponEventAdminService: CouponEventAdminService = mock()
    private val controller = CouponEventAdminController(couponEventAdminService)

    @Test
    fun `이벤트 생성 요청을 서비스로 위임하고 201 응답을 만든다`() {
        val startAt = LocalDateTime.of(2026, 7, 11, 10, 0)
        val request = CreateCouponEventRequest(name = "여름 쿠폰 v2", totalQuantity = 10000, startAt = startAt)
        val response = CouponEventResponse(id = 1L, name = "여름 쿠폰 v2", totalQuantity = 10000, issuedQuantity = 0, startAt = startAt)
        whenever(couponEventAdminService.create(request)).thenReturn(response)

        val result = controller.create(request)

        assertEquals(201, result.statusCode.value())
        assertEquals(response, result.body)
    }

    @Test
    fun `목록 조회 요청을 서비스로 위임한다`() {
        whenever(couponEventAdminService.findAll(null)).thenReturn(emptyList())

        val result = controller.list(null)

        assertEquals(emptyList(), result)
    }
}
```

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.api.CouponEventAdminControllerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/redis/api/dto/CouponEventDto.kt \
        src/main/kotlin/jh/couponevent/coupon/redis/application/CouponEventAdminService.kt \
        src/main/kotlin/jh/couponevent/coupon/redis/api/CouponEventAdminController.kt \
        src/test/kotlin/jh/couponevent/coupon/redis/api/CouponEventAdminControllerTest.kt
git commit -m "Add v2 admin API that seeds Redis stock on event creation"
```

---

### Task 6: v2 쿠폰 발급 핵심 로직 (CouponIssueService)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/application/CouponIssueService.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/redis/application/CouponIssueServiceTest.kt`

**Interfaces:**
- Consumes: `CouponEventRepository`(공유), `IssuedCouponRepository`(공유), Task 3의 `CouponRedisIssuer`/`CouponIssueLuaResult`, Task 4의 `CouponIssuePersistenceFailedException`, `jh.couponevent.common.ClockConfig`의 `Clock` 빈(공유)
- Produces:
  - `CouponIssueResult(eventId: Long, userId: Long, issuedAt: LocalDateTime)`
  - `CouponIssueService.issue(eventId: Long, userId: Long): CouponIssueResult`

- [ ] **Step 1: 실패하는 테스트를 먼저 작성한다**

```kotlin
package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
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

class CouponIssueServiceTest {

    private val couponEventRepository: CouponEventRepository = mock()
    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val couponRedisIssuer: CouponRedisIssuer = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-07-11T10:00:00Z"), ZoneOffset.UTC)
    private val service = CouponIssueService(couponEventRepository, issuedCouponRepository, couponRedisIssuer, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        startAt: LocalDateTime = LocalDateTime.of(2026, 7, 11, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(name = "test-event", totalQuantity = 10, startAt = startAt)
        event.id = id
        return event
    }

    @Test
    fun `Redis가 SUCCESS를 반환하면 발급에 성공한다`() {
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
            .thenReturn(Optional.of(eventWithId(startAt = LocalDateTime.of(2026, 7, 11, 11, 0))))

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
    fun `DB insert가 실패하면 Redis를 롤백하고 CouponIssuePersistenceFailedException을 던진다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)
        whenever(issuedCouponRepository.save(any())).thenThrow(RuntimeException("DB down"))

        assertThrows<CouponIssuePersistenceFailedException> { service.issue(1L, 100L) }

        verify(couponRedisIssuer).rollback(1L, 100L)
    }
}
```

- [ ] **Step 2: 테스트 실행 → 컴파일 실패 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponIssueServiceTest"`
Expected: FAIL (`CouponIssueService`가 아직 없어서 컴파일 에러)

- [ ] **Step 3: `CouponIssueService`를 구현한다**

```kotlin
package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.LocalDateTime

data class CouponIssueResult(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

@Service
class CouponIssueService(
    private val couponEventRepository: CouponEventRepository,
    private val issuedCouponRepository: IssuedCouponRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val clock: Clock
) {
    fun issue(eventId: Long, userId: Long): CouponIssueResult {
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
            issuedCouponRepository.save(IssuedCoupon(couponEventId = eventId, userId = userId, issuedAt = now))
        } catch (e: Exception) {
            couponRedisIssuer.rollback(eventId, userId)
            throw CouponIssuePersistenceFailedException(eventId, userId)
        }

        return CouponIssueResult(eventId, userId, now)
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponIssueServiceTest"`
Expected: `BUILD SUCCESSFUL`, 6 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/redis/application/CouponIssueService.kt \
        src/test/kotlin/jh/couponevent/coupon/redis/application/CouponIssueServiceTest.kt
git commit -m "Add v2 CouponIssueService using Redis Lua for atomic issuance"
```

---

### Task 7: v2 쿠폰 발급 API (CouponIssueController)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/api/dto/CouponIssueDto.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/redis/api/CouponIssueController.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/redis/api/CouponIssueControllerTest.kt`

**Interfaces:**
- Consumes: Task 6의 `CouponIssueService.issue(...)`, `IssuedCouponRepository`(공유), `jh.couponevent.common.GlobalExceptionHandler`(공유, Task 4에서 확장됨)
- Produces:
  - `POST /api/v2/coupon-events/{eventId}/issue` → 200 `CouponIssueResponse` / 403 / 404 / 409 / 410 / 500
  - `GET /api/v2/coupon-events/{eventId}/users/{userId}/status` → 200 `CouponStatusResponse`

- [ ] **Step 1: DTO를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.api.dto

import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CouponIssueRequest(
    @field:Positive
    val userId: Long
)

data class CouponIssueResponse(
    val status: String,
    val issuedAt: LocalDateTime
)

data class CouponStatusResponse(
    val issued: Boolean
)
```

- [ ] **Step 2: 컨트롤러를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.api

import jakarta.validation.Valid
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.redis.api.dto.CouponIssueRequest
import jh.couponevent.coupon.redis.api.dto.CouponIssueResponse
import jh.couponevent.coupon.redis.api.dto.CouponStatusResponse
import jh.couponevent.coupon.redis.application.CouponIssueService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v2/coupon-events/{eventId}")
class CouponIssueController(
    private val couponIssueService: CouponIssueService,
    private val issuedCouponRepository: IssuedCouponRepository
) {
    @PostMapping("/issue")
    fun issue(@PathVariable eventId: Long, @Valid @RequestBody request: CouponIssueRequest): CouponIssueResponse {
        val result = couponIssueService.issue(eventId, request.userId)
        return CouponIssueResponse(status = "SUCCESS", issuedAt = result.issuedAt)
    }

    @GetMapping("/users/{userId}/status")
    fun status(@PathVariable eventId: Long, @PathVariable userId: Long): CouponStatusResponse {
        val issued = issuedCouponRepository.existsByCouponEventIdAndUserId(eventId, userId)
        return CouponStatusResponse(issued = issued)
    }
}
```

- [ ] **Step 3: `@WebMvcTest`로 예외 → HTTP 상태 매핑까지 검증하는 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.api

import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.redis.api.dto.CouponIssueRequest
import jh.couponevent.coupon.redis.application.CouponIssueResult
import jh.couponevent.coupon.redis.application.CouponIssueService
import jh.couponevent.coupon.domain.IssuedCouponRepository
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

@WebMvcTest(CouponIssueController::class)
class CouponIssueControllerTest @Autowired constructor(
    private val mockMvc: MockMvc
) {
    @MockitoBean
    private lateinit var couponIssueService: CouponIssueService

    @MockitoBean
    private lateinit var issuedCouponRepository: IssuedCouponRepository

    private val objectMapper = ObjectMapper()

    @Test
    fun `재고가 소진되면 410 GONE과 SOLD_OUT 상태를 반환한다`() {
        whenever(couponIssueService.issue(any(), any())).thenThrow(CouponSoldOutException(1L))

        mockMvc.post("/api/v2/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequest(userId = 100L))
        }.andExpect {
            status { isGone() }
            jsonPath("$.status") { value("SOLD_OUT") }
        }
    }

    @Test
    fun `발급에 성공하면 200과 SUCCESS 상태를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 7, 11, 10, 0)
        whenever(couponIssueService.issue(any(), any()))
            .thenReturn(CouponIssueResult(eventId = 1L, userId = 100L, issuedAt = issuedAt))

        mockMvc.post("/api/v2/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequest(userId = 100L))
        }.andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SUCCESS") }
        }
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.api.CouponIssueControllerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 전체 테스트 스위트 실행**

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/redis/api/dto/CouponIssueDto.kt \
        src/main/kotlin/jh/couponevent/coupon/redis/api/CouponIssueController.kt \
        src/test/kotlin/jh/couponevent/coupon/redis/api/CouponIssueControllerTest.kt
git commit -m "Add v2 coupon issue API endpoint"
```

---

### Task 8: 동시성 통합 테스트 (실제 Redis 원자성 검증)

**Files:**
- Test: `src/test/kotlin/jh/couponevent/coupon/redis/application/CouponIssueConcurrencyTest.kt`

**Interfaces:**
- Consumes: Task 6의 `CouponIssueService.issue(...)`, `CouponEventRepository`(공유), Task 3의 `CouponRedisIssuer`, `IssuedCouponRepository`(공유) — `@SpringBootTest`로 전체 컨텍스트를 띄워 실제 빈 사용
- Produces: 없음 (검증 전용 테스트)

- [ ] **Step 1: MySQL과 Redis가 떠 있는지 확인한다**

Run: `docker compose up -d && docker compose ps`
Expected: `mysql`, `redis` 서비스 모두 `running`

- [ ] **Step 2: 동시성 통합 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

@SpringBootTest
class CouponIssueConcurrencyTest @Autowired constructor(
    private val couponIssueService: CouponIssueService,
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동시에 200명이 요청해도 Redis 재고 100개만 정확히 발급된다`() {
        val totalQuantity = 100
        val requestCount = 200
        val event = couponEventRepository.save(
            CouponEvent(
                name = "redis-concurrency-test-${System.currentTimeMillis()}",
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

        assertEquals(totalQuantity, successCount.get())
        assertEquals(requestCount - totalQuantity, failCount.get())
        assertEquals(totalQuantity.toLong(), issuedCouponRepository.countByCouponEventId(eventId))
        assertEquals(0, couponRedisIssuer.remainingStock(eventId))
    }
}
```

- [ ] **Step 3: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.redis.application.CouponIssueConcurrencyTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed (정확히 100건 성공, 100건 실패, DB row 수 100, Redis 잔여 재고 0)

- [ ] **Step 4: 커밋**

```bash
git add src/test/kotlin/jh/couponevent/coupon/redis/application/CouponIssueConcurrencyTest.kt
git commit -m "Add concurrency integration test against real Redis"
```

---

### Task 9: k6 부하테스트 (~1,000 TPS 실측)

**Files:**
- Create: `load-test/k6/phase2-issue.js`
- Modify: `load-test/README.md`

**Interfaces:**
- Consumes: Task 7의 `POST /api/v2/coupon-events/{eventId}/issue`
- Produces: 없음 (실측/관찰용 산출물)

- [ ] **Step 1: 애플리케이션을 실행한다**

Run: `docker compose up -d && ./gradlew bootRun`
Expected: `Started CouponEventApplicationKt` 로그, 포트 8080에서 서비스 중

- [ ] **Step 2: 별도 터미널에서 v2 테스트용 이벤트를 생성한다 (재고를 넉넉히 잡아 품절이 아니라 처리량 자체를 관찰)**

Run:
```bash
curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"phase2-load-test","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
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
    phase2_1000tps: {
      executor: 'constant-arrival-rate',
      rate: 1000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
    },
  },
};

export default function () {
  const userId = (Date.now() % 1000000000) + __VU * 100000 + __ITER;
  const payload = JSON.stringify({ userId });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const res = http.post(`${BASE_URL}/api/v2/coupon-events/${EVENT_ID}/issue`, payload, params);

  check(res, {
    'status is 200, 409, or 410': (r) => [200, 409, 410].includes(r.status),
  });
}
```

- [ ] **Step 4: k6를 실행한다**

Run: `EVENT_ID=<Step 2에서 확인한 id> k6 run load-test/k6/phase2-issue.js`
Expected: 콘솔에 `http_req_duration`, `checks` 등 요약 통계가 출력된다.

- [ ] **Step 5: 실행 결과를 관찰하고 기록한다**

Phase 1(`load-test/README.md`의 실측 결과: p95 1.04s, `dropped_iterations` 67건)과 비교해서, p95/p99 지연 시간이 훨씬 낮게 유지되는지, `dropped_iterations`가 크게 줄어드는지, 실제 처리량이 1,000 req/s에 얼마나 근접하는지 확인한다.

- [ ] **Step 6: `load-test/README.md`에 Phase 2 섹션을 추가한다**

파일 끝에 추가:

```markdown

## Phase 2: Redis Lua Script (~1,000 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (재고를 넉넉히 잡아 품절이 아닌 처리량 자체를 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/v2/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase2-load-test","totalQuantity":50000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase2-issue.js`
4. 관찰 포인트: Phase 1(p95 1.04s, dropped_iterations 67건) 대비 p95/p99 응답시간과 dropped_iterations가 얼마나 줄어드는지, 실제 처리량이 1,000 req/s에 얼마나 근접하는지
```

- [ ] **Step 7: 커밋**

```bash
git add load-test/k6/phase2-issue.js load-test/README.md
git commit -m "Add k6 load test script for Phase 2 (~1,000 TPS)"
```

---

### Task 10: 손으로 확인하는 수동 테스트 가이드 작성

**Files:**
- Create: `docs/superpowers/reports/2026-07-11-phase2-redis-lua/manual-testing-guide.md`

**Interfaces:**
- Consumes: 앞선 모든 Task의 API 엔드포인트
- Produces: 없음 (문서 산출물)

Phase 1의 `docs/superpowers/reports/2026-07-05-phase1-mysql-lock/manual-testing-guide.md`와 같은 형식으로, v1과 v2를 나란히 손으로 호출해 차이를 직접 비교해볼 수 있는 가이드를 만든다.

- [ ] **Step 1: 가이드 문서를 작성한다**

```markdown
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
```

- [ ] **Step 2: 커밋**

```bash
git add docs/superpowers/reports/2026-07-11-phase2-redis-lua/manual-testing-guide.md
git commit -m "Add manual testing guide for hands-on verification of Phase 2"
```

---

## 완료 후 다음 단계

Phase 2가 끝나면 실측한 처리량 개선을 근거로 Phase 3(Redis 동기 차감 + Kafka 비동기 영속화) 계획을 별도 문서로 작성한다. Phase 3는 이 계획의 범위 밖이다.

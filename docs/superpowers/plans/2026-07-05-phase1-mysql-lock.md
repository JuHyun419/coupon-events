# Phase 1 (MySQL 비관적 락 기반 쿠폰 발급) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** MySQL `SELECT ... FOR UPDATE` 비관적 락으로 재고를 직렬화하는 가장 단순한 선착순 쿠폰 발급 API를 만들고, ~100 TPS 부하테스트로 락 경합 병목을 실측한다.

**Architecture:** Spring Boot + Kotlin + JPA 3계층(controller/application/domain) 구조. `CouponEvent`의 재고 행을 `FOR UPDATE`로 잠근 뒤 재고 확인 → 중복 발급 확인 → `IssuedCoupon` insert → 재고 증가를 하나의 트랜잭션으로 묶는다. 인증은 없고 `userId`는 요청 바디로 직접 받는다.

**Tech Stack:** Kotlin 2.3.21, Spring Boot 4.1.0 (Web, Data JPA, Validation), MySQL 8.0(docker-compose), Mockito + mockito-kotlin, k6.

## Global Constraints

- 참조 스펙: `docs/superpowers/specs/2026-07-04-coupon-event-design.md`
- 인증/인가 없음 — `userId`는 요청 바디에서 직접 받는다.
- `User` 테이블 없음, 쿠폰 "사용(redeem)" 흐름 없음 — 발급 기록의 존재 여부로 발급 여부를 판단한다.
- 데이터베이스는 MySQL만 사용한다. Redis/Kafka는 이 계획의 범위 밖(Phase 2/3에서 별도 계획으로 추가).
- Testcontainers는 쓰지 않는다 — 모든 DB 관련 테스트는 로컬 docker-compose로 띄운 실제 MySQL에 직접 연결해서 검증한다.
- QueryDSL은 아직 도입하지 않는다 — 관리자 조회는 단순 파생 쿼리로 충분하다(YAGNI, 필요해지면 나중에 추가).
- 1인 1매 제한은 `issued_coupon(coupon_event_id, user_id)` UNIQUE 제약이 최종 방어선이다.
- 이벤트 생성/구성 API는 `/api/admin/coupon-events`, 발급 API는 `/api/coupon-events/{eventId}/issue`, 상태 조회는 `/api/coupon-events/{eventId}/users/{userId}/status`.

---

### Task 1: 의존성 추가 + 로컬 MySQL 인프라 구성

**Files:**
- Modify: `build.gradle.kts`
- Create: `docker-compose.yml`
- Create: `src/main/resources/application.yml`
- Delete: `src/main/resources/application.properties`

**Interfaces:**
- Consumes: 없음 (최초 작업)
- Produces: `spring.datasource.*` 설정(다음 Task들이 이 설정을 통해 로컬 MySQL에 연결), docker-compose MySQL 서비스(호스트 `localhost:3306`, DB `coupon_event`, 계정 `coupon`/`coupon`)

- [ ] **Step 1: `build.gradle.kts`에 JPA/Web/Validation/MySQL/테스트 의존성을 추가한다**

```kotlin
plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    kotlin("plugin.jpa") version "2.3.21"
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "jh"
version = "0.0.1-SNAPSHOT"
description = "coupon-event"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    runtimeOnly("com.mysql:mysql-connector-j")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
```

`kotlin("plugin.jpa")`가 빠지면 Kotlin으로 작성한 `@Entity` 클래스에 Hibernate가 요구하는 no-arg 생성자가 없어서 런타임에 실패하므로 반드시 추가한다.

- [ ] **Step 2: 저장소 루트에 `docker-compose.yml`을 만든다**

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

volumes:
  mysql-data:
```

- [ ] **Step 3: `application.properties`를 삭제하고 `application.yml`을 만든다**

```yaml
spring:
  application:
    name: coupon-event
  datasource:
    url: jdbc:mysql://localhost:3306/coupon_event?useSSL=false&serverTimezone=UTC
    username: coupon
    password: coupon
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
    properties:
      hibernate:
        format_sql: true
```

`open-in-view: false`로 두어서 트랜잭션 경계를 서비스 계층에서 명시적으로 관리한다(지연 로딩을 컨트롤러/뷰까지 끌고 가지 않음).

- [ ] **Step 4: MySQL 컨테이너를 띄우고 정상 기동을 확인한다**

Run: `docker compose up -d && sleep 5 && docker compose ps`
Expected: `mysql` 서비스 상태가 `running` (또는 `healthy`)

- [ ] **Step 5: 빌드가 새 의존성으로 성공하는지 확인한다**

Run: `./gradlew build -x test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 커밋**

```bash
git add build.gradle.kts docker-compose.yml src/main/resources/application.yml
git rm src/main/resources/application.properties
git commit -m "Add JPA/Web/MySQL dependencies and local docker-compose infra"
```

---

### Task 2: 도메인 엔티티 + 리포지토리 (CouponEvent, IssuedCoupon)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/domain/CouponEvent.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/domain/IssuedCoupon.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/domain/CouponEventRepository.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/domain/IssuedCouponRepository.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/domain/IssuedCouponRepositoryTest.kt`

**Interfaces:**
- Consumes: Task 1의 `spring.datasource` 설정
- Produces:
  - `CouponEvent(name: String, totalQuantity: Int, startAt: LocalDateTime, issuedQuantity: Int = 0, createdAt: LocalDateTime = LocalDateTime.now())` — `var id: Long?`, `var issuedQuantity: Int`
  - `IssuedCoupon(couponEventId: Long, userId: Long, issuedAt: LocalDateTime = LocalDateTime.now())` — `var id: Long?`
  - `CouponEventRepository : JpaRepository<CouponEvent, Long>` — `findByIdForUpdate(id: Long): CouponEvent?`, `findByNameContaining(name: String): List<CouponEvent>`
  - `IssuedCouponRepository : JpaRepository<IssuedCoupon, Long>` — `existsByCouponEventIdAndUserId(couponEventId: Long, userId: Long): Boolean`, `countByCouponEventId(couponEventId: Long): Long`

- [ ] **Step 1: `CouponEvent` 엔티티를 작성한다**

```kotlin
package jh.couponevent.coupon.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "coupon_event")
class CouponEvent(
    @Column(nullable = false)
    val name: String,

    @Column(name = "total_quantity", nullable = false)
    val totalQuantity: Int,

    @Column(name = "start_at", nullable = false)
    val startAt: LocalDateTime,

    @Column(name = "issued_quantity", nullable = false)
    var issuedQuantity: Int = 0,

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now()
) {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null
}
```

- [ ] **Step 2: `IssuedCoupon` 엔티티를 작성한다**

```kotlin
package jh.couponevent.coupon.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.time.LocalDateTime

@Entity
@Table(
    name = "issued_coupon",
    uniqueConstraints = [
        UniqueConstraint(name = "uk_event_user", columnNames = ["coupon_event_id", "user_id"])
    ]
)
class IssuedCoupon(
    @Column(name = "coupon_event_id", nullable = false)
    val couponEventId: Long,

    @Column(name = "user_id", nullable = false)
    val userId: Long,

    @Column(name = "issued_at", nullable = false)
    val issuedAt: LocalDateTime = LocalDateTime.now()
) {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null
}
```

- [ ] **Step 3: 리포지토리 2개를 작성한다**

```kotlin
package jh.couponevent.coupon.domain

import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface CouponEventRepository : JpaRepository<CouponEvent, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from CouponEvent c where c.id = :id")
    fun findByIdForUpdate(@Param("id") id: Long): CouponEvent?

    fun findByNameContaining(name: String): List<CouponEvent>
}
```

```kotlin
package jh.couponevent.coupon.domain

import org.springframework.data.jpa.repository.JpaRepository

interface IssuedCouponRepository : JpaRepository<IssuedCoupon, Long> {
    fun existsByCouponEventIdAndUserId(couponEventId: Long, userId: Long): Boolean
    fun countByCouponEventId(couponEventId: Long): Long
}
```

- [ ] **Step 4: 실제 MySQL(docker-compose)에 붙는 리포지토리 테스트를 작성한다**

먼저 MySQL이 떠 있는지 확인: `docker compose up -d` (Task 1에서 이미 띄웠다면 생략 가능)

```kotlin
package jh.couponevent.coupon.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.dao.DataIntegrityViolationException
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class IssuedCouponRepositoryTest @Autowired constructor(
    private val couponEventRepository: CouponEventRepository,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동일한 이벤트-사용자 조합은 중복 저장할 수 없다`() {
        val event = couponEventRepository.save(
            CouponEvent(name = "unique-test", totalQuantity = 10, startAt = LocalDateTime.now())
        )
        issuedCouponRepository.saveAndFlush(
            IssuedCoupon(couponEventId = requireNotNull(event.id), userId = 1L)
        )

        assertThrows<DataIntegrityViolationException> {
            issuedCouponRepository.saveAndFlush(
                IssuedCoupon(couponEventId = requireNotNull(event.id), userId = 1L)
            )
        }
    }

    @Test
    fun `findByIdForUpdate는 존재하는 이벤트를 반환한다`() {
        val event = couponEventRepository.save(
            CouponEvent(name = "lock-test", totalQuantity = 10, startAt = LocalDateTime.now())
        )

        val found = couponEventRepository.findByIdForUpdate(requireNotNull(event.id))

        assertNotNull(found)
        assertEquals("lock-test", found?.name)
    }
}
```

`@AutoConfigureTestDatabase(replace = NONE)`이 없으면 Spring이 임베디드 DB로 바꾸려다 실패한다(H2를 의존성에 추가하지 않았으므로) — 반드시 실제 `application.yml`의 MySQL 설정을 그대로 쓰도록 이 어노테이션을 넣는다.

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.domain.IssuedCouponRepositoryTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/domain src/test/kotlin/jh/couponevent/coupon/domain
git commit -m "Add CouponEvent/IssuedCoupon entities and repositories"
```

---

### Task 3: 예외 클래스 + 전역 예외 핸들러

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/exception/CouponExceptions.kt`
- Create: `src/main/kotlin/jh/couponevent/common/GlobalExceptionHandler.kt`
- Test: `src/test/kotlin/jh/couponevent/common/GlobalExceptionHandlerTest.kt`

**Interfaces:**
- Consumes: 없음
- Produces:
  - `CouponEventNotFoundException(eventId: Long)`, `CouponEventNotOpenException(eventId: Long)`, `CouponSoldOutException(eventId: Long)`, `DuplicateCouponIssueException(eventId: Long, userId: Long)` — 모두 `RuntimeException`
  - `ErrorResponse(status: String, message: String)`
  - `GlobalExceptionHandler` — 각 예외를 404/403/410/409로 매핑

- [ ] **Step 1: 예외 클래스를 작성한다**

```kotlin
package jh.couponevent.coupon.exception

class CouponEventNotFoundException(eventId: Long) : RuntimeException("Coupon event not found: $eventId")

class CouponEventNotOpenException(eventId: Long) : RuntimeException("Coupon event not open yet: $eventId")

class CouponSoldOutException(eventId: Long) : RuntimeException("Coupon event sold out: $eventId")

class DuplicateCouponIssueException(eventId: Long, userId: Long) :
    RuntimeException("User $userId already issued a coupon for event $eventId")
```

- [ ] **Step 2: 전역 예외 핸들러를 작성한다**

```kotlin
package jh.couponevent.common

import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

data class ErrorResponse(val status: String, val message: String)

@RestControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(CouponEventNotFoundException::class)
    fun handleNotFound(e: CouponEventNotFoundException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse("NOT_FOUND", e.message.orEmpty()))

    @ExceptionHandler(CouponEventNotOpenException::class)
    fun handleNotOpen(e: CouponEventNotOpenException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.FORBIDDEN).body(ErrorResponse("NOT_OPEN_YET", e.message.orEmpty()))

    @ExceptionHandler(DuplicateCouponIssueException::class)
    fun handleDuplicate(e: DuplicateCouponIssueException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.CONFLICT).body(ErrorResponse("DUPLICATE", e.message.orEmpty()))

    @ExceptionHandler(CouponSoldOutException::class)
    fun handleSoldOut(e: CouponSoldOutException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.GONE).body(ErrorResponse("SOLD_OUT", e.message.orEmpty()))
}
```

- [ ] **Step 3: 실패하는 테스트를 먼저 작성한다**

```kotlin
package jh.couponevent.common

import jh.couponevent.coupon.exception.CouponSoldOutException
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import kotlin.test.assertEquals

class GlobalExceptionHandlerTest {
    private val handler = GlobalExceptionHandler()

    @Test
    fun `품절 예외는 410 GONE, SOLD_OUT 상태로 매핑된다`() {
        val response = handler.handleSoldOut(CouponSoldOutException(1L))

        assertEquals(HttpStatus.GONE, response.statusCode)
        assertEquals("SOLD_OUT", response.body?.status)
    }

    @Test
    fun `중복 발급 예외는 409 CONFLICT, DUPLICATE 상태로 매핑된다`() {
        val response = handler.handleDuplicate(
            jh.couponevent.coupon.exception.DuplicateCouponIssueException(1L, 100L)
        )

        assertEquals(HttpStatus.CONFLICT, response.statusCode)
        assertEquals("DUPLICATE", response.body?.status)
    }
}
```

- [ ] **Step 4: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/exception src/main/kotlin/jh/couponevent/common src/test/kotlin/jh/couponevent/common
git commit -m "Add coupon domain exceptions and global exception handler"
```

---

### Task 4: 관리자 API (이벤트 생성/조회)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/api/dto/CouponEventDto.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/application/CouponEventAdminService.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/api/CouponEventAdminController.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/api/CouponEventAdminControllerTest.kt`

**Interfaces:**
- Consumes: Task 2의 `CouponEventRepository`, Task 3의 `CouponEventNotFoundException`
- Produces:
  - `CreateCouponEventRequest(name: String, totalQuantity: Int, startAt: LocalDateTime)`
  - `CouponEventResponse(id: Long, name: String, totalQuantity: Int, issuedQuantity: Int, startAt: LocalDateTime)`
  - `CouponEventAdminService.create(request: CreateCouponEventRequest): CouponEventResponse`
  - `CouponEventAdminService.findAll(name: String?): List<CouponEventResponse>`
  - `CouponEventAdminService.findById(eventId: Long): CouponEventResponse`

- [ ] **Step 1: DTO를 작성한다**

```kotlin
package jh.couponevent.coupon.api.dto

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
package jh.couponevent.coupon.application

import jh.couponevent.coupon.api.dto.CouponEventResponse
import jh.couponevent.coupon.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CouponEventAdminService(
    private val couponEventRepository: CouponEventRepository
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
        return saved.toResponse()
    }

    @Transactional(readOnly = true)
    fun findAll(name: String?): List<CouponEventResponse> {
        val events = if (name.isNullOrBlank()) {
            couponEventRepository.findAll()
        } else {
            couponEventRepository.findByNameContaining(name)
        }
        return events.map { it.toResponse() }
    }

    @Transactional(readOnly = true)
    fun findById(eventId: Long): CouponEventResponse {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }
        return event.toResponse()
    }
}

private fun CouponEvent.toResponse() = CouponEventResponse(
    id = requireNotNull(id),
    name = name,
    totalQuantity = totalQuantity,
    issuedQuantity = issuedQuantity,
    startAt = startAt
)
```

- [ ] **Step 3: 컨트롤러를 작성한다**

```kotlin
package jh.couponevent.coupon.api

import jakarta.validation.Valid
import jh.couponevent.coupon.api.dto.CouponEventResponse
import jh.couponevent.coupon.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.application.CouponEventAdminService
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
@RequestMapping("/api/admin/coupon-events")
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

- [ ] **Step 4: 컨트롤러 단위 테스트를 작성한다 (Spring 컨텍스트 없이, 서비스는 mock)**

```kotlin
package jh.couponevent.coupon.api

import jh.couponevent.coupon.api.dto.CouponEventResponse
import jh.couponevent.coupon.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.application.CouponEventAdminService
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
        val startAt = LocalDateTime.of(2026, 7, 5, 10, 0)
        val request = CreateCouponEventRequest(name = "여름 쿠폰", totalQuantity = 10000, startAt = startAt)
        val response = CouponEventResponse(id = 1L, name = "여름 쿠폰", totalQuantity = 10000, issuedQuantity = 0, startAt = startAt)
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

Run: `./gradlew test --tests "jh.couponevent.coupon.api.CouponEventAdminControllerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/api src/main/kotlin/jh/couponevent/coupon/application src/test/kotlin/jh/couponevent/coupon/api
git commit -m "Add coupon event admin API (create/list/get)"
```

---

### Task 5: 쿠폰 발급 핵심 로직 (CouponIssueService)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/common/ClockConfig.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/application/CouponIssueService.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/application/CouponIssueServiceTest.kt`

**Interfaces:**
- Consumes: Task 2의 `CouponEventRepository`/`IssuedCouponRepository`, Task 3의 예외 클래스
- Produces:
  - `Clock` 빈 (`jh.couponevent.common.ClockConfig`)
  - `CouponIssueResult(eventId: Long, userId: Long, issuedAt: LocalDateTime)`
  - `CouponIssueService.issue(eventId: Long, userId: Long): CouponIssueResult` — 실패 시 Task 3의 예외를 던짐

- [ ] **Step 1: 테스트에서 시간을 고정하기 위한 `Clock` 빈을 등록한다**

```kotlin
package jh.couponevent.common

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class ClockConfig {
    @Bean
    fun clock(): Clock = Clock.systemDefaultZone()
}
```

- [ ] **Step 2: 실패하는 테스트를 먼저 작성한다**

```kotlin
package jh.couponevent.coupon.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals

class CouponIssueServiceTest {

    private val couponEventRepository: CouponEventRepository = mock()
    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-07-05T10:00:00Z"), ZoneOffset.UTC)
    private val service = CouponIssueService(couponEventRepository, issuedCouponRepository, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        totalQuantity: Int = 10,
        issuedQuantity: Int = 0,
        startAt: LocalDateTime = LocalDateTime.of(2026, 7, 5, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(
            name = "test-event",
            totalQuantity = totalQuantity,
            startAt = startAt,
            issuedQuantity = issuedQuantity
        )
        event.id = id
        return event
    }

    @Test
    fun `재고가 남아있고 미발급 사용자면 발급에 성공한다`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(eventWithId())
        whenever(issuedCouponRepository.existsByCouponEventIdAndUserId(1L, 100L)).thenReturn(false)

        val result = service.issue(1L, 100L)

        assertEquals(1L, result.eventId)
        assertEquals(100L, result.userId)
    }

    @Test
    fun `이벤트가 존재하지 않으면 CouponEventNotFoundException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(null)

        assertThrows<CouponEventNotFoundException> { service.issue(1L, 100L) }
    }

    @Test
    fun `오픈 시각 이전이면 CouponEventNotOpenException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L))
            .thenReturn(eventWithId(startAt = LocalDateTime.of(2026, 7, 5, 11, 0)))

        assertThrows<CouponEventNotOpenException> { service.issue(1L, 100L) }
    }

    @Test
    fun `재고가 소진되었으면 CouponSoldOutException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L))
            .thenReturn(eventWithId(totalQuantity = 10, issuedQuantity = 10))

        assertThrows<CouponSoldOutException> { service.issue(1L, 100L) }
    }

    @Test
    fun `이미 발급받은 사용자면 DuplicateCouponIssueException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(eventWithId())
        whenever(issuedCouponRepository.existsByCouponEventIdAndUserId(1L, 100L)).thenReturn(true)

        assertThrows<DuplicateCouponIssueException> { service.issue(1L, 100L) }
    }
}
```

- [ ] **Step 3: 테스트 실행 → 컴파일 실패(또는 클래스 없음) 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueServiceTest"`
Expected: FAIL (`CouponIssueService`가 아직 없어서 컴파일 에러)

- [ ] **Step 4: `CouponIssueService`를 구현한다**

```kotlin
package jh.couponevent.coupon.application

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
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
    private val clock: Clock
) {
    @Transactional
    fun issue(eventId: Long, userId: Long): CouponIssueResult {
        val event = couponEventRepository.findByIdForUpdate(eventId)
            ?: throw CouponEventNotFoundException(eventId)

        val now = LocalDateTime.now(clock)
        if (now.isBefore(event.startAt)) {
            throw CouponEventNotOpenException(eventId)
        }

        if (event.issuedQuantity >= event.totalQuantity) {
            throw CouponSoldOutException(eventId)
        }

        if (issuedCouponRepository.existsByCouponEventIdAndUserId(eventId, userId)) {
            throw DuplicateCouponIssueException(eventId, userId)
        }

        issuedCouponRepository.save(IssuedCoupon(couponEventId = eventId, userId = userId, issuedAt = now))
        event.issuedQuantity += 1

        return CouponIssueResult(eventId, userId, now)
    }
}
```

재고 확인이 중복 확인보다 먼저 오는 순서는 설계 문서(`docs/superpowers/specs/2026-07-04-coupon-event-design.md` Phase 1 섹션)의 순서와 일치시킨 것이다.

- [ ] **Step 5: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueServiceTest"`
Expected: `BUILD SUCCESSFUL`, 5 tests passed

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/common/ClockConfig.kt src/main/kotlin/jh/couponevent/coupon/application/CouponIssueService.kt src/test/kotlin/jh/couponevent/coupon/application
git commit -m "Add CouponIssueService with pessimistic-lock issuance logic"
```

---

### Task 6: 쿠폰 발급 API (CouponIssueController)

**Files:**
- Create: `src/main/kotlin/jh/couponevent/coupon/api/dto/CouponIssueDto.kt`
- Create: `src/main/kotlin/jh/couponevent/coupon/api/CouponIssueController.kt`
- Test: `src/test/kotlin/jh/couponevent/coupon/api/CouponIssueControllerTest.kt`

**Interfaces:**
- Consumes: Task 5의 `CouponIssueService.issue(...)`, Task 2의 `IssuedCouponRepository.existsByCouponEventIdAndUserId(...)`, Task 3의 `GlobalExceptionHandler`
- Produces:
  - `POST /api/coupon-events/{eventId}/issue` → 200 `CouponIssueResponse` / 403 / 409 / 410 / 404 (Task 3 핸들러가 처리)
  - `GET /api/coupon-events/{eventId}/users/{userId}/status` → 200 `CouponStatusResponse`

- [ ] **Step 1: DTO를 작성한다**

```kotlin
package jh.couponevent.coupon.api.dto

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
package jh.couponevent.coupon.api

import jakarta.validation.Valid
import jh.couponevent.coupon.api.dto.CouponIssueRequest
import jh.couponevent.coupon.api.dto.CouponIssueResponse
import jh.couponevent.coupon.api.dto.CouponStatusResponse
import jh.couponevent.coupon.application.CouponIssueService
import jh.couponevent.coupon.domain.IssuedCouponRepository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/coupon-events/{eventId}")
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
package jh.couponevent.coupon.api

import com.fasterxml.jackson.databind.ObjectMapper
import jh.couponevent.coupon.api.dto.CouponIssueRequest
import jh.couponevent.coupon.application.CouponIssueResult
import jh.couponevent.coupon.application.CouponIssueService
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponSoldOutException
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
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

        mockMvc.post("/api/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequest(userId = 100L))
        }.andExpect {
            status { isGone() }
            jsonPath("$.status") { value("SOLD_OUT") }
        }
    }

    @Test
    fun `발급에 성공하면 200과 SUCCESS 상태를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 7, 5, 10, 0)
        whenever(couponIssueService.issue(any(), any()))
            .thenReturn(CouponIssueResult(eventId = 1L, userId = 100L, issuedAt = issuedAt))

        mockMvc.post("/api/coupon-events/1/issue") {
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

Run: `./gradlew test --tests "jh.couponevent.coupon.api.CouponIssueControllerTest"`
Expected: `BUILD SUCCESSFUL`, 2 tests passed

- [ ] **Step 5: 전체 테스트 스위트를 돌려 지금까지의 작업이 서로 깨뜨리지 않았는지 확인한다**

Run: `./gradlew test`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 6: 커밋**

```bash
git add src/main/kotlin/jh/couponevent/coupon/api/dto/CouponIssueDto.kt src/main/kotlin/jh/couponevent/coupon/api/CouponIssueController.kt src/test/kotlin/jh/couponevent/coupon/api/CouponIssueControllerTest.kt
git commit -m "Add coupon issue API endpoint with exception-to-status mapping"
```

---

### Task 7: 동시성 통합 테스트 (실제 MySQL 락 경합 검증)

**Files:**
- Test: `src/test/kotlin/jh/couponevent/coupon/application/CouponIssueConcurrencyTest.kt`

**Interfaces:**
- Consumes: Task 5의 `CouponIssueService.issue(...)`, Task 2의 리포지토리들 (`@SpringBootTest`로 전체 컨텍스트를 띄워 실제 빈을 사용)
- Produces: 없음 (검증 전용 테스트)

- [ ] **Step 1: MySQL이 떠 있는지 확인한다**

Run: `docker compose up -d && docker compose ps`
Expected: `mysql` 서비스 `running`

- [ ] **Step 2: 동시성 통합 테스트를 작성한다**

```kotlin
package jh.couponevent.coupon.application

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
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동시에 200명이 요청해도 재고 100개만 정확히 발급된다`() {
        val totalQuantity = 100
        val requestCount = 200
        val event = couponEventRepository.save(
            CouponEvent(
                name = "concurrency-test-${System.currentTimeMillis()}",
                totalQuantity = totalQuantity,
                startAt = LocalDateTime.now().minusMinutes(1)
            )
        )
        val eventId = requireNotNull(event.id)

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
    }
}
```

이 테스트는 매 실행마다 새 이벤트 row를 만들어 DB에 데이터가 누적된다(학습용 로컬 DB이므로 정리는 선택 사항). 필요하면 `docker compose down -v && docker compose up -d`로 초기화한다.

- [ ] **Step 3: 테스트 실행 및 통과 확인**

Run: `./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueConcurrencyTest"`
Expected: `BUILD SUCCESSFUL`, 1 test passed (정확히 100건 성공, 100건 실패, DB row 수 100)

- [ ] **Step 4: 커밋**

```bash
git add src/test/kotlin/jh/couponevent/coupon/application/CouponIssueConcurrencyTest.kt
git commit -m "Add concurrency integration test against real MySQL"
```

---

### Task 8: k6 부하테스트 (~100 TPS 병목 실측)

**Files:**
- Create: `load-test/k6/phase1-issue.js`
- Create: `load-test/README.md`

**Interfaces:**
- Consumes: Task 6의 `POST /api/coupon-events/{eventId}/issue`
- Produces: 없음 (실측/관찰용 산출물)

- [ ] **Step 1: 애플리케이션을 실행한다**

Run: `docker compose up -d && ./gradlew bootRun`
Expected: `Started CouponEventApplication` 로그, 포트 8080에서 서비스 중

- [ ] **Step 2: 별도 터미널에서 테스트용 이벤트를 생성한다 (재고를 넉넉히 잡아 "품절"이 아니라 "락 경합"을 관찰하기 위함)**

Run:
```bash
curl -X POST http://localhost:8080/api/admin/coupon-events \
  -H "Content-Type: application/json" \
  -d '{"name":"phase1-load-test","totalQuantity":5000,"startAt":"2020-01-01T00:00:00"}'
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
    phase1_100tps: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 300,
    },
  },
};

export default function () {
  const userId = (Date.now() % 1000000000) + __VU * 100000 + __ITER;
  const payload = JSON.stringify({ userId });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const res = http.post(`${BASE_URL}/api/coupon-events/${EVENT_ID}/issue`, payload, params);

  check(res, {
    'status is 200, 409, or 410': (r) => [200, 409, 410].includes(r.status),
  });
}
```

- [ ] **Step 4: k6를 설치하고(미설치 시) 실행한다**

Run: `brew install k6` (이미 설치되어 있으면 생략)
Run: `EVENT_ID=<Step 2에서 확인한 id> k6 run load-test/k6/phase1-issue.js`
Expected: 콘솔에 `http_req_duration`, `checks` 등 요약 통계가 출력된다.

- [ ] **Step 5: 실행 결과를 관찰하고 기록한다**

TPS가 100에 가까워질수록 `http_req_duration`의 p95/p99가 급격히 늘어나거나, 서버 로그에 HikariCP 커넥션 타임아웃(`Connection is not available, request timed out`)이 찍히는지 확인한다. 이것이 Phase 1의 예상된 병목이며, 다음 단계(Phase 2: Redis Lua Script)로 넘어갈 근거가 된다.

- [ ] **Step 6: `load-test/README.md`에 실행 방법을 정리한다**

```markdown
# 부하 테스트 (k6)

## Phase 1: MySQL 비관적 락 (~100 TPS)

1. `docker compose up -d && ./gradlew bootRun`
2. 테스트용 이벤트 생성 (재고를 넉넉히 잡아 품절이 아닌 락 경합을 관찰):
   ```bash
   curl -X POST http://localhost:8080/api/admin/coupon-events \
     -H "Content-Type: application/json" \
     -d '{"name":"phase1-load-test","totalQuantity":5000,"startAt":"2020-01-01T00:00:00"}'
   ```
3. 실행: `EVENT_ID=<생성된 id> k6 run load-test/k6/phase1-issue.js`
4. 관찰 포인트: TPS가 100에 가까워질수록 p95/p99 응답시간 증가, HikariCP 커넥션 타임아웃 로그 발생 여부
```

- [ ] **Step 7: 커밋**

```bash
git add load-test/
git commit -m "Add k6 load test script for Phase 1 bottleneck observation"
```

---

## 완료 후 다음 단계

Phase 1이 끝나면 실측한 병목(락 경합, 커넥션 풀 고갈)을 근거로 Phase 2(Redis Lua Script 원자적 차감) 계획을 별도 문서로 작성한다. Phase 2/3는 이 계획의 범위 밖이다.

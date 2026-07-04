# 개발 가이드라인

## 아키텍처

Spring Boot + Kotlin 기반 푸시 알림 서비스. Layered Architecture를 따른다.

```
domain/         # 엔티티, enum, 리포지터리 인터페이스 — 순수 비즈니스 규칙
application/    # 서비스, DTO — 유스케이스 조합
infrastructure/ # Kafka, Redis, 외부 클라이언트 — 기술 세부 구현
scheduler/      # 스케줄러 — 트리거 역할만, 로직은 application으로 위임
config/         # 설정 클래스
```

**의존성 방향**: `infrastructure` / `scheduler` → `application` → `domain`

### 레이어 책임 원칙
- `domain`에는 Spring 의존성(`@Service`, `@Component`)을 두지 않는다.
- `scheduler`는 트리거 역할만 하며, 비즈니스 로직은 `application` 서비스에 위임한다.
- `infrastructure`는 인터페이스로 추상화하고, `application`은 구현체를 직접 참조하지 않는다.
- DTO는 레이어 경계에서만 사용한다. 도메인 엔티티를 외부로 노출하지 않는다.

---

## Kotlin 관용구

### null 처리
```kotlin
// 지양
val user = repository.findById(id)!!

// 권장
val user = repository.findById(id) ?: throw IllegalArgumentException("User not found: $id")

// nullable 체인
val name = user?.profile?.name ?: "unknown"
```

### 스코프 함수 기준
| 함수 | 사용 기준 |
|------|----------|
| `let` | nullable 처리, 결과를 변환할 때 |
| `also` | 사이드 이펙트(로깅 등), 원본 객체 반환 시 |
| `apply` | 객체 초기화/설정 블록 |
| `run` | 객체 컨텍스트에서 계산 결과 반환 시 |

### 컬렉션 API — 함수형 체인 선호
```kotlin
// 지양
val result = mutableListOf<Long>()
for (id in ids) {
    val item = cache.get(id)
    if (item != null) result.addAll(item)
}

// 권장
val result = ids
    .mapNotNull { cache.get(it) }
    .flatten()
```

### sealed class vs enum class
- 상태/분기에 **추가 데이터**가 필요하면 `sealed class`
- 단순 분류/상수 집합이면 `enum class`

### data class
- 불변 상태 전달 객체는 `data class`로 선언한다.
- 일부 필드만 변경할 때는 `copy()`를 사용한다.

---

## 클린 코드

### 명명
- 함수명은 동사로 시작, 의도를 드러낸다: `getCandidatePostIds()`, `sendPushNotification()`
- Boolean 반환은 `is`, `has`, `can` 접두사: `isActive()`, `hasSubscription()`
- 마법 숫자/문자열은 `companion object` 상수로 추출한다.

```kotlin
// 지양
val endId = startId + 1000 - 1

// 권장
companion object { const val BATCH_SIZE = 1000 }
val endId = startId + BATCH_SIZE - 1
```

### 함수 크기 & 단일 책임
- 한 함수는 한 가지 일만 한다.
- 함수가 20줄을 넘으면 분리를 검토한다.
- 중첩 depth가 3 이상이면 early return 또는 함수 분리를 고려한다.

### 주석
- 코드로 의도를 표현할 수 없을 때만 주석을 작성한다.
- 주석 처리된 코드는 커밋에 남기지 않는다. 필요하면 git으로 복원한다.
- `// TODO`는 구체적인 내용을 포함한다: `// TODO: DLT 연동 후 재처리 로직 추가`

### enum의 else 분기 — 명시적 처리
```kotlin
// 지양 — 새 값 추가 시 else가 잡아버려 컴파일 경고 없음
fun from(hour: Int) = when (hour) {
    LUNCH.hour -> LUNCH
    else -> DINNER
}

// 권장
fun from(hour: Int) = when (hour) {
    LUNCH.hour -> LUNCH
    DINNER.hour -> DINNER
    else -> throw IllegalArgumentException("유효하지 않은 시간: $hour")
}
```

---

## 로깅

- 선언: `private val log = LoggerFactory.getLogger(javaClass)`
- 문자열 보간(`${}`) 대신 SLF4J placeholder(`{}`) 사용

```kotlin
// 지양 — 로그 레벨 비활성화 시에도 문자열 생성됨
log.info("발송 시작 - pushTime=${pushTime}")

// 권장
log.info("발송 시작 - pushTime={}", pushTime)
```

| 레벨 | 기준 |
|------|------|
| INFO | 비즈니스 흐름의 주요 이벤트 (배치 시작/완료) |
| ERROR | 복구 불가능하거나 외부 시스템 실패 (Kafka 발행 실패) |
| DEBUG | 개발/진단용 상세 정보 (운영에서는 비활성화) |

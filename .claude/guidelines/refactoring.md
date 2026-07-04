# 리팩터링 가이드라인

## 원칙

- 리팩터링은 동작을 바꾸지 않는다. 기능 변경과 리팩터링을 한 커밋에 섞지 않는다.
- 리팩터링 전 테스트가 있어야 한다. 테스트가 없으면 먼저 테스트를 작성한다.
- 작은 단위로 자주 커밋한다. 하나의 리팩터링이 완료될 때마다 커밋한다.

---

## 우선순위

1. **가독성** — 코드를 읽는 사람이 즉시 의도를 파악할 수 있는가
2. **단일 책임** — 하나의 함수/클래스가 하나의 일만 하는가
3. **중복 제거** — 동일한 로직이 두 곳 이상에 있는가
4. **추상화 수준** — 한 함수 안에서 고수준/저수준 로직이 섞여 있지 않은가

---

## 주요 기법

### 함수 추출 (Extract Function)
긴 함수에서 의미 있는 단위를 별도 함수로 분리한다.

```kotlin
// 전
fun execute(pushTime: PushTime) {
    val maxId = userRepository.findMaxId() ?: throw IllegalStateException("User not exist!")
    var startId = 1L
    while (startId <= maxId) {
        val endId = startId + BATCH_SIZE - 1
        val userIds = userRepository.findActiveIdsBetween(startId, endId)
        if (userIds.isNotEmpty()) {
            pushNotificationProducer.send(PushNotificationBatchMessage(userIds, LocalDateTime.now()))
        }
        startId += BATCH_SIZE
    }
}

// 후
fun execute(pushTime: PushTime) {
    val maxId = findMaxUserIdOrThrow()
    publishBatchesUpTo(maxId)
}
```

### 조건 단순화 — Early Return
중첩 조건문을 early return으로 평탄화한다.

```kotlin
// 전
fun process(ids: List<Long>) {
    if (ids.isNotEmpty()) {
        ids.forEach { process(it) }
    }
}

// 후
fun process(ids: List<Long>) {
    if (ids.isEmpty()) return
    ids.forEach { process(it) }
}
```

### 매직 값 상수화
반복되는 리터럴은 `companion object` 상수로 추출한다.

---

## 하지 말아야 할 리팩터링

- 동작하는 코드를 "더 예쁘게" 만들겠다고 테스트 없이 건드리는 것
- 사용처가 하나뿐인 코드에 불필요한 추상화 레이어 추가
- 리팩터링과 기능 추가를 동시에 진행

# 테스트 코드 가이드라인

## 기본 원칙

- 단위 테스트와 통합 테스트를 명확히 분리한다.
- 외부 의존성(Redis, Kafka, DB)은 단위 테스트에서 mock 처리, 통합 테스트에서만 실제 연결한다.
- 테스트는 독립적으로 실행 가능해야 한다. 테스트 간 상태를 공유하지 않는다.

---

## 테스트 구조

given / when / then 패턴을 따른다.

```kotlin
@Test
fun `커뮤니티가 없으면 빈 목록을 반환한다`() {
    // given
    every { userCommunityRepository.findCommunityIdsByUserId(userId) } returns emptyList()

    // when
    val result = sut.getCandidatePostIds(userId)

    // then
    result shouldBe emptyList()
    verify(exactly = 0) { redisCacheManager.getMembers(any()) }
}
```

---

## mockk 사용 기준

```kotlin
// mock 생성
private val repository = mockk<UserCommunityRepository>()

// stub
every { repository.findById(1L) } returns user
every { repository.findById(2L) } throws RuntimeException("DB error")

// 호출 검증
verify(exactly = 1) { repository.save(any()) }
verify(exactly = 0) { producer.send(any()) }

// relaxed mock — 반환값이 중요하지 않을 때
private val log = mockk<Logger>(relaxed = true)
```

---

## 테스트 명명

- 한글로 작성하며 행위와 결과를 명확히 표현한다.
- `` `행위_조건_기대결과` `` 또는 `` `조건이면 기대결과이다` `` 형태

```kotlin
// 권장
fun `유효하지 않은 시간이면 예외를 던진다`()
fun `활성 사용자가 없으면 Kafka 메시지를 발행하지 않는다`()
```

---

## 경계값 & 예외 케이스

- 정상 경로뿐 아니라 빈 컬렉션, null, 경계값, 예외 케이스를 반드시 포함한다.
- 예외 테스트는 `shouldThrow<ExceptionType> { }` 사용

```kotlin
shouldThrow<IllegalArgumentException> {
    PushTime.from(99)
}.message shouldContain "유효하지 않은 시간"
```

# Task 3: 예외 클래스 + 전역 예외 핸들러

**커밋:** `658e1dd` Add coupon domain exceptions and global exception handler

## 한 일

- `CouponExceptions.kt` (`coupon.exception`) — `CouponEventNotFoundException`, `CouponEventNotOpenException`,
  `CouponSoldOutException`, `DuplicateCouponIssueException` (모두 `RuntimeException`)
- `GlobalExceptionHandler` (`common`, `@RestControllerAdvice`) — 예외를 HTTP 상태로 매핑
  - `CouponEventNotFoundException` → 404 `NOT_FOUND`
  - `CouponEventNotOpenException` → 403 `NOT_OPEN_YET`
  - `DuplicateCouponIssueException` → 409 `DUPLICATE`
  - `CouponSoldOutException` → 410 `SOLD_OUT`
  - 응답 바디는 `ErrorResponse(status, message)`
- `GlobalExceptionHandlerTest` — 핸들러 메서드를 Spring 컨텍스트 없이 직접 호출해 상태 코드/바디 매핑 검증

타입 하나(예: `CouponIssueException(code: String)`)로 합치지 않고 4개의 별도 클래스로 나눈 이유: `@ExceptionHandler`가 예외 타입으로 분기하므로, 클래스를 나누면 컴파일 타임에 "이 상황엔 이 예외"라는 게 강제되고 `when`/`if`로 code 문자열을 비교하는 코드가 생기지 않는다. [Task 5](task-5-issue-service.md)의 판정 흐름도에서 각 분기가 어떤 예외로 이어지는지, [Task 6](task-6-issue-api.md)의 시퀀스 다이어그램에서 그 예외가 어떻게 HTTP 응답으로 바뀌는지 확인할 수 있다.

## 계획과 다른 점

없음 — 계획 그대로 진행됨.

## 검증

- `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"` → `BUILD SUCCESSFUL`, 2 tests passed

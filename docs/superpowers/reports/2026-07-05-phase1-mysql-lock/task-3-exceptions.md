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

## 계획과 다른 점

없음 — 계획 그대로 진행됨.

## 검증

- `./gradlew test --tests "jh.couponevent.common.GlobalExceptionHandlerTest"` → `BUILD SUCCESSFUL`, 2 tests passed

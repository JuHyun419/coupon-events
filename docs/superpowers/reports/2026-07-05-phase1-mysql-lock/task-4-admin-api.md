# Task 4: 관리자 API (이벤트 생성/조회)

**커밋:** `a2a92b6` Add coupon event admin API (create/list/get)

## 한 일

- DTO: `CreateCouponEventRequest`(`@NotBlank name`, `@Positive totalQuantity`, `startAt`),
  `CouponEventResponse`
- `CouponEventAdminService`
  - `create` — `CouponEvent` 저장 후 응답으로 변환
  - `findAll(name)` — `name`이 없으면 전체 조회, 있으면 `findByNameContaining`
  - `findById(eventId)` — 없으면 `CouponEventNotFoundException`
- `CouponEventAdminController`
  - `POST /api/admin/coupon-events` → 201
  - `GET /api/admin/coupon-events?name=` → 목록
  - `GET /api/admin/coupon-events/{eventId}` → 단건
- `CouponEventAdminControllerTest` — 서비스를 mock으로 두고 컨트롤러가 요청을 위임하는지, 201 응답을 만드는지 검증 (Spring 컨텍스트 없이 순수 단위 테스트)

## 계획과 다른 점

없음 — 계획 그대로 진행됨.

## 검증

- `./gradlew test --tests "jh.couponevent.coupon.api.CouponEventAdminControllerTest"` → `BUILD SUCCESSFUL`, 2 tests passed

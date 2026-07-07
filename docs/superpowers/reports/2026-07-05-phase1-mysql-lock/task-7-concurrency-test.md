# Task 7: 동시성 통합 테스트 (실제 MySQL 락 경합 검증)

**커밋:** `992bc5c` Add concurrency integration test against real MySQL

## 한 일

- `CouponIssueConcurrencyTest` — `@SpringBootTest`로 전체 컨텍스트를 띄우고 실제 빈(`CouponIssueService`,
  리포지토리들)을 사용, 실제 로컬 MySQL(docker-compose)에 대해 검증
- 시나리오: 재고 100개짜리 이벤트를 만들고, 32개 스레드 풀로 서로 다른 사용자 200명이 동시에 발급 요청
- 검증: 성공 100건, 실패(품절) 100건, `IssuedCouponRepository.countByCouponEventId`로 DB에 실제 저장된 row 수도 정확히 100건임을 확인 — 비관적 락이 초과 발급을 막는다는 것을 실제 DB 레벨에서 증명

## 계획과 다른 점

없음 — 계획 그대로 진행됨. (참고: 이 테스트는 실행마다 새 이벤트 row를 만들어 로컬 DB에 데이터가 누적된다 — 학습용 로컬 DB라 정리는 선택 사항이며, 필요하면 `docker compose down -v && docker compose up -d`로 초기화)

## 검증

- MySQL 기동 확인: `docker compose up -d && docker compose ps` → `mysql` 서비스 `running`
- `./gradlew test --tests "jh.couponevent.coupon.application.CouponIssueConcurrencyTest"` → `BUILD SUCCESSFUL`, 1 test passed
  (성공 100 / 실패 100 / DB row 100 정확히 일치)

# Task 1: 의존성 추가 + 로컬 MySQL 인프라 구성

**커밋:** `40c4836` Add JPA/Web/MySQL dependencies and local docker-compose infra

## 한 일

- `build.gradle.kts`에 계획대로 JPA/Web/Validation/MySQL/테스트 의존성 추가
  (`spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-boot-starter-validation`,
  `mysql-connector-j`, `spring-boot-starter-test`, `mockito-kotlin` 등)
- 저장소 루트에 `docker-compose.yml` 생성 — MySQL 8.0, DB `coupon_event`, 계정 `coupon`/`coupon`, 포트 `3306`
- `application.properties` 삭제, `application.yml` 생성
  - `spring.jpa.hibernate.ddl-auto: update`
  - `spring.jpa.open-in-view: false` — 지연 로딩을 컨트롤러/뷰까지 끌고 가지 않도록 트랜잭션 경계를 서비스 계층에 명시적으로 둠

## 계획과 다른 점

없음 — 계획 그대로 진행됨. (단, 이후 Task 2에서 `application.yml`의 JDBC URL에 `allowPublicKeyRetrieval=true`가 추가로 필요해짐 — [task-2](task-2-entities-repositories.md) 참고)

## 검증

- `docker compose up -d && docker compose ps` → `mysql` 서비스 `running`
- `./gradlew build -x test` → `BUILD SUCCESSFUL`

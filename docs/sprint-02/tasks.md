# Sprint 02 — Task Breakdown

## 1. Dependencies
- [ ] 1.1 Add argon2, async-trait to workspace Cargo.toml
- [ ] 1.2 Add argon2, async-trait, thiserror to auth-service Cargo.toml

## 2. Database
- [ ] 2.1 Create `0003_create_users_accounts.sql` migration
- [ ] 2.2 Apply migration to running PostgreSQL

## 3. Domain Layer
- [ ] 3.1 Create `domain/error.rs` with DomainError enum
- [ ] 3.2 Create `domain/account.rs` with Account entity
- [ ] 3.3 Create `domain/repository.rs` with AccountRepository trait

## 4. Infrastructure Layer
- [ ] 4.1 Create `infrastructure/password.rs` with Argon2 hashing service
- [ ] 4.2 Create `infrastructure/jwt_service.rs` wrapping common-auth JWT
- [ ] 4.3 Create `infrastructure/postgres_repo.rs` with SQLx implementation

## 5. Application Layer
- [ ] 5.1 Create `application/auth.rs` with register and login use cases
- [ ] 5.2 Validate email format and password strength in register use case
- [ ] 5.3 Return proper errors for duplicate email, wrong credentials

## 6. Presentation Layer
- [ ] 6.1 Create `presentation/http/dto.rs` with request/response DTOs
- [ ] 6.2 Create `presentation/http/auth.rs` with register/login handlers
- [ ] 6.3 Update `config/routes.rs` to wire register and login endpoints

## 7. Tests
- [ ] 7.1 Unit test password hashing (hash, verify, weak password rejection)
- [ ] 7.2 Unit test JWT service (generate, validate, wrong secret)
- [ ] 7.3 Unit test register use case (success, duplicate, invalid email, weak password)
- [ ] 7.4 Integration test register → login → verify token

## 8. Verification
- [ ] 8.1 `cargo build --workspace` compiles
- [ ] 8.2 `cargo test -p auth-service` passes all tests
- [ ] 8.3 `cargo clippy -p auth-service -- -D warnings` clean
- [ ] 8.4 Security review completed

## 9. Documentation
- [ ] 9.1 `docs/sprint-02/spec.md`
- [ ] 9.2 `docs/sprint-02/plan.md`
- [ ] 9.3 `docs/sprint-02/tasks.md`
- [ ] 9.4 `docs/sprint-02/implementation-report.md`
- [ ] 9.5 `docs/sprint-02/quickstart.md`

## 10. Git
- [ ] 10.1 Create branch sprint/02-auth
- [ ] 10.2 Commit with conventional commit messages
- [ ] 10.3 Push and create PR

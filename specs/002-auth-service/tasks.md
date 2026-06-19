# Tasks: Auth Service — Login, Refresh, Logout & Profile

**Input**: Design documents from `/specs/002-auth-service/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Service**: `source/services/auth-service/src/`, `source/services/auth-service/tests/`
- Paths below assume Cargo project at `source/services/auth-service/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the Cargo project and install dependencies

- [X] T001 Initialize Rust Cargo project at `source/services/auth-service/` with Actix-web, sqlx (postgres feature), reqwest, serde, jsonwebtoken, tokio, chrono, uuid dependencies
- [X] T002 [P] Add `rustfmt` and `clippy` configuration in `source/services/auth-service/rustfmt.toml` and `.cargo/config.toml`
- [X] T003 Create directory structure: `src/routes/`, `src/keycloak/`, `src/db/`, `src/models/`, `src/middleware/`, `src/validation/`, `src/services/`, `tests/integration/`, `tests/contracts/`, `tests/load/`

---

## Phase 2: Foundation (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 [P] Implement unified error enum `AuthError` with `ResponseError` trait in `source/services/auth-service/src/error.rs` covering all 4 error codes (400 validation_error, 401 invalid_credentials, 401 token_expired, 503 auth_unavailable)
- [X] [P] T004a Implement log redaction middleware in `source/services/auth-service/src/middleware/redaction.rs` — must never log `password`, `access_token`, or `refresh_token` fields (required by FR-001)
- [X] T005 [P] Define request/response types in `source/services/auth-service/src/models/auth.rs` (LoginRequest, RefreshRequest, LogoutRequest, LogoutResponse, TokenResponse with `refresh_expires_in`, ErrorResponse)
- [X] [P] T005a Define `LogoutResponse` struct with `message: String` in `source/services/auth-service/src/models/auth.rs`
- [X] T006 Define UserProfile struct and `UpsertUser` query type in `source/services/auth-service/src/models/user.rs`
- [X] [P] T006a Implement JWT claims parser in `source/services/auth-service/src/keycloak/claims.rs` — extract `sub`, `email`, `given_name`, `family_name`, `realm_access.roles`, and `aud` claims from the Keycloak token response
- [X] [P] T006b Create SQL migration at `source/infra/migrations/0003_users_profiles.sql` — `CREATE TABLE users.user_profiles`, indexes, and `updated_at` trigger
- [X] [P] T006c Implement refresh/logout token validation in `source/services/auth-service/src/validation/token.rs` — validate non-empty, expected JWT structure, max length; return 400 `validation_error` before any Keycloak call
- [X] T007 [P] Implement Keycloak HTTP client in `source/services/auth-service/src/keycloak/client.rs` with methods: `login(email, password)`, `refresh(refresh_token)`, `logout(refresh_token)` — each returning raw jsonwebtoken values or an `AuthError`
- [X] T008 [P] Implement DB users repository in `source/services/auth-service/src/db/users.rs` with `upsert_user` sqlx query that inserts or updates a USR- row keyed to `keycloak_sub`
- [X] T009 Set up Actix-web app entrypoint in `source/services/auth-service/src/main.rs` with router, JSON config, CORS, and a `GET /health` returning 200
- [X] [P] T009a Implement readiness endpoint in `source/services/auth-service/src/main.rs` — `GET /ready` checks DB connectivity and Keycloak connectivity, returns 503 if any check fails
- [X] T010 [P] Create KeycloakConfig struct in `source/services/auth-service/src/keycloak/config.rs` — holds `realm`, `client_id`, `client_secret`, `token_url`, `logout_url`, `introspect_url` from environment variables
- [X] T011 [P] Add Keycloak DTO models in `source/services/auth-service/src/keycloak/models.rs` — `TokenResponse`, `TokenErrorResponse`, `LogoutRequest`, `LogoutResponse`
- [X] T012 Add structured tracing in `source/services/auth-service/Cargo.toml` — add `tracing` and `tracing-actix-web` dependencies
- [X] [P] T013 Add request correlation middleware in `source/services/auth-service/src/middleware/request_id.rs` — generates `X-Request-Id` header, sets correlation ID in logs

**Checkpoint**: Foundation ready — error handling, Keycloak client, JWT claims parser, token validation, DB users repo, server scaffold, config, models, tracing, readiness, request IDs all wired. US1/2/4/5 can begin.

---

## Phase 3: User Story 1 - Admin/partner logs in (Priority: P1) 🎯 MVP

**Goal**: `POST /api/v1/auth/login` accepts email+password, calls Keycloak, upserts user profile, returns token pair.

**Independent Test**: Send valid credentials to login endpoint → receive access_token + refresh_token. Send invalid password → 401 `invalid_credentials`. Verify USR- row created in DB.

### Implementation for User Story 1

- [X] T014 [US1] Implement login route handler in `source/services/auth-service/src/routes/login.rs` — validate request, call Keycloak client, upsert user profile, return `TokenResponse`
- [X] T015 [US1] Wire login route into router (`POST /api/v1/auth/login`) in `src/routes/mod.rs` and `src/main.rs`
- [X] T016 [US1] Write integration test in `tests/integration/login_test.rs` — send valid credentials, assert 200 with token fields; send bad password, assert 401 with `invalid_credentials`
- [X] T017 [US1] Add structured logging for login success/failure in `src/routes/login.rs`
- [X] [P] T018 Write audience propagation test in `tests/integration/login_audience_test.rs` — verify aud claim returned unchanged from Keycloak

**Checkpoint**: Login endpoint fully functional. Token pair returned, user profile persisted in `platform_db.users`. Audience propagation verified.

---

## Phase 4: User Story 2 - Client refreshes token (Priority: P1)

**Goal**: `POST /api/v1/auth/refresh` accepts refresh_token, calls Keycloak, upserts user profile, returns new token pair.

**Independent Test**: Login, then submit the refresh_token to refresh endpoint → receive new tokens. Submit expired token → 401 `token_expired`.

### Implementation for User Story 2

- [X] T019 [P] [US2] Implement refresh route handler in `source/services/auth-service/src/routes/refresh.rs` — validate request, call Keycloak client, upsert user profile, return `TokenResponse`
- [X] T020 [US2] Wire refresh route into router (`POST /api/v1/auth/refresh`) in `src/routes/mod.rs`
- [X] T021 [US2] Write integration test in `tests/integration/refresh_test.rs` — login first, then refresh, assert new tokens; use expired token, assert 401 `token_expired`
- [X] T022 [US2] Add structured logging for refresh success/failure
- [X] [P] T023 Write audience propagation test in `tests/integration/refresh_audience_test.rs` — verify refreshed token retains expected audience
- [X] [P] T024 Create profile_sync service in `source/services/auth-service/src/services/profile_sync.rs` — sync_user_profile() handles create/update, maps Keycloak claims
- [X] T025 Refactor login.rs to call profile_sync service instead of DB directly
- [X] T026 Refactor refresh.rs to call profile_sync service instead of DB directly

**Checkpoint**: Refresh endpoint functional. Token rotation works without re-authentication. Profile sync service centralized.

---

## Phase 5: User Story 4 - User logs out (Priority: P1)

**Goal**: `POST /api/v1/auth/logout` accepts refresh_token, calls Keycloak logout, returns 200.

**Independent Test**: Login, then submit the refresh_token to logout endpoint → 200 `logged_out`. Submit the same token again → 200 (idempotent). Submit already-expired token → 200.

### Implementation for User Story 4

- [X] T027 [P] [US4] Implement logout route handler in `source/services/auth-service/src/routes/logout.rs` — validate request, call Keycloak logout, return 200
- [X] T028 [US4] Wire logout route into router (`POST /api/v1/auth/logout`) in `src/routes/mod.rs`
- [X] T029 [US4] Write integration test in `tests/integration/logout_test.rs` — login, logout, assert 200; logout again with same token, assert 200 (idempotent)
- [X] T030 [US4] Add structured logging for logout success/failure
- [X] [P] T031 Implement logout idempotency wrapper in `src/routes/logout.rs` — catch Keycloak errors (invalid_grant, token_expired, token_revoked) and return HTTP 200 with message "logged_out"

**Checkpoint**: Logout endpoint functional. Full auth lifecycle (login → refresh → logout) complete. Logout idempotent.

---

## Phase 5a: User Story 5 - Retrieve authenticated profile (Priority: P2)

**Goal**: `GET /api/v1/auth/me` accepts Bearer token, validates it, returns synchronized user profile from database.

**Why this priority**: Enables dashboard bootstrapping, role-gating, and mobile auth without frontend JWT decoding.

**Independent Test**: Login, then call /me with the access token → receive profile. Use expired token → 401.

### Implementation for User Story 5

- [X] [P] T032 [US5] Implement `GET /api/v1/auth/me` route handler in `source/services/auth-service/src/routes/me.rs` — validate Bearer token, look up user profile by `sub`, return profile
- [X] T033 [US5] Wire `/me` route into router in `src/routes/mod.rs`
- [X] T034 [US5] Write integration test in `tests/integration/me_test.rs` — valid token returns profile; invalid token returns 401

**Checkpoint**: Profile retrieval endpoint functional.

---

## Phase 6: Cross-Cutting Concerns & Testing

**Purpose**: Production readiness improvements, testing coverage, and contract verification

- [X] [P] T035 Create permission integration tests in `tests/integration/db_permissions_test.rs` — verify auth_service_role cannot SELECT from inventory.*, admin_service_role cannot SELECT from users.user_profiles, driver_service_role cannot INSERT/UPDATE/DELETE inventory.*
- [X] T036 Add request body size limits and timeout configuration to Actix-web server in `src/main.rs`
- [X] T037 Harden error responses — ensure no Keycloak URLs or internal details leak in any error body in `src/error.rs`
- [X] T038 Write integration test for malformed JSON in `tests/integration/failure_scenarios_test.rs` — assert 400 `validation_error`
- [X] T039 Write integration test for missing required fields in `tests/integration/failure_scenarios_test.rs` — assert 400 `validation_error`
- [X] [P] T040 Write Keycloak unavailable test in `tests/integration/failure_scenarios_test.rs` — T034 (US1), T035 (US2), T036 (US4) all assert 503 when Keycloak is down
- [X] T041 Validate OpenAPI contract against implementation — write contract test `tests/contracts/login_contract_test.rs`, `tests/contracts/refresh_contract_test.rs`, `tests/contracts/logout_contract_test.rs`
- [X] T042 [P] Extend load test script at `tests/load/auth_load_test.py` — 100 concurrent requests to login, refresh, and logout endpoints; verify SC-003: no degradation, logout idempotent
- [X] T043 Write SC-004 verification procedure — document manual steps to review Keycloak access logs for direct token-endpoint calls after integration test run

---

## Phase 7: Polish & Deployment

**Purpose**: Production deployment and documentation

- [X] T044 [P] Create production Dockerfile at `source/services/auth-service/Dockerfile` (multi-stage, distroless runtime)
- [X] T045 Add environment configuration module in `src/config.rs` loading from environment variables (Keycloak URL, DB URL, listen port)
- [X] T046 Update documentation in `docs/SYSTEM_STATE.md` to reflect Auth Service deployment
- [X] T047 Update sprint backlog in `docs/sprint_backlog.md` to reflect Sprint 1 completion

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundation (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 — Login (Phase 3)**: Depends on Phase 2 — MVP, no other story dependencies
- **US2 — Refresh (Phase 4)**: Depends on Phase 2 + US1 login (needs working login to get a refresh_token for testing)
- **US4 — Logout (Phase 5)**: Depends on Phase 2 + US1 login (needs working login to get a refresh_token for testing)
- **US5 — Profile (Phase 5a)**: Depends on Phase 2 (needs DB users repo + JWT validation) — independent of US1/2/4
- **Cross-Cutting (Phase 6)**: Depends on all user stories being complete
- **Polish (Phase 7)**: Depends on all desired user stories and cross-cutting concerns being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — no dependencies on other stories
- **US2 (P1)**: Depends on US1 — needs login to produce a valid refresh_token for testing, but the handler file is independent
- **US4 (P1)**: Depends on US1 — needs login to produce a valid refresh_token for testing, but the handler file is independent
- **US5 (P2)**: Depends on Foundational (JWT claims parser + DB users repo) — independent of US1/2/4
- **US2 and US4**: Independent of each other — can be implemented in parallel once US1 is done

### Parallel Opportunities

- T002 + T003 (Setup) can run in parallel
- T004 + T005 + T005a + T006a + T006c + T007 + T008 (Foundation) can run in parallel
- T010 + T011 + T012 + T013 (Foundation extras) can run in parallel
- T014 and T018 (US2 handler, US4 handler) can run in parallel once US1 completes
- T021a (US5 handler) can run in parallel with US2/US4 (no dependency on US1)
- T034, T035, T036 (failure tests) can run in parallel
- T042 (load test) can run in parallel with contract tests

---

## Parallel Example: User Story 1

```bash
# All US1 implementation tasks are sequential (handler → route → test → logging)
```

## Parallel Example: User Stories 2 & 4 (after US1)

```bash
# US2 and US4 can be developed simultaneously:
Task: T019 [P] [US2] Implement refresh handler
Task: T027 [P] [US4] Implement logout handler
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: US1 — Login endpoint
4. **STOP and VALIDATE**: Test login independently — valid credentials return tokens, invalid return 401, user profile persisted
5. Deploy/demo if ready — a user can log in

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 (Login) → Test independently → Deploy/Demo (MVP!)
3. Add US2 (Refresh) → Test independently → Deploy/Demo
4. Add US4 (Logout) → Test independently → Deploy/Demo
5. Add US5 (Profile) → Test independently → Deploy/Demo
6. Add cross-cutting concerns (testing, contracts, tracing) → Test independently → Deploy/Demo
7. Add polish (Docker, env config, docs) → Test independently → Deploy/Demo
8. Each story adds value without breaking previous stories

---

## Notes

- US3 (profile sync) is not a separate endpoint — it's a side effect of login and refresh. Implemented inside login/refresh route handlers via `db::users::upsert_user`.
- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Audience propagation verified in T018 (login) and T023 (refresh)
- Logout idempotency enforced in T031 (invalid_grant, token_expired, token_revoked → HTTP 200)
- Permission enforcement verified in T035 (db_permissions_test.rs)
- Load testing extended to cover logout in T042
- Failure scenario coverage: malformed JSON, missing fields, Keycloak unavailable in T038-T040

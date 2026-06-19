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

- [ ] T001 Initialize Rust Cargo project at `source/services/auth-service/` with Actix-web, sqlx (postgres feature), reqwest, serde, jsonwebtoken, tokio, chrono, uuid dependencies
- [ ] T002 [P] Add `rustfmt` and `clippy` configuration in `source/services/auth-service/rustfmt.toml` and `.cargo/config.toml`
- [ ] T003 Create directory structure: `src/routes/`, `src/keycloak/`, `src/db/`, `src/models/`, `src/middleware/`, `src/validation/`, `tests/integration/`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 [P] Implement unified error enum `AuthError` with `ResponseError` trait in `source/services/auth-service/src/error.rs` covering all 4 error codes (400 validation_error, 401 invalid_credentials, 401 token_expired, 503 auth_unavailable)
- [ ] [P] T004a Implement log redaction middleware in `source/services/auth-service/src/middleware/redaction.rs` — must never log `password`, `access_token`, or `refresh_token` fields (required by FR-001)
- [ ] T005 [P] Define request/response types in `source/services/auth-service/src/models/auth.rs` (LoginRequest, RefreshRequest, LogoutRequest, LogoutResponse, TokenResponse with `refresh_expires_in`, ErrorResponse)
- [ ] [P] T005a Define `LogoutResponse` struct with `message: String` in `source/services/auth-service/src/models/auth.rs`
- [ ] T006 Define UserProfile struct and `UpsertUser` query type in `source/services/auth-service/src/models/user.rs`
- [ ] [P] T006a Implement JWT claims parser in `source/services/auth-service/src/keycloak/claims.rs` — extract `sub`, `email`, `given_name`, `family_name`, `realm_access.roles`, and `aud` claims from the Keycloak token response
- [ ] [P] T006b Create SQL migration at `source/infra/migrations/0003_users_profiles.sql` — `CREATE TABLE users.user_profiles`, indexes, and `updated_at` trigger
- [ ] [P] T006c Implement refresh/logout token validation in `source/services/auth-service/src/validation/token.rs` — validate non-empty, expected JWT structure, max length; return 400 `validation_error` before any Keycloak call
- [ ] T007 [P] Implement Keycloak HTTP client in `source/services/auth-service/src/keycloak/client.rs` with methods: `login(email, password)`, `refresh(refresh_token)`, `logout(refresh_token)` — each returning raw jsonwebtoken values or an `AuthError`
- [ ] T008 [P] Implement DB users repository in `source/services/auth-service/src/db/users.rs` with `upsert_user` sqlx query that inserts or updates a USR- row keyed to `keycloak_sub`
- [ ] T009 Set up Actix-web app entrypoint in `source/services/auth-service/src/main.rs` with router, JSON config, CORS, and a `GET /health` returning 200
- [ ] [P] T009a Implement rate limiting middleware in `source/services/auth-service/src/middleware/rate_limit.rs` — 10 attempts/minute/IP, applies to `POST /login` only; refresh and logout exempt

**Checkpoint**: Foundation ready — error handling, Keycloak client, JWT claims parser, token validation, DB users repo, and server scaffold all wired. US1/2/4/5 can begin.

---

## Phase 3: User Story 1 - Admin/partner logs in (Priority: P1) 🎯 MVP

**Goal**: `POST /api/v1/auth/login` accepts email+password, calls Keycloak, upserts user profile, returns token pair.

**Independent Test**: Send valid credentials to login endpoint → receive access_token + refresh_token. Send invalid password → 401 `invalid_credentials`. Verify USR- row created in DB.

### Implementation for User Story 1

- [ ] T010 [US1] Implement login route handler in `source/services/auth-service/src/routes/login.rs` — validate request, call Keycloak client, upsert user profile, return `TokenResponse`
- [ ] T011 [US1] Wire login route into router (`POST /api/v1/auth/login`) in `src/routes/mod.rs` and `src/main.rs`
- [ ] T012 [US1] Write integration test in `tests/integration/login_test.rs` — send valid credentials, assert 200 with token fields; send bad password, assert 401 with `invalid_credentials`
- [ ] T013 [US1] Add structured logging for login success/failure in `src/routes/login.rs`

**Checkpoint**: Login endpoint fully functional. Token pair returned, user profile persisted in `platform_db.users`.

---

## Phase 4: User Story 2 - Client refreshes token (Priority: P1)

**Goal**: `POST /api/v1/auth/refresh` accepts refresh_token, calls Keycloak, upserts user profile, returns new token pair.

**Independent Test**: Login, then submit the refresh_token to refresh endpoint → receive new tokens. Submit expired token → 401 `token_expired`.

### Implementation for User Story 2

- [ ] T014 [P] [US2] Implement refresh route handler in `source/services/auth-service/src/routes/refresh.rs` — validate request, call Keycloak client, upsert user profile, return `TokenResponse`
- [ ] T015 [US2] Wire refresh route into router (`POST /api/v1/auth/refresh`) in `src/routes/mod.rs`
- [ ] T016 [US2] Write integration test in `tests/integration/refresh_test.rs` — login first, then refresh, assert new tokens; use expired token, assert 401 `token_expired`
- [ ] T017 [US2] Add structured logging for refresh success/failure

**Checkpoint**: Refresh endpoint functional. Token rotation works without re-authentication.

---

## Phase 5: User Story 4 - User logs out (Priority: P1)

**Goal**: `POST /api/v1/auth/logout` accepts refresh_token, calls Keycloak logout, returns 200.

**Independent Test**: Login, then submit the refresh_token to logout endpoint → 200 `logged_out`. Submit the same token again → 200 (idempotent). Submit already-expired token → 200.

### Implementation for User Story 4

- [ ] T018 [P] [US4] Implement logout route handler in `source/services/auth-service/src/routes/logout.rs` — validate request, call Keycloak logout, return 200
- [ ] T019 [US4] Wire logout route into router (`POST /api/v1/auth/logout`) in `src/routes/mod.rs`
- [ ] T020 [US4] Write integration test in `tests/integration/logout_test.rs` — login, logout, assert 200; logout again with same token, assert 200 (idempotent)
- [ ] T021 [US4] Add structured logging for logout success/failure

**Checkpoint**: Logout endpoint functional. Full auth lifecycle (login → refresh → logout) complete.

---

## Phase 5a: User Story 5 - Retrieve authenticated profile (Priority: P2)

**Goal**: `GET /api/v1/auth/me` accepts Bearer token, validates it, returns synchronized user profile from database.

**Why this priority**: Enables dashboard bootstrapping, role-gating, and mobile auth without frontend JWT decoding.

**Independent Test**: Login, then call /me with the access token → receive profile. Use expired token → 401.

### Implementation for User Story 5

- [ ] [P] T021a [US5] Implement `GET /api/v1/auth/me` route handler in `source/services/auth-service/src/routes/me.rs` — validate Bearer token, look up user profile by `sub`, return profile
- [ ] T021b [US5] Wire `/me` route into router in `src/routes/mod.rs`
- [ ] T021c [US5] Write integration test in `tests/integration/me_test.rs` — valid token returns profile; invalid token returns 401

**Checkpoint**: Profile retrieval endpoint functional.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Production readiness improvements

- [ ] T022 [P] Create production Dockerfile at `source/services/auth-service/Dockerfile` (multi-stage, distroless runtime)
- [ ] T023 Add environment configuration module in `src/config.rs` loading from environment variables (Keycloak URL, DB URL, listen port)
- [ ] T024 Add request body size limits and timeout configuration to Actix-web server
- [ ] T025 Harden error responses — ensure no Keycloak URLs or internal details leak in any error body
- [ ] T026 Run full integration test suite against live Docker stack and fix any failures
- [ ] T026a [P] Write integration test AUTH-IT-01: successful login creates USR row in `platform_db.users`
- [ ] T026b [P] Write integration test AUTH-IT-02: successful refresh updates USR row (last_login_at)
- [ ] T026c [P] Write integration test AUTH-IT-03: logout revokes refresh token (cannot reuse)
- [ ] T026d [P] Write integration test AUTH-IT-04: Keycloak unavailable returns 503 `auth_unavailable`
- [ ] T026e [P] Write integration test AUTH-IT-05: malformed refresh token returns 400 `validation_error` without contacting Keycloak
- [ ] T026f [P] Write integration test AUTH-IT-06: `auth_service_role` cannot SELECT from `inventory.partners`
- [ ] T027 Update `docs/SYSTEM_STATE.md` to reflect Auth Service deployment
- [ ] [P] T028 Write load test script at `tests/load/login_load_test.py` targeting login + refresh endpoints — verify SC-003: 100 concurrent requests without degradation
- [ ] [P] T029 Write SC-004 verification procedure — document manual steps to review Keycloak access logs for direct token-endpoint calls after integration test run

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 — Login (Phase 3)**: Depends on Phase 2 — MVP, no other story dependencies
- **US2 — Refresh (Phase 4)**: Depends on Phase 2 + US1 login (needs working login to get a refresh_token for testing)
- **US4 — Logout (Phase 5)**: Depends on Phase 2 + US1 login (needs working login to get a refresh_token for testing)
- **US5 — Profile (Phase 5a)**: Depends on Phase 2 (needs DB users repo + JWT validation) — independent of US1/2/4
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — no dependencies on other stories
- **US2 (P1)**: Depends on US1 — needs login to produce a valid refresh_token for testing, but the handler file is independent
- **US4 (P1)**: Depends on US1 — needs login to produce a valid refresh_token for testing, but the handler file is independent
- **US5 (P2)**: Depends on Foundational (JWT claims parser + DB users repo) — independent of US1/2/4
- **US2 and US4**: Independent of each other — can be implemented in parallel once US1 is done

### Parallel Opportunities

- T002 + T003 (Setup) can run in parallel
- T004 + T005 + T005a + T006a + T006c + T007 + T008 (Foundational) can run in parallel
- T014 and T018 (US2 handler, US4 handler) can run in parallel once US1 completes
- T021a (US5 handler) can run in parallel with US2/US4 (no dependency on US1)

---

## Parallel Example: User Story 1

```bash
# All US1 implementation tasks are sequential (handler → route → test → logging)
```

## Parallel Example: User Stories 2 & 4 (after US1)

```bash
# US2 and US4 can be developed simultaneously:
Task: T014 [P] [US2] Implement refresh handler
Task: T018 [P] [US4] Implement logout handler
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
6. Each story adds value without breaking previous stories

---

## Notes

- US3 (profile sync) is not a separate endpoint — it's a side effect of login and refresh. Implemented inside login/refresh route handlers via `db::users::upsert_user`.
- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently

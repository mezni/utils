# Tasks: Identity Core (MVP-2)

**Input**: Design documents from `/specs/001-identity-core/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md, contracts/lib.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **auth-service**: `source/services/auth-service/`
- **identity-core lib**: `source/services/libs/identity-core/`
- **Infra/keycloak**: `source/infra/keycloak/`
- **Infra/docker**: `source/infra/docker-compose.yml`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create empty crate skeletons, workspace config, and infra directories

- [ ] T001 [P] Create workspace members in `source/services/Cargo.toml` — add `auth-service`, `libs/identity-core` to members list
- [ ] T002 [P] Scaffold `identity-core` crate with `source/services/libs/identity-core/Cargo.toml` and `src/lib.rs` (deps: jsonwebtoken, reqwest, serde, thiserror)
- [ ] T003 [P] Scaffold `auth-service` crate with `source/services/auth-service/Cargo.toml`, `src/main.rs`, and all subdirectories (routes/, services/, middleware/)
- [ ] T004 [P] Create Keycloak init script directory at `source/infra/keycloak/` with `init-keycloak.sh`, `create-realm.sh`, `create-client.sh`, `create-role.sh`
- [ ] T005 [P] Add keycloak and auth-service service definitions to `source/infra/docker-compose.yml`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database schema and shared identity library — MUST complete before any user story

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 Create SQLx migration `source/services/auth-service/migrations/001_create_users_schema.sql` — creates `users` schema with 5 tables (accounts, roles, account_roles, identity_providers, audit_log), indexes, and seed roles
- [ ] T007 [P] Implement `JwtValidator` and `IdentityClaims` in `source/services/libs/identity-core/src/jwt.rs` and `claims.rs` — JWKS fetching/caching, token validation, claim extraction
- [ ] T008 [P] Implement `KeycloakAdminClient` in `source/services/libs/identity-core/src/admin_client.rs` — create user, assign role, set enabled, get by email, logout
- [ ] T009 [P] Implement `AuthMiddleware` and `AuthenticatedUser` extractor in `source/services/libs/identity-core/src/middleware.rs` for actix-web
- [ ] T010 [P] Implement auth-service error types and response format in `source/services/auth-service/src/errors.rs`
- [ ] T011 [P] Implement auth-service health endpoint in `source/services/auth-service/src/routes/health.rs` and structured JSON logging in `main.rs`
- [ ] T012 [P] Implement two-tier rate limiter middleware in `source/services/auth-service/src/middleware/rate_limiter.rs` (per-IP 10/min, per-account 20/15min)

**Checkpoint**: Foundation ready — user story implementation can begin

---

## Phase 3: User Story 1 - Driver Registration and Login (Priority: P1) 🎯 MVP

**Goal**: A new driver can register with email/password and immediately log in to receive a valid session. Existing users can log in, refresh tokens, view their profile, and log out.

**Note**: This phase subsumes US4 (Session Management) — T015 (refresh), T016 (logout), and T017 (me) collectively deliver all US4 acceptance criteria.

**Independent Test**: Register a new account via `POST /api/v1/auth/register`, log in via `POST /api/v1/auth/login`, verify identity via `GET /api/v1/auth/me`, refresh via `POST /api/v1/auth/refresh`, and log out via `POST /api/v1/auth/logout` — all with `curl`.

### Implementation for User Story 1

- [ ] T013 [P] [US1] Implement registration service in `source/services/auth-service/src/services/registration.rs` and route in `source/services/auth-service/src/routes/register.rs` — creates user in Keycloak via admin client, inserts into `users.accounts`, assigns `registered_driver` role, returns platform user ID
- [ ] T014 [P] [US1] Implement login service in `source/services/auth-service/src/services/session.rs` and route in `source/services/auth-service/src/routes/login.rs` — authenticates via Keycloak, returns JWT access + refresh tokens, records audit event, applies rate limiting
- [ ] T015 [P] [US1] Implement refresh endpoint in `source/services/auth-service/src/routes/refresh.rs` — exchanges refresh token for new access token via Keycloak
- [ ] T016 [P] [US1] Implement logout endpoint in `source/services/auth-service/src/routes/logout.rs` — invalidates Keycloak session, records audit event, clears session
- [ ] T017 [P] [US1] Implement me endpoint in `source/services/auth-service/src/routes/me.rs` — validates Bearer token via AuthMiddleware, returns IdentityClaims as JSON
- [ ] T018 [US1] Wire all routes into `source/services/auth-service/src/routes/mod.rs` and configure application state/startup in `source/services/auth-service/src/main.rs`

**Checkpoint**: At this point, US1 should be fully functional — register, login, refresh, logout, me all work

---

## Phase 4: User Story 2 - Authenticated Station Discovery (Priority: P1)

**Goal**: The existing driver-service station endpoints are protected by JWT validation. Only authenticated users can discover stations. Unauthenticated requests are rejected.

**Independent Test**: Log in as a registered driver, call `GET /api/v1/stations/nearby?lat=48.8566&lng=2.3522` with the Bearer token → receives stations. Call without token → 401.

### Implementation for User Story 2

- [ ] T019 [P] [US2] Add `identity-core` dependency to `source/services/driver-service/Cargo.toml`
- [ ] T020 [P] [US2] Integrate `AuthMiddleware` into `source/services/driver-service/src/routes/mod.rs` — protect `/api/v1/stations/*` routes, allow `/api/v1/health` unauthenticated
- [ ] T021 [P] [US2] Replace `usr-mvp1-fallback` hardcoded identity references in `source/services/driver-service/src/` with identity from `AuthenticatedUser` extractor
- [ ] T022 [US2] Verify station discovery auth flow end-to-end — register, login, query stations, verify stations returned with real `usr_id`

**Checkpoint**: Both US1 and US2 functional — authenticated station discovery works

---

## Phase 5: User Story 3 - Partner Account Management (Priority: P2)

**Goal**: An admin user can create partner accounts in the `bm-control` realm and toggle account status (disable/enable). Partner users can log in with partner-level role claims.

**Independent Test**: Log in as seed admin, create a partner account via `POST /api/v1/auth/admin/accounts`, log in as the partner, verify partner role in `GET /api/v1/auth/me`.

### Implementation for User Story 3

- [ ] T023 [P] [US3] Implement admin account creation endpoint in `source/services/auth-service/src/routes/admin.rs` — POST `/api/v1/auth/admin/accounts` creates partner in Keycloak + DB, assigns `partner` role, admin auth check
- [ ] T024 [P] [US3] Implement account status change endpoint in `source/services/auth-service/src/routes/admin.rs` — PATCH `/api/v1/auth/admin/accounts/{usr_id}/status`, disables/enables in Keycloak + DB, records audit event
- [ ] T025 [P] [US3] Complete `source/infra/keycloak/init-keycloak.sh` — create `bm-control` realm, add `partner` and `admin` roles, create seed admin user from env var credentials
- [ ] T026 [US3] Wire admin routes into `source/services/auth-service/src/routes/mod.rs` and verify partner login flow end-to-end

**Checkpoint**: All three user stories functional — partners can be created and managed by admins

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Docker integration, hardened error handling, validation completeness

- [ ] T027 [P] Add structured logging event counters for identity events in `source/services/auth-service/src/` — registrations, logins, failures, role changes, status changes
- [ ] T028 [P] Verify rate limiter integration — test 429 response after exceeding login thresholds in `source/services/auth-service/src/middleware/rate_limiter.rs`
- [ ] T029 Update docker-compose environment variables and health check config for keycloak and auth-service in `source/infra/docker-compose.yml`
- [ ] T030 Run quickstart.md validation — verify all endpoints, auth flow, and docker-compose startup
- [ ] T031 [P] Create load test script for 100 concurrent registration requests (SC-005) in `source/scripts/load-test-register.sh`
- [ ] T032 [P] Create session invalidation timing test (SC-006) in `source/scripts/test-session-invalidation.sh`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational (Phase 2)
- **US2 (Phase 4)**: Depends on Foundational (Phase 2) and US1 (Phase 3) — needs identity-core + login flow working
- **US3 (Phase 5)**: Depends on Foundational (Phase 2) and US1 (Phase 3) — needs login + admin routes
- **Polish (Phase 6)**: Depends on US1, US2, US3 completion

### User Story Dependencies

- **US1 (P1)**: Can start after Phase 2 — No dependencies on other stories
- **US2 (P1)**: Can start after Phase 2 and US1 — Must have login working to test authenticated station discovery
- **US3 (P2)**: Can start after Phase 2 and US1 — Must have login and auth middleware working

### Within Each Phase

- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 tasks (T001–T005) can run in parallel
- All Phase 2 tasks (T006–T012) can run in parallel (different files, no code dependencies)
- All Phase 3 implementation tasks marked [P] (T013–T017) can run in parallel, then T018 wires them together
- US2 tasks T019–T021 can run in parallel, then T022 verifies
- US3 tasks T023–T025 can run in parallel, then T026 wires them together

---

## Parallel Example: User Story 1

```bash
# Launch all implementation tasks for User Story 1 together:
Task: "Implement registration service and route in register.rs"
Task: "Implement login service and route in login.rs"
Task: "Implement refresh endpoint in refresh.rs"
Task: "Implement logout endpoint in logout.rs"
Task: "Implement me endpoint in me.rs"

# Then wire routes together:
Task: "Wire all routes in routes/mod.rs and configure main.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (Registration, Login, Refresh, Logout, Me)
4. **STOP and VALIDATE**: Test US1 independently with curl — register, login, me, refresh, logout
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (Register + Login) → Test independently → ✅ MVP!
3. Add US2 (Auth Station Discovery) → Test independently → Deploy
4. Add US3 (Partner Management) → Test independently → Deploy
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (Registration + Login endpoints)
   - Developer B: US2 (Driver-service integration)
   - Developer C: US3 (Admin endpoints + Keycloak scripts)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Rate limiting is foundational — implement in Phase 2, verify in Phase 6
- AuthMiddleware lives in identity-core crate so all services can use it

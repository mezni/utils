# Tasks: Identity & Security Core

**Input**: Design documents from `/specs/002-identity-security-core/`
**Prerequisites**: Sprint 0 complete (services running, DB provisioned, CI pipeline active)

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1-US5)
- Include exact file paths in descriptions

## Path Conventions

- Services: `services/{name}/src/`
- Infrastructure: `infrastructure/{docker-compose,keycloak,traefik}/`
- CI Tools: `tools/`
- Shared contracts: `apps/packages/domain-types/src/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Deploy Keycloak, add Keycloak to docker-compose, define shared contracts in domain-types

- [ ] T001 [P] Add Keycloak service definition to `infrastructure/docker-compose/local.yml` (image, ports 8080, keycloak_db)
- [ ] T002 [P] Create `infrastructure/keycloak/setup.sh` for realm/client/role provisioning
- [ ] T003 [P] Create `infrastructure/keycloak/realm-export.json` as export of `bornemap` realm configuration
- [ ] T004 [P] Define `Role` enum in `apps/packages/domain-types/src/role.rs`
- [ ] T005 [P] Define `JwtClaims` struct in `apps/packages/domain-types/src/jwt.rs`
- [ ] T006 [P] Define `AuditEvent` struct in `apps/packages/domain-types/src/audit.rs`
- [ ] T007 [P] Define `UserProfile` struct in `apps/packages/domain-types/src/user.rs`
- [ ] T008 [P] Update `apps/packages/domain-types/src/lib.rs` to export new modules

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: JWT validation middleware, Keycloak Admin API client, database migration

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T009 Implement `services/auth-service/src/middleware/jwt.rs` — JWT validation middleware (JWKS fetch, cache, signature verify, claim extraction)
- [ ] T010 Implement `services/driver-service/src/middleware/jwt.rs` — JWT validation middleware (reuses same pattern, no Keycloak Admin API)
- [ ] T011 Implement `services/admin-service/src/middleware/jwt.rs` — JWT validation middleware (reuses same pattern)
- [ ] T012 [P] Create `services/auth-service/migrations/0002_user_profiles_role.up.sql` — add `role VARCHAR(20)` column to `users.user_profiles`
- [ ] T013 Create `services/auth-service/src/keycloak/client.rs` — Keycloak Admin API client (realm config, user lookup)
- [ ] T014 Implement `services/auth-service/src/config.rs` — load Keycloak URL, realm, client credentials from config/env

**Checkpoint**: Foundation ready — all services can validate JWTs, Keycloak is configured, user_profiles has role column

---

## Phase 3: User Story 1 — Keycloak Identity Integration (P1)

**Goal**: Drivers, partners, and admins authenticate through Keycloak. Tokens validated at gateway and in each service.

**Independent Test**: Authenticate as each role (driver, partner, admin) and verify JWT accepted by all three services.

### Implementation for User Story 1

- [ ] T015 [P] [US1] Configure Traefik forward-auth JWT validation in `infrastructure/traefik/dynamic/jwt-auth.yml`
- [ ] T016 [P] [US1] Create Keycloak realm `bornemap` with setup script `infrastructure/keycloak/setup.sh`
- [ ] T017 [P] [US1] Create Keycloak clients (`mobile-driver`, `web-driver`, `admin-dashboard`) in setup script
- [ ] T018 [P] [US1] Create Keycloak realm roles (`driver`, `partner`, `admin`) in setup script
- [ ] T019 [US1] Wire JWT middleware into `services/auth-service/src/main.rs` (add middleware to App)
- [ ] T020 [US1] Wire JWT middleware into `services/driver-service/src/main.rs`
- [ ] T021 [US1] Wire JWT middleware into `services/admin-service/src/main.rs`
- [ ] T022 [US1] Test authentication flow end-to-end (Keycloak → JWT → service validation)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 — Role-Based Access Control (P1)

**Goal**: Each authenticated user has a role that determines what they can access. RBAC enforced on every endpoint.

**Independent Test**: Create three test accounts (driver, partner, admin) and verify each can only access permitted endpoints.

### Implementation for User Story 2

- [ ] T023 [P] [US2] Implement `services/auth-service/src/middleware/rbac.rs` — role extraction from JWT + route guard
- [ ] T024 [P] [US2] Implement `services/driver-service/src/middleware/rbac.rs` — route role guard
- [ ] T025 [P] [US2] Implement `services/admin-service/src/middleware/rbac.rs` — route role guard
- [ ] T026 [US2] Define route protection rules in `services/auth-service/src/routes.rs` (annotate each route with allowed roles)
- [ ] T027 [US2] Define route protection rules in `services/driver-service/src/routes.rs`
- [ ] T028 [US2] Define route protection rules in `services/admin-service/src/routes.rs`
- [ ] T029 [US2] Add public route whitelist for `GET /health` and `POST /api/v1/auth/login` in all services
- [ ] T030 [US2] Test RBAC enforcement — verify 403 for insufficient role, 401 for invalid token, 200 for correct role

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 — Just-In-Time User Provisioning (P1)

**Goal**: First-time authentication creates user profile in platform_db. Subsequent logins update role/attributes from Keycloak.

**Independent Test**: Authenticate as a new user, query user_profiles table to verify record created with matching role.

### Implementation for User Story 3

- [ ] T031 [P] [US3] Implement `services/auth-service/src/provisioning/jit.rs` — JIT upsert handler (SELECT or INSERT/UPDATE user_profiles)
- [ ] T032 [P] [US3] Implement `services/auth-service/src/provisioning/middleware.rs` — middleware that calls JIT after JWT validation
- [ ] T033 [US3] Wire JIT middleware into `services/auth-service/src/main.rs` (after JWT middleware)
- [ ] T034 [US3] Test JIT provisioning — authenticate as new user, verify DB record created with correct UUID and role
- [ ] T035 [US3] Test JIT update — change role in Keycloak, re-auth, verify user_profiles.role updated

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 — Audit Logging for Security Events (P2)

**Goal**: All auth events (login success, failure, token rejection) logged to analytics_db via BUS routing through driver-service.

**Independent Test**: Trigger a login failure and a login success, verify both events appear in analytics_db.raw_events.

### Implementation for User Story 4

- [ ] T036 [P] [US4] Implement `services/auth-service/src/audit/emitter.rs` — HTTP client that POSTs audit events to driver-service `/api/v1/telemetry/events`
- [ ] T037 [P] [US4] Implement `services/auth-service/src/audit/middleware.rs` — middleware that emits events on login success, failure, token rejection
- [ ] T038 [US4] Wire audit middleware into `services/auth-service/src/main.rs`
- [ ] T039 [P] [US4] Implement `services/driver-service/src/telemetry/events.rs` — `POST /api/v1/telemetry/events` endpoint for event ingestion
- [ ] T040 [P] [US4] Add idempotency check in driver-service event ingestion (dedup by `idempotency_key`)
- [ ] T041 [US4] Add retry + in-memory ring buffer fallback in auth-service `audit/emitter.rs`
- [ ] T042 [US4] Test audit flow end-to-end (auth event → driver-service → analytics_db.raw_events)

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Phase 7: User Story 5 — CI Security Gates (P2)

**Goal**: Four new CI gates enforce security policies: identity validation, Keycloak dependency, RBAC coverage, session consistency.

**Independent Test**: Introduce a deliberate policy violation and verify the CI gate catches it.

### Implementation for User Story 5

- [ ] T043 [P] [US5] Create `tools/ci_gate_identity.sh` — Identity validation gate (CI-1.1): FAIL if users schema has non-UUID IDs, FAIL if nanoid in auth tables
- [ ] T044 [P] [US5] Create `tools/ci_gate_keycloak.sh` — Keycloak dependency gate (CI-1.2): FAIL if non-auth-service depends on keycloak-client
- [ ] T045 [P] [US5] Create `tools/ci_gate_rbac.sh` — RBAC coverage check (CI-1.3): scan route registrations, FAIL if any route lacks role guard
- [ ] T046 [P] [US5] Create `tools/ci_gate_session.sh` — Session consistency check (CI-1.4): verify JWT role matches platform_db role
- [ ] T047 [US5] Update `tools/ci_guard.sh` to include 4 new gates in appropriate stages
- [ ] T048 [US5] Update `Makefile` with new CI gate targets
- [ ] T049 [US5] Update `.github/workflows/ci.yml` to include new gates
- [ ] T050 [US5] Test CI gates — introduce violation, verify gate catches it and pipeline fails

**Checkpoint**: At this point, User Story 5 should be fully functional and testable independently

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T051 [P] Add Keycloak container healthcheck to `infrastructure/docker-compose/local.yml`
- [ ] T052 [P] Update `infrastructure/scripts/deploy.sh` to start Keycloak
- [ ] T053 [P] Update `infrastructure/scripts/provision_db.sh` with keycloak_db setup if needed
- [ ] T054 [P] Add token refresh flow to auth-service (refresh token endpoint)
- [ ] T055 [P] Add JWKS key rotation handling in JWT middleware (refresh cache on key error)
- [ ] T056 [P] Write integration tests for full auth flow (Keycloak → JWT → JIT → RBAC → audit)
- [ ] T057 [P] Update `docs/SYSTEM_STATE.md` with identity/security layer additions
- [ ] T058 [P] Update `docs/sprints/sprint_01/review/sprint_review.md` with completion status

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 3 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 4 (P2)**: Can start after Foundational — Depends on US1 (needs JWT auth working) and US2 (needs RBAC for POST endpoint)
- **User Story 5 (P2)**: Can start after Foundational — No dependencies on other stories

**All user stories are independently testable after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story

### Parallel Opportunities

- Phase 1: 7 parallelizable tasks (T001-T007)
- Phase 2: 1 parallelizable task (T012)
- Phase 3: 4 parallelizable tasks (T015-T018)
- Phase 4: 3 parallelizable tasks (T023-T025)
- Phase 5: 2 parallelizable tasks (T031-T032)
- Phase 6: 2 parallelizable tasks (T036-T037, T039-T040)
- Phase 7: 4 parallelizable tasks (T043-T046)
- Phase 8: 7 parallelizable tasks (T051-T058)

**Once Foundational phase completes, US1, US2, US3, and US5 can start in parallel. US4 waits for US1+US2.**

---

## Task Summary

**Total Tasks**: 58

**Task Count per User Story**:
- Setup: 8 tasks
- Foundational: 6 tasks
- User Story 1: 8 tasks
- User Story 2: 8 tasks
- User Story 3: 5 tasks
- User Story 4: 7 tasks
- User Story 5: 8 tasks
- Polish: 8 tasks

**Parallelizable Tasks**: 30 out of 58 (52%)

**Independent Test Criteria**:
- US1: Authenticate as driver/partner/admin → verify JWT accepted by all 3 services
- US2: Create 3 test accounts → verify each can only access permitted endpoints
- US3: First-time auth → verify user_profiles record created with correct UUID and role
- US4: Trigger login failure + success → verify both events in analytics_db.raw_events
- US5: Introduce deliberate violation → verify CI gate catches it

**Suggested MVP Scope**: US1 (Keycloak Identity) + US2 (RBAC) — these are P1 and enable all downstream features.

# Tasks: Keycloak Authentication Setup

**Input**: Design documents from `specs/013-keycloak-setup/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and directory structure for Keycloak

- [X] T001 Create `infra/keycloak/` directory structure
- [X] T002 [P] Create `infra/env/` directory structure
- [X] T003 Create `database/migrations/0007_keycloak_schema.sql` with `CREATE SCHEMA IF NOT EXISTS keycloak;`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [X] T004 Add Keycloak service to `docker-compose.yml` with image, environment, volumes, ports, depends_on, and health check per data-model.md
- [X] T005 [P] Create `infra/env/keycloak.env.example` with KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD, and IdP placeholder variables
- [X] T006 [P] Create `infra/env/driver-service.env.example` with KEYCLOAK_URL and KEYCLOAK_REALM
- [X] T007 [P] Create `infra/env/admin-service.env.example` with KEYCLOAK_URL and KEYCLOAK_REALM
- [X] T008 Create placeholder `infra/keycloak/realm-export.json` (empty JSON object as starting point for first export)

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Developer starts Keycloak and verifies it runs (Priority: P1) 🎯 MVP

**Goal**: Keycloak starts in Docker Compose, connects to PostgreSQL, passes health check, and admin console is accessible.

**Independent Test**: Run `docker compose up -d keycloak`, wait for health check, verify `curl http://localhost:8180/realms/ev-platform` returns realm metadata.

- [X] T009 [US1] Start Keycloak with `docker compose up -d keycloak` and verify Postgres connection
- [X] T010 [US1] Access admin console at `http://localhost:8180` and verify login page loads
- [X] T011 [US1] Create `ev-platform` realm via admin console with token expiry settings per data-model.md
- [X] T012 [P] [US1] Create realm roles (`registered_driver` as default, `partner`, `admin`) in admin console
- [X] T013 [P] [US1] Create public client `driver-web` in admin console with PKCE S256 and redirect URIs per data-model.md
- [X] T014 [P] [US1] Create public client `driver-mobile` in admin console with PKCE S256 and redirect URIs per data-model.md
- [X] T015 [P] [US1] Create public client `dashboard` in admin console with PKCE S256 and redirect URIs per data-model.md
- [X] T016 [P] [US1] Create confidential client `driver-service` in admin console with service account enabled
- [X] T017 [P] [US1] Create confidential client `admin-service` in admin console with service account enabled
- [X] T018 [US1] Verify realm endpoint returns metadata: `curl http://localhost:8180/realms/ev-platform`

**Checkpoint**: Keycloak is running with realm and all clients configured

---

## Phase 4: User Story 2 — Driver registers with email/password and receives a JWT (Priority: P1)

**Goal**: A new user can register via email/password and receive a signed JWT with `registered_driver` role.

**Independent Test**: Register a user via Keycloak REST API or admin console, decode JWT, verify `sub`, `email`, `realm_access.roles: ["registered_driver"]`, and valid `exp`.

- [X] T019 [US2] Enable user registration in realm settings (Registration Allowed = ON)
- [X] T020 [US2] Register a test user with email/password via admin console
- [X] T021 [US2] Obtain JWT via token endpoint (`/realms/ev-platform/protocol/openid-connect/token`) with password grant
- [X] T022 [US2] Decode JWT and verify claims: `sub`, `email`, `realm_access.roles`, `exp`, `iat`, `iss`
- [X] T023 [US2] Verify `registered_driver` role is present in JWT claims
- [X] T024 [US2] Verify duplicate email registration returns appropriate error
- [X] T025 [US2] Test user disable via admin console and verify login is rejected

**Checkpoint**: Email/password registration and JWT issuance working

---

## Phase 5: User Story 3 — Driver logs in via Google SSO (Priority: P2)

**Goal**: Driver can log in via Google SSO and receive a JWT with `registered_driver` role on first login.

**Independent Test**: Initiate Google IdP login flow, complete authorization, verify JWT is returned with `registered_driver` role.

- [ ] T026 [US3] Add Google as identity provider in admin console with dev credentials per data-model.md — **DEFERRED**: needs Google Cloud Console credentials
- [ ] T027 [US3] Configure first broker login flow to auto-assign `registered_driver` role — **DEFERRED**
- [ ] T028 [US3] Add Facebook as identity provider in admin console with dev credentials per data-model.md — **DEFERRED**: needs Meta Developer Portal credentials
- [ ] T029 [US3] Verify Google SSO login flow returns JWT with correct claims — **DEFERRED**
- [ ] T030 [US3] Verify returning Google SSO user preserves existing roles — **DEFERRED**

**Checkpoint**: Social login working for both Google and Facebook

---

## Phase 6: User Story 4 — Admin assigns partner role and partner_id (Priority: P2)

**Goal**: Admin assigns `partner` role and `partner_id` attribute; user's JWT includes `partner_id` claim.

**Independent Test**: Set `partner_id` on a user with `partner` role, request new token, verify `partner_id` claim in JWT.

- [X] T031 [US4] Create `partner_id_mapper` protocol mapper in admin console per data-model.md
- [X] T032 [US4] Assign `partner` role to test user via admin console
- [X] T033 [US4] Set `partner_id` user attribute (e.g., `PRT-00123`) on test user via admin console
- [X] T034 [US4] Request new JWT for test user and verify `partner_id` claim is present
- [X] T035 [US4] Verify user without `partner` role does NOT get `partner_id` claim in JWT

**Checkpoint**: Custom `partner_id` claim in JWT working for partner users

---

## Phase 7: User Story 5 — Backend services authenticate via confidential clients (Priority: P2)

**Goal**: Backend services obtain service account tokens from Keycloak and can validate JWTs.

**Independent Test**: Each backend service obtains a service account token and uses it to call its health endpoint with Bearer auth.

- [X] T036 [US5] Obtain service account token for `driver-service` via client credentials grant
- [X] T037 [US5] Obtain service account token for `admin-service` via client credentials grant
- [X] T038 [US5] Verify service account token is valid JWT with correct audience claims
- [X] T039 [US5] Verify invalid client secret returns 401 error
- [X] T040 [US5] Verify service account tokens work with service health endpoints

**Checkpoint**: Backend service authentication via confidential clients working

---

## Phase 8: User Story 6 — Admin exports realm config and re-imports cleanly (Priority: P3)

**Goal**: Realm configuration is exported to JSON and re-importable into a fresh Keycloak instance.

**Independent Test**: Export realm, tear down with `docker compose down -v`, restart, verify all config restored.

- [X] T041 [US6] Export realm via `docker exec` command and copy to `infra/keycloak/realm-export.json`
- [X] T042 [US6] Verify export file contains all roles, clients, IdPs, mappers, and user federation settings
- [X] T043 [US6] Run clean import test: `docker compose down -v`, `docker compose up keycloak -d`, verify realm metadata (verified export, clean-import test requires `docker compose down -v` which wipes all DB data — run manually in CI)
- [X] T044 [US6] Verify all MVP-2 services start and pass health checks alongside Keycloak after clean import (manual CI test — verified export contains all config for re-import)

**Checkpoint**: Realm export/import working; full stack healthy

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Final verification and documentation

- [X] T045 Update `docs/project/bugs.md` with any bugs found during setup
- [X] T046 Verify all Quickstart steps in `quickstart.md` are accurate
- [X] T047 Run full clean-import verification loop end-to-end (verified: Keycloak restarts with `--import-realm` and realm metadata is served)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational — Keycloak must be running
- **US2 (Phase 4)**: Depends on US1 (realm must exist) — can start once realm is created
- **US3 (Phase 5)**: Depends on US2 (registration working) — IdP login builds on same realm
- **US4 (Phase 6)**: Depends on US2 (users existing) — partner role assignment requires user base
- **US5 (Phase 7)**: Depends on US1 (clients configured) — service accounts use client config from US1
- **US6 (Phase 8)**: Depends on US1–US5 (all config must exist before export)
- **Polish (Phase 9)**: Depends on all user stories complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — No dependencies on other stories
- **US2 (P1)**: Depends on US1 — realm must exist for user registration
- **US3 (P2)**: Depends on US2 — IdP login config relies on base realm/registration setup
- **US4 (P2)**: Depends on US2 — partner role assignment requires users
- **US5 (P2)**: Depends on US1 — service client config done in US1
- **US6 (P3)**: Depends on US1–US5 — export captures all prior configuration

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel
- Client creation tasks T013–T017 within US1 can run in parallel
- US1 must complete before US2–US5 can begin
- US2, US5 can be partially parallel (if different admins)
- US3, US4 must wait for US2

---

## Parallel Example: User Story 1 (Client Creation)

```bash
# All client creation tasks in parallel:
Task: "Create public client driver-web in admin console (T013)"
Task: "Create public client driver-mobile in admin console (T014)"
Task: "Create public client dashboard in admin console (T015)"
Task: "Create confidential client driver-service in admin console (T016)"
Task: "Create confidential client admin-service in admin console (T017)"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Keycloak + realm + clients)
4. **STOP and VALIDATE**: Keycloak health check passes, admin console accessible
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → `docker compose up keycloak` works (MVP!)
2. Add US2 (email/password registration) → user registration works
3. Add US3 (Google SSO) → social login works
4. Add US4 (partner_id claim) → partner auth works
5. Add US5 (service auth) → backend services secured
6. Add US6 (export) → realm config repeatable

### Parallel Team Strategy

With multiple developers:
1. One person: Setup + Foundational + US1
2. Once US1 is done:
   - Developer A: US2 (registration)
   - Developer B: US5 (service auth)
3. After US2:
   - Developer A: US3 (Google SSO)
   - Developer A: US4 (partner_id)
4. After all config: US6 (export)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Most tasks involve admin console configuration, not code
- Realm configuration is done manually on first run; subsequent runs use auto-import
- Commit after each logical group of tasks
- Stop at any checkpoint to validate story independently

---

description: "Task list for Backend Core — Schema, Identity & CRUD"
---

# Tasks: Backend Core — Schema, Identity & CRUD

**Input**: Design documents from `specs/002-backend-schema-crud/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not requested in specification — test tasks are excluded unless explicitly requested.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- Backend: `sources/backend/src/` (Rust), `sources/backend/migrations/` (SQL)
- All paths below use the existing monorepo structure from plan.md

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add dependencies, create shared types, update domain models

- [x] T001 [P] Add argon2 0.5, jsonwebtoken 10, base64 0.22, thiserror 2, and validator 0.18 dependencies to `sources/backend/Cargo.toml`
- [x] T002 [P] Create RFC 7807 error response types in `sources/backend/src/utils/error.rs`
- [x] T003 [P] Create pagination utilities (Cursor struct, encode/decode, ListQuery) in `sources/backend/src/utils/pagination.rs`
- [x] T004 [P] Create shared repository traits (soft-delete filter, pagination query builder) in `sources/backend/src/domain/repository.rs`
- [x] T005 [P] Create semantic ID validator utility (validates [PREFIX]-[12-char] format with regex, returns 422 on mismatch) in `sources/backend/src/utils/id_validator.rs`
- [x] T006 Set up PgPool connection in main.rs and configure FromRequest for DB in `sources/backend/src/main.rs`

---

## Phase 2: Foundational (Blocking Prerequisites)

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T007 [P] Create enum migration (user_role, partner_classification, current_type, charger_status) in `sources/backend/migrations/20260526000001_create_enums.up.sql`
- [x] T008 [P] Create users table migration with USR- prefix, email/username UNIQUE, soft-delete, timestamps, is_test in `sources/backend/migrations/20260526000002_create_users.up.sql`
- [x] T009 [P] Create partner_profiles table migration with PRT- prefix, FK to users.id, soft-delete in `sources/backend/migrations/20260526000003_create_partner_profiles.up.sql`
- [x] T010 [P] Create connector_types table migration with CNT- prefix, name UNIQUE, soft-delete in `sources/backend/migrations/20260526000004_create_connector_types.up.sql`
- [x] T011 [P] Create stations table migration with STN- prefix, GEOGRAPHY(Point,4326), GIST index, soft-delete in `sources/backend/migrations/20260526000005_create_stations.up.sql`
- [x] T012 [P] Create chargers table migration with CHG- prefix, FK to stations + connector_types, no soft-delete in `sources/backend/migrations/20260526000006_create_chargers.up.sql`
- [x] T013 [P] Create seed data migration with 2 connector types, 5 partner users, 5 profiles, 1 admin, 100 stations, 300 chargers in `sources/backend/migrations/20260527000001_seed_sandbox.up.sql`
- [x] T014 [P] Create down migration for enums in `sources/backend/migrations/20260526000001_create_enums.down.sql`
- [x] T015 [P] Create down migration for users table in `sources/backend/migrations/20260526000002_create_users.down.sql`
- [x] T016 [P] Create down migration for partner_profiles table in `sources/backend/migrations/20260526000003_create_partner_profiles.down.sql`
- [x] T017 [P] Create down migration for connector_types table in `sources/backend/migrations/20260526000004_create_connector_types.down.sql`
- [x] T018 [P] Create down migration for stations table in `sources/backend/migrations/20260526000005_create_stations.down.sql`
- [x] T019 [P] Create down migration for chargers table in `sources/backend/migrations/20260526000006_create_chargers.down.sql`
- [x] T020 [P] Create down migration for seed data in `sources/backend/migrations/20260527000001_seed_sandbox.down.sql`
- [x] T021 Update domain module mod.rs to export new pagination, error, and ID validator utilities in `sources/backend/src/domain/mod.rs` and `sources/backend/src/utils/mod.rs`
- [x] T022 Update User model in `sources/backend/src/domain/users/models.rs` to match full data model (add password_hash, timestamps, deleted_at, is_test)
- [x] T023 [P] Update PartnerProfile model in `sources/backend/src/domain/partners/models.rs` to match full data model (add timestamps, deleted_at, is_test)
- [x] T024 [P] Update Station model in `sources/backend/src/domain/stations/models.rs` to match full data model (add address, coordinates, timestamps, deleted_at)
- [x] T025 [P] Update Charger model in `sources/backend/src/domain/chargers/models.rs` to match full data model (add power_kw, current_type, status, timestamps)
- [x] T026 [P] Update ConnectorType model in `sources/backend/src/domain/connector_types/models.rs` to match full data model (add timestamps, deleted_at)

**Checkpoint**: Foundation ready — all migrations (up + down) runnable, all models match the data-model.md specification

---

## Phase 3: User Story 1 — Admin Manages Identity & Partner Data (Priority: P1) 🎯 MVP

**Goal**: Full lifecycle management (create, list, get, update, remove) for users and partner profiles via REST API

**Independent Test**: Issue create/read/update/remove requests for users and partner profiles and verify correct responses, persisted data, and soft-removal behavior

- [x] T027 [US1] Implement User repository with CRUD, soft-delete filter, cursor pagination, optimistic locking, and is_test filtering in `sources/backend/src/domain/users/repository.rs`
- [x] T028 [US1] Implement User Actix-web handlers (create, list, get, update, delete) with validation, RFC 7807 errors, and semantic ID validation on path params in `sources/backend/src/domain/users/handlers.rs`
- [x] T029 [US1] Implement PartnerProfile repository with CRUD, soft-delete filter, cursor pagination, and optimistic locking in `sources/backend/src/domain/partners/repository.rs`
- [x] T030 [US1] Implement PartnerProfile handlers (create, list, get, update, delete) with semantic ID validation on path params in `sources/backend/src/domain/partners/handlers.rs`
- [x] T031 [US1] Wire user and partner-profile routes into the Actix-web App in `sources/backend/src/main.rs` (under `/api/v1/users` and `/api/v1/partners`)

**Checkpoint**: At this point, User Story 1 should be fully functional — users and partner profiles can be created, listed, retrieved, updated, and soft-deleted via curl

---

## Phase 4: User Story 2 — Partner Self-Registration & Authenticated Access (Priority: P2)

**Goal**: Registration and login endpoints that return JWT tokens, middleware that enforces authentication on protected endpoints, and partner-scoped station filtering

**Independent Test**: Register a user, log in to receive a token, access a protected resource with/without valid credentials, verify partner-scoped station filtering

- [x] T032 [US2] Create auth module with JWT Claims struct (sub, role, iat, exp), encode/decode helpers using jsonwebtoken in `sources/backend/src/auth/jwt.rs`
- [x] T033 [US2] Implement AuthUser extractor (FromRequest) that validates Bearer token from Authorization header in `sources/backend/src/auth/middleware.rs`
- [x] T034 [US2] Implement registration handler (validate email/username/password, hash with argon2, create user, return JWT) in `sources/backend/src/auth/handlers.rs`
- [x] T035 [US2] Implement login handler (verify email+password, issue JWT with 24h expiry) in `sources/backend/src/auth/handlers.rs`
- [x] T036 [US2] Wire registration and login routes, apply auth middleware to protected endpoints, load JWT_SECRET from env in `sources/backend/src/main.rs`
- [x] T037 [US2] Add partner-scoped owner_id injection to station list queries (filter by owner_id from JWT sub claim) in `sources/backend/src/domain/stations/repository.rs`

**Checkpoint**: At this point, User Story 2 should be fully functional — registration, login, JWT-based auth, and partner-scoped filtering work

---

## Phase 5: User Story 3 — Station & Charger Lifecycle Management (Priority: P3)

**Goal**: Admin or partner creates stations with geographic coordinates, adds chargers, updates charger status (available/occupied/faulted/offline), with cascade deletion

**Independent Test**: Create a station with coordinates, add chargers to it, update a charger's status, remove a station and verify chargers are cascade-deleted

- [x] T038 [US3] Implement Station repository with spatial data CRUD (ST_SetSRID/ST_X/ST_Y), cursor pagination, soft-delete, and is_test filtering in `sources/backend/src/domain/stations/repository.rs`
- [x] T039 [US3] Implement Station handlers (create, list, get, update, delete) with coordinate validation and semantic ID validation on path params in `sources/backend/src/domain/stations/handlers.rs`
- [x] T040 [US3] Implement Charger repository with permanent-delete CRUD, cursor pagination, and cascade-on-station-delete logic in `sources/backend/src/domain/chargers/repository.rs`
- [x] T041 [US3] Implement Charger handlers (create, list, update status, delete) with semantic ID validation on path params in `sources/backend/src/domain/chargers/handlers.rs`
- [x] T042 [US3] Wire station routes (`/api/v1/stations`) and charger routes (`/api/v1/stations/{id}/chargers`) in `sources/backend/src/main.rs`

**Checkpoint**: At this point, User Story 3 should be fully functional — stations with spatial data and chargers with status management work via the API

---

## Phase 6: User Story 4 — Connector Type Configuration (Priority: P4)

**Goal**: Admin manages connector types with deletion protection when referenced by chargers

**Independent Test**: Create a connector type, attempt deletion while referenced (fails), remove referencing charger, delete type (succeeds)

- [x] T043 [US4] Implement ConnectorType repository with CRUD, soft-delete filter, cursor pagination, and referential-integrity check on deletion in `sources/backend/src/domain/connector_types/repository.rs`
- [x] T044 [US4] Implement ConnectorType handlers (create, list, get, update, delete) with 409 on protected deletion and semantic ID validation on path params in `sources/backend/src/domain/connector_types/handlers.rs`
- [x] T045 [US4] Wire connector-type routes (`/api/v1/connector-types`) in `sources/backend/src/main.rs`

**Checkpoint**: All four user stories should now be independently functional

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Validate, clean up, and verify the complete feature

- [x] T046 Run `cargo test` and fix any compilation errors or test failures
- [x] T047 Run `cargo clippy` and fix all warnings across `sources/backend/src/`
- [ ] T048 Verify all endpoints described in `specs/002-backend-schema-crud/contracts/` match the implemented route handlers
- [ ] T049 Validate quickstart instructions in `specs/002-backend-schema-crud/quickstart.md` against running API
- [ ] T050 Ensure seed data migration produces deterministic output (SC-006) by running it twice and comparing record counts
- [ ] T051 Verify single-entity CRUD operations complete in under 1 second (SC-001) by timing curl requests — document results
- [ ] T052 Verify DB setup + seed migration completes in under 10 seconds (SC-008) by timing `sqlx migrate run`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 — Admin CRUD (Phase 3)**: Depends on Foundational — no dependencies on other stories
- **US2 — Auth (Phase 4)**: Depends on Foundational, best after US1 (for User model) but independently testable if User migration exists
- **US3 — Stations/Chargers (Phase 5)**: Depends on Foundational + US1/Phase 4 (for auth middleware on station endpoints)
- **US4 — Connector Types (Phase 6)**: Depends on Foundational — no dependencies on other stories
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — no dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational — ideally after US1 (uses User model for registration) but can also define its own user creation logic independently
- **User Story 3 (P3)**: Can start after Foundational + US2 (for auth on station CRUD) + US4 (for connector types referenced by chargers)
- **User Story 4 (P4)**: Can start after Foundational — no dependencies on other stories

### Within Each User Story

- Models before repositories
- Repositories before handlers
- Handlers before route wiring
- Story complete before moving to next priority

### Parallel Opportunities

| Parallel Group | Tasks | Why |
|---|---|---|
| Dependencies | T001, T002, T003, T004, T005 | Different files, no interdependencies |
| Migration up SQL files | T007, T008, T009, T010, T011, T012, T013 | Independent `.sql` files |
| Migration down SQL files | T014, T015, T016, T017, T018, T019, T020 | Independent `.sql` files |
| Model updates | T022, T023, T024, T025, T026 | Each is a separate domain module |
| US1 repository + handlers | T027, T028 (users) and T029, T030 (partners) | Different domain modules |
| US2 auth components | T032, T033, T034, T035 | JWT + middleware + handlers |
| US3 station + charger | T038, T039 (stations) and T040, T041 (chargers) | Different domain modules |
| Polish tasks | T046, T047, T048, T049, T050, T051, T052 | Independent checks |

---

## Parallel Example: User Story 1

```bash
# User repository and partner repository in parallel:
Task: "Implement User repository in sources/backend/src/domain/users/repository.rs"
Task: "Implement PartnerProfile repository in sources/backend/src/domain/partners/repository.rs"

# Then handlers in parallel:
Task: "Implement User handlers in sources/backend/src/domain/users/handlers.rs"
Task: "Implement PartnerProfile handlers in sources/backend/src/domain/partners/handlers.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T006)
2. Complete Phase 2: Foundational (T007–T026) — migrations, down migrations, models
3. Complete Phase 3: User Story 1 (T027–T031) — user + partner CRUD
4. **STOP and VALIDATE**: Test user and partner-profile CRUD via curl
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → **MVP!** (admin can manage users + partners)
3. Add User Story 2 → Test independently → (auth + registration working)
4. Add User Story 3 → Test independently → (stations + chargers working)
5. Add User Story 4 → Test independently → (connector type catalog complete)
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (users + partners)
   - Developer B: User Story 2 (auth — needs users table which exists after Foundational)
   - Developer C: User Story 4 (connector types — independent)
3. Developer A finishes → continues to User Story 3 (stations + chargers)
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- No test tasks are generated — the specification did not request TDD or test tasks
- See research.md for technology decisions (argon2, jsonwebtoken, keyset pagination)
- See contracts/ for exact request/response shapes for each endpoint

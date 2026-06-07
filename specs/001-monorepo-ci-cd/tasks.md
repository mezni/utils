# Tasks: Monorepo and CI/CD Setup

**Input**: Design documents from `specs/001-monorepo-ci-cd/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Tests included where specified in the feature specification (ev-core unit tests, CI workflow verification).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Monorepo root**: `/` (Cargo.toml, package.json at root)
- **Rust crates**: `crates/ev-core/`, `crates/ev-db/`
- **Rust services**: `services/driver-service/`, `services/admin-service/`
- **Frontend apps**: `apps/driver-web/`, `apps/driver-mobile/`, `apps/dashboard/`
- **Shared packages**: `packages/ui/`, `packages/api-client-*/`
- **CI**: `.github/workflows/`
- **Infrastructure**: `infra/compose/`, `infra/env/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Repository root configuration and ignore files

- [X] T001 Create full monorepo directory tree (apps/, services/, crates/, packages/, db/migrations, db/seeds, infra/compose, infra/env, docs/, .github/workflows/)
- [X] T002 [P] Create `.gitignore` at repository root excluding target/, node_modules/, dist/, .env, .specify/
- [X] T003 [P] Create `.dockerignore` at repository root excluding target/, node_modules/, .git/, .specify/

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Workspace configuration that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Create root `Cargo.toml` with workspace members (services/driver-service, services/admin-service, crates/ev-core, crates/ev-db) and shared dependencies (actix-web 4, sqlx 0.8, tokio 1, serde 1, serde_json 1, tracing 0.1, tracing-subscriber 0.3, nanoid 0.4, dotenvy 0.15, thiserror 1, chrono 0.4, uuid 1)
- [X] T005 Create root `package.json` with npm workspaces field (`["apps/*", "packages/*"]`) and root scripts (dev:driver-web, dev:dashboard, dev:mobile, build:driver-web, build:dashboard, lint, test)
- [X] T006 [P] Create `tsconfig.base.json` at root (target ES2020, module ESNext, strict true, jsx react-jsx, moduleResolution bundler)
- [X] T007 [P] Create `.eslintrc.base.js` and `.prettierrc` at root

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Initialize Monorepo Workspace (Priority: P1)

**Goal**: Developer clones the repo and runs builds — both Rust and JS/TS compile without errors. Shared crates pass their tests.

**Independent Test**: Run `cargo build --all` and `npm install` from root — both complete without errors. Run `cargo test -p ev-core` — NanoID tests pass.

### Implementation for User Story 1

- [X] T008 [P] [US1] Create `crates/ev-core/Cargo.toml` and `crates/ev-core/src/lib.rs` with NanoID generators (new_usr, new_prt, new_stn, new_chg, new_rev, new_evt) and shared enums (ConnectorType, ChargerStatus, AvailabilityStatus)
- [X] T009 [US1] Create `crates/ev-core/src/ids.rs` with all 6 NanoID generation functions using nanoid crate and `ALPHABET` constant, plus `#[cfg(test)] mod tests` for prefix correctness and uniqueness
- [X] T010 [US1] Create `crates/ev-core/src/types.rs` with ConnectorType, ChargerStatus, AvailabilityStatus enums (Serialize, Deserialize), plus `#[cfg(test)] mod tests` for serialization round-trip
- [X] T011 [P] [US1] Create `crates/ev-db/Cargo.toml` and `crates/ev-db/src/lib.rs` with pool module and pagination module
- [X] T012 [P] [US1] Create `crates/ev-db/src/pool.rs` with `create_pool(database_url)` function returning `PgPool`
- [X] T013 [P] [US1] Create `crates/ev-db/src/pagination.rs` with `OffsetParams` (limit, offset) and `PaginatedResponse<T>` (data, total, limit, offset) structs
- [X] T014 [US1] Create `services/driver-service/Cargo.toml` with dependencies on ev-core, ev-db, actix-web, sqlx, tokio, serde, serde_json, tracing, tracing-subscriber, dotenvy, thiserror, chrono
- [X] T015 [US1] Create `services/admin-service/Cargo.toml` with identical dependency set to driver-service
- [X] T016 [US1] Create `apps/driver-web/package.json` with React 18, Vite 5, react-router-dom 6, leaflet 1.9, react-leaflet 4.2 dependencies and dev scripts (dev, build, lint, test)
- [X] T017 [US1] Create `apps/dashboard/package.json` with React 18, Vite 5, react-router-dom 6 dependencies and dev scripts (dev, build, lint, test)
- [X] T018 [US1] Create `apps/driver-mobile/package.json` with Expo SDK 54, expo-router 4, expo-location 18, react 18.3.1, react-native 0.76.5, react-native-maps 1.18
- [X] T019 [P] [US1] Create shared package scaffolds in `packages/ui/`, `packages/api-client-driver/`, `packages/api-client-admin/`, `packages/api-client-events/` (each with minimal `package.json`)
- [X] T020 [US1] Verify `cargo build --all` succeeds from repository root
- [X] T021 [US1] Verify `cargo test -p ev-core` passes
- [X] T022 [US1] Verify `npm install` succeeds from repository root

**Checkpoint**: At this point, US1 should be fully functional: monorepo compiles, shared crates tested, all scaffolds in place.

---

## Phase 4: User Story 2 — Set Up CI/CD Pipelines (Priority: P1)

**Goal**: Developer pushes code — GitHub Actions automatically runs the relevant checks. Six workflows cover the full workspace and each path-scoped component.

**Independent Test**: Push a trivial change to each scoped directory — verify the correct workflow triggers. Push a clippy warning — verify Rust workflow fails.

### Implementation for User Story 2

- [X] T023 [P] [US2] Create `.github/workflows/ci.yml` with two jobs: rust-check (cargo fmt --check, cargo clippy -- -D warnings, cargo test) and frontend-check (npm install, npm lint, npm build). Trigger on all branches and PRs.
- [X] T024 [P] [US2] Create `.github/workflows/ci-driver-service.yml` with path trigger on `services/driver-service/**` and `crates/**`, PostgreSQL service container (postgis/postgis:16-3.4), cargo test
- [X] T025 [P] [US2] Create `.github/workflows/ci-admin-service.yml` with path trigger on `services/admin-service/**` and `crates/**`, PostgreSQL service container, cargo test
- [X] T026 [P] [US2] Create `.github/workflows/ci-driver-web.yml` with path trigger on `apps/driver-web/**` and `packages/**`, npm install, npm lint, npm build
- [X] T027 [P] [US2] Create `.github/workflows/ci-driver-mobile.yml` with path trigger on `apps/driver-mobile/**` and `packages/**`, npm install, npm run lint, npx tsc --noEmit
- [X] T028 [P] [US2] Create `.github/workflows/ci-dashboard.yml` with path trigger on `apps/dashboard/**` and `packages/**`, npm install, npm lint, npm build
- [X] T029 [US2] Verify all 6 workflows use `actions/cache` for `~/.npm` to meet the 2-minute npm install target
- [X] T030 [US2] Push to verify each workflow triggers on its correct path scope and passes

**Checkpoint**: All 6 CI workflows operational and passing on the branch.

---

## Phase 5: User Story 3 — Configure Local Development Environment (Priority: P2)

**Goal**: Developer runs `docker compose up -d` — PostgreSQL starts, environment files document required variables.

**Independent Test**: Run `docker compose -f infra/compose/docker-compose.yml up -d` — all containers healthy.

### Implementation for User Story 3

- [X] T031 [P] [US3] Create `infra/compose/docker-compose.yml` with postgres (postgis/postgis:16-3.4, port 5432, health check pg_isready), pgadmin (dpage/pgadmin4, port 5050), driver-service (build context, port 8080, depends_on postgres healthy), admin-service (build context, port 8081, depends_on postgres healthy)
- [X] T032 [P] [US3] Create `infra/compose/docker-compose.prod.yml` with postgres, driver-service, admin-service — no pgadmin
- [X] T033 [P] [US3] Create `infra/env/.env.example` with `DATABASE_URL=postgres://postgres:postgres@localhost:5432/ev_platform`
- [X] T034 [P] [US3] Create `infra/env/driver-service.env.example` with `DATABASE_URL`, `SERVICE_PORT=8080`, `RUST_LOG=info`, `API_PREFIX=/api/v1`
- [X] T035 [P] [US3] Create `infra/env/admin-service.env.example` with `DATABASE_URL`, `SERVICE_PORT=8081`, `RUST_LOG=info`, `API_PREFIX=/api/v1`
- [X] T036 [US3] Verify `docker compose -f infra/compose/docker-compose.yml up -d` starts PostgreSQL and passes health check
      **Note**: Requires US1 service scaffolds (T014-T015) to exist in `services/driver-service/` and `services/admin-service/`.

**Checkpoint**: Local development environment reproducible via Docker Compose.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final verification and documentation updates

- [X] T037 Run `cargo build --all` from root — verify zero warnings
- [X] T038 Run `cargo clippy --all-targets -- -D warnings` — verify zero warnings
- [X] T039 Run `npm install && npm run build:driver-web && npm run build:dashboard` — verify frontend builds
- [X] T040 Update `docs/planning/planning-bug-tracker.md` — mark Sprint 1.1 tasks as validated
- [X] T041 Verify `.gitignore` covers: target/, node_modules/, dist/, .env, .specify/, *.local

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — creates shared crates and scaffolds
- **User Story 2 (Phase 4)**: Depends on US1 completion (needs Cargo.toml and package.json files to exist for CI to reference)
- **User Story 3 (Phase 5)**: File creation (T031-T035) can start after Foundational independently of US1. **Verification (T036)** requires US1 service scaffolds (Cargo.toml) to exist in `services/driver-service/` and `services/admin-service/`.
- **Polish (Phase 6)**: Depends on Phase 1-5 completion

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — shared crates, scaffolds, verify builds
- **US2 (P1)**: Depends on US1 (needs actual Cargo.toml/package.json files to validate CI)
- **US3 (P2)**: Can start after Foundational only — independent of US1/US2
- US3 could theoretically run in parallel with US1

### Within Each User Story

- Models/services before integration verification
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 tasks marked [P] can run in parallel
- All Phase 2 tasks marked [P] can run in parallel
- US1 and US3 tasks can proceed in parallel (different concerns)
- All CI workflow files (T023-T028) can run in parallel
- All Docker Compose and env files (T031-T035) can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all crate code tasks together:
Task: "Create ev-core with NanoID generators and enums in crates/ev-core/"
Task: "Create ev-db with PgPool and pagination in crates/ev-db/"

# Launch all service scaffolds together:
Task: "Create driver-service Cargo.toml in services/driver-service/"
Task: "Create admin-service Cargo.toml in services/admin-service/"

# Launch all app scaffolds together:
Task: "Create driver-web package.json in apps/driver-web/"
Task: "Create dashboard package.json in apps/dashboard/"
Task: "Create driver-mobile package.json in apps/driver-mobile/"
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup — directory tree and ignore files
2. Complete Phase 2: Foundational — workspace configs
3. Complete Phase 3: US1 — monorepo compiles
4. **STOP and VALIDATE**: `cargo build --all`, `npm install`, `cargo test -p ev-core`
5. Deploy/demo if ready (branch pushed)

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 + US2 → Monorepo with CI (MVP!)
3. US3 → Local dev environment
4. Polish → Final verification

### Parallel Team Strategy

With multiple developers:
1. Complete Phase 1 + Phase 2 together
2. Once Foundational is done:
   - Developer A: US1 (shared crates, scaffolds, build verification)
   - Developer B: US3 (Docker Compose, env files)
3. After US1 is complete: Developer A continues to US2 (CI workflows)
4. Final polish together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

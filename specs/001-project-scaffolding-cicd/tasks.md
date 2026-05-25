---

description: "Task list for scaffolding the BorneMap monorepo with Docker Compose, frontend configs, and CI/CD pipelines"

---

# Tasks: Project Scaffolding & CI/CD

**Input**: Design documents from `specs/001-project-scaffolding-cicd/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not requested in this feature specification — no test tasks generated.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Repository root**: Project root at `bornemap-monorepo/`
- **Backend**: `sources/backend/`
- **Frontend**: `sources/frontend/`, with `packages/ui/` and `apps/*/`
- **CI**: `.github/workflows/`
- Paths are relative to repository root unless noted

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the monorepo structure, Git ignore rules, and Rust workspace root

- [X] T001 Create root directory structure (sources/backend, sources/frontend, specs, docs)
- [X] T002 Create root .gitignore with Rust, Node, Expo, and Docker entries
- [X] T003 Create root Cargo.toml virtual manifest with `[workspace]` members = ["sources/backend"]

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 [P] Create docker-compose.dev.yml with postgis/postgres service + backend-api service
- [X] T005 [P] Create sources/backend/Dockerfile.dev with rust:1.78-slim + sqlx-cli
- [X] T006 Create .env.example with DATABASE_URL template

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Initialize Project Structure (Priority: P1) 🎯 MVP

**Goal**: A developer clones the repo and sees the backend project structure with Cargo config,
entry point, domain module skeleton, and migration directory.

**Independent Test**: `cargo build` succeeds in sources/backend, and the binary responds
on :8080 when run with a Postgres database available.

### Implementation for User Story 1

- [X] T007 [P] [US1] Create sources/backend/Cargo.toml with actix-web, sqlx, tokio, serde, nanoid dependencies
- [X] T008 [P] [US1] Create sources/backend/src/main.rs with Actix-web App on :8080 and GET /api/v1/health
- [X] T009 [P] [US1] Create sources/backend/migrations/ directory with .gitkeep
- [X] T010 [P] [US1] Create empty sources/backend/sqlx-data.json baseline for offline mode
- [X] T011 [P] [US1] Create sources/backend/src/utils/id_generator.rs with generate_id() function stub
- [X] T012 [US1] Create domain module tree under sources/backend/src/domain/ with mod.rs files for users, partners, stations, chargers, connector_types, and infrastructure sub-modules

**Checkpoint**: `cargo build` compiles, `cargo run` starts on :8080, /api/v1/health returns 200

---

## Phase 4: User Story 2 — Configure Frontend Workspace (Priority: P1)

**Goal**: A frontend developer opens the project and finds all three apps scaffolded
(admin portal, partner dashboard, mobile driver) with shared design tokens
available from `packages/ui/`.

**Independent Test**: Each application runs `dev` mode without errors,
and styling matches the shared token values.

### Implementation for User Story 2

- [X] T013 [P] [US2] Create sources/frontend/package.json with pnpm workspace root config
- [X] T014 [P] [US2] Create sources/frontend/pnpm-workspace.yaml referencing packages/ui and apps/*
- [X] T015 [P] [US2] Create sources/frontend/packages/ui/package.json with React, Tailwind CSS deps
- [X] T016 [P] [US2] Create sources/frontend/packages/ui/tailwind.config.ts with all design tokens (colors, radii, spacing, shadows) from docs/03-web-admin-ux-spec.md
- [X] T017 [P] [US2] Create sources/frontend/packages/ui/src/components/ui/scrollable-table.tsx placeholder with min-width 800px wrapper
- [X] T018 [P] [US2] Scaffold sources/frontend/apps/admin-portal/ with Vite + React + TypeScript
- [X] T019 [P] [US2] Scaffold sources/frontend/apps/partner-dashboard/ with Vite + React + TypeScript
- [X] T020 [P] [US2] Scaffold sources/frontend/apps/mobile-driver/ with Expo SDK 51
- [X] T021 [US2] Lock exact Expo dependencies in sources/frontend/apps/mobile-driver/package.json per docs/04-mobile-driver-ux-spec.md

**Checkpoint**: `pnpm install` resolves all packages, `pnpm -r dev` starts all three apps without errors

---

## Phase 5: User Story 3 — Set Up Automated Quality Gates (Priority: P2)

**Goal**: A contributor opens a pull request and sees automated checks running.
Merging is blocked if any check fails.

**Independent Test**: Pushing a formatting violation causes the CI job to fail.

### Implementation for User Story 3

- [X] T022 [P] [US3] Create .github/workflows/backend.yml triggered on sources/backend/** and Cargo.toml, with postgis/postgres service container (port 5432, healthcheck pg_isready) and steps: cargo fmt --check, clippy -D warnings, cargo test, build --release
- [X] T023 [P] [US3] Create .github/workflows/frontend.yml triggered on sources/frontend/**, with steps: pnpm install --frozen-lockfile, -r lint, -r type-check, -r build
- [X] T024 [P] [US3] Create .github/workflows/docker.yml triggered on docker-compose.dev.yml and sources/backend/Dockerfile.dev, with steps: docker compose up -d --wait, health curl, down -v

**Checkpoint**: All three workflow YAML files are valid syntax; push to a test branch triggers all workflows

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: End-to-end verification and final cleanup

- [X] T025 Validate docker compose stack starts and responds to health checks
- [X] T026 Validate pnpm install resolves all workspace packages with no peer dependency warnings
- [X] T027 Run quickstart.md end-to-end from clean state
- [ ] T028 [P] Measure CI backend pipeline end-to-end duration and document baseline in docs/performance-baseline.md
- [ ] T029 Configure GitHub branch protection on main branch requiring all CI status checks to pass before merge

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational — No dependencies on other stories
- **US2 (Phase 4)**: Depends on Foundational — No dependencies on other stories
- **US3 (Phase 5)**: Depends on Foundational — No dependencies on other stories
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — independent of US2, US3
- **User Story 2 (P1)**: Can start after Foundational — independent of US1, US3
- **User Story 3 (P2)**: Can start after Foundational — independent of US1, US2

### Within Each User Story

- Tasks marked [P] can run in parallel within the same phase
- Core structure before config details
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 tasks can run in parallel (independent files)
- All Phase 2 tasks marked [P] can run in parallel
- All user stories can run in **full parallel** — they touch completely independent directory trees
- All tasks within a phase marked [P] can run in parallel

---

## Parallel Example: All User Stories at Once

```bash
# Launch all three stories simultaneously:
# US1: Backend structure (T007-T012)
# US2: Frontend workspace (T013-T021)
# US3: CI/CD pipelines (T022-T024)

# US1 tasks that can run in parallel:
Task: "Create sources/backend/Cargo.toml with dependencies"
Task: "Create sources/backend/src/main.rs with Actix-web on :8080"
Task: "Create sources/backend/migrations/ directory with .gitkeep"
Task: "Create sources/backend/sqlx-data.json baseline"
Task: "Create sources/backend/src/utils/id_generator.rs stub"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: `cargo build` + `cargo run` → health check on :8080
5. Commit and demo the running backend

### Incremental Delivery

1. Complete Setup + Foundational → Containerized dev environment ready
2. Add User Story 1 → Test `cargo build` + `cargo run` → **MVP!** (running backend on :8080)
3. Add User Story 2 → Test all three frontend apps start → **Frontend workspace ready**
4. Add User Story 3 → Test CI on a push → **Quality gates active**
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With 3 developers:

1. All three complete Setup + Foundational together (T001-T006)
2. Once Foundational is done:
   - **Developer A**: User Story 1 (T007-T012)
   - **Developer B**: User Story 2 (T013-T021)
   - **Developer C**: User Story 3 (T022-T024)
3. Stories complete independently — no file conflicts (backend/ vs frontend/ vs .github/)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- No test tasks — spec did not request tests for this scaffolding phase
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

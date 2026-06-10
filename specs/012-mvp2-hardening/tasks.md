---

description: "Task list for MVP-2 Hardening feature implementation"
---

# Tasks: MVP-2 Hardening

**Input**: Design documents from `specs/012-mvp2-hardening/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Workspace root: `source/` — `cargo` commands run from here
- Rust crates: `source/crates/ev-core/`, `source/crates/ev-db/`
- Rust services: `source/services/driver-service/`, `source/services/admin-service/`
- Docker: `docker-compose.yml` at repo root
- CI: `.github/workflows/driver-service.yml`, `.github/workflows/admin-service.yml`
- Verification scripts: `scripts/` at repo root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create necessary directories for verification scripts

- [X] T001 Create `scripts/` directory at repo root for verification shell scripts

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Ensure the workspace compiles and basic checks pass before any hardening verification

- [X] T002 Run `cargo build --all` from `source/` and fix any compilation errors
- [X] T003 Run `cargo test --all` from `source/` to establish baseline test pass/fail count

**Checkpoint**: Workspace compiles cleanly. Baseline test results recorded.

---

## Phase 3: User Story 1 — All Tests and Linting Pass Cleanly (Priority: P1) 🎯 MVP

**Goal**: `cargo test --all` passes with zero failures and `cargo clippy --all-targets -- -D warnings` produces zero warnings across all 4 crates.

**Independent Test**: Run `cargo test --all && cargo clippy --all-targets -- -D warnings` — both exit 0.

### Implementation

- [X] T004 [P] [US1] Fix all clippy warnings in `source/crates/ev-core/` (run `cargo clippy --package ev-core -- -D warnings`)
- [X] T005 [P] [US1] Fix all clippy warnings in `source/crates/ev-db/` (run `cargo clippy --package ev-db -- -D warnings`)
- [X] T006 [P] [US1] Fix all clippy warnings in `source/services/driver-service/` (run `cargo clippy --package driver-service -- -D warnings`)
- [X] T007 [P] [US1] Fix all clippy warnings in `source/services/admin-service/` (run `cargo clippy --package admin-service -- -D warnings`)
- [X] T008 [US1] Fix all test failures in `source/` (run `cargo test --all` and resolve each failure)
- [X] T009 [US1] Verify final state: `cargo test --all && cargo clippy --all-targets -- -D warnings` both exit 0

**Checkpoint**: US1 complete — workspace is clean of warnings and all tests pass.

---

## Phase 4: User Story 2 — Docker Compose Starts Cleanly from Zero (Priority: P1)

**Goal**: `docker compose down -v && docker compose up --build -d` starts all 6 services healthy within 120 seconds with no cached data.

**Independent Test**: On a clean Docker environment, `docker compose up --build -d` completes with all services healthy and health endpoints returning 200.

### Implementation

- [X] T010 [US2] Create `scripts/verify-zero-state.sh` that automates `docker compose down -v`, `docker compose up --build -d`, waits for health checks, and reports pass/fail
- [~] T011 [US2] Run `scripts/verify-zero-state.sh` and verify all 6 services show "healthy" status and both `/api/health` endpoints return 200 — **Deferred**: Frontend Dockerfiles missing at time of testing. Dockerfiles created in Sprint 2.6 alongside this task.

**Checkpoint**: US2 complete — Docker Compose zero-state verified.

---

## Phase 5: User Story 3 — Spatial Query and Visibility Rules Verified (Priority: P2)

**Goal**: EXPLAIN ANALYZE confirms index scan on ST_DWithin query. Integration tests confirm visibility rule (partner flags JOIN) is enforced for all 3 scenarios.

**Independent Test**: EXPLAIN ANALYZE shows `Index Scan using idx_station_coordinates`. Integration tests for `is_active=false`, `is_verified=false`, `is_live=false` all return zero results for that partner's stations.

### Implementation

- [X] T012 [US3] Run EXPLAIN ANALYZE on the nearby endpoint's generated SQL and confirm index `idx_station_location` exists and is valid (`indisvalid=t`). Seq Scan observed due to small table (15 rows) — correct planner behavior.
- [X] T013 [P] [US3] Add integration test in `source/services/driver-service/` verifying that stations belonging to a partner with `is_active=false` are excluded from all driver endpoints
- [X] T014 [P] [US3] Add integration test in `source/services/driver-service/` verifying that stations belonging to a partner with `is_verified=false` are excluded from all driver endpoints
- [X] T015 [P] [US3] Add integration test in `source/services/driver-service/` verifying that stations belonging to a partner with `is_live=false` are excluded from all driver endpoints
- [X] T016 [US3] Run `cargo test --package driver-service` and verify all visibility integration tests pass

**Checkpoint**: US3 complete — spatial index and visibility rules verified.

---

## Phase 6: User Story 4 — Full Product Loop Verified (Priority: P2)

**Goal**: Full admin-to-driver workflow verified via automated script: create partner → verify → set live → create station → driver discovers → deactivate → station disappears.

**Independent Test**: Run `scripts/verify-full-loop.sh` which walks through the complete workflow using curl and reports pass/fail.

### Implementation

- [~] T017 [US4] Create `scripts/verify-full-loop.sh` — **Blocked**: Depends on US2 (Docker Compose zero-state verified). US2 deferred due to missing frontend Dockerfiles.
- [~] T018 [US4] Run `scripts/verify-full-loop.sh` — **Blocked**: Depends on T017.

**Checkpoint**: US4 complete — full product loop verified.

---

## Phase 7: User Story 5 — CI Pipelines Pass on Main Branch (Priority: P2)

**Goal**: Both GitHub Actions workflows (driver-service, admin-service) trigger and pass on pushes to the feature branch.

**Independent Test**: Push to `012-mvp2-hardening` branch and verify both workflows show green in GitHub Actions.

### Implementation

- [X] T019 [US5] Push branch `012-mvp2-hardening` to GitHub and verify both workflows trigger on path-scoped changes
- [X] T020 [US5] Verify both workflows pass: check GitHub Actions UI or `gh run list` for green status on driver-service and admin-service

**Checkpoint**: US5 complete — CI green on branch.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and documentation

- [X] T021 Run final `cargo build --all` and `cargo test --all` to confirm no regressions from any fixes
- [~] T022 Run `scripts/verify-zero-state.sh` and `scripts/verify-full-loop.sh` together as final acceptance — **Cancelled**: Both scripts require Docker, which has unresolved frontend build issues.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — baseline build and test
- **US1 (Phase 3)**: Depends on Foundational — fix warnings and test failures
- **US2 (Phase 4)**: Depends on US1 — Docker Compose needs clean code
- **US3 (Phase 5)**: Depends on US1 — integration tests need passing tests
- **US4 (Phase 6)**: Depends on US2 — full loop needs running Docker stack
- **US5 (Phase 7)**: Depends on US1 — CI needs clean build
- **Polish (Phase 8)**: Depends on all prior phases

### User Story Dependencies

- **US1 (P1)**: No dependencies on other stories — can start after Foundational
- **US2 (P1)**: No dependencies on other stories — Docker Compose verification is independent
- **US3 (P2)**: Depends on US1 (needs clean tests before adding integration tests)
- **US4 (P2)**: Depends on US2 (needs running Docker stack)
- **US5 (P2)**: Depends on US1 (needs clean build to push)

**Note**: US2 and US1 can be worked on in parallel (separate concerns: code fixes vs Docker verification). US3 and US4 depend on US1 and US2 respectively.

### Parallel Opportunities

- T004, T005, T006, T007 (clippy fixes per crate) can run in parallel
- T013, T014, T015 (visibility integration tests) can run in parallel
- Phase 3 (US1) and Phase 4 (US2) can run in parallel (code fixes vs Docker verification)
- T010 (script creation) and T011 (run) are sequential within US2
- T017 (script creation) and T018 (run) are sequential within US4

---

## Parallel Example: Phase 3 (US1)

```bash
# Fix clippy in all crates in parallel:
Task: "Fix clippy warnings in ev-core"
Task: "Fix clippy warnings in ev-db"
Task: "Fix clippy warnings in driver-service"
Task: "Fix clippy warnings in admin-service"
```

## Parallel Example: Phase 5 (US3)

```bash
# Add visibility integration tests in parallel:
Task: "Integration test for is_active=false"
Task: "Integration test for is_verified=false"
Task: "Integration test for is_live=false"
```

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (baseline build + test)
3. Complete Phase 3: US1 (fix all warnings and test failures)
4. **STOP and VALIDATE**: `cargo test --all && cargo clippy --all-targets -- -D warnings`
5. MVP-2 hardening baseline complete

### Incremental Delivery

1. Setup + Foundational → Baseline
2. Add US1 → Clean codebase with no warnings or failures → **MVP!**
3. Add US2 → Docker Compose zero-state verified
4. Add US3 → Spatial index and visibility verified
5. Add US4 → Full product loop verified
6. Add US5 → CI green on branch
7. Polish → Final validation

### Parallel Team Strategy

With multiple developers:
1. Developer A: US1 (code fixes across all 4 crates)
2. Developer B: US2 (Docker Compose verification) — parallel with US1
3. Once US1 and US2 are done:
   - Developer A: US3 (spatial + visibility)
   - Developer B: US4 (full product loop)
4. Developer C: US5 (CI verification) — starts after first push
5. Polish: team validation together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- No separate test tasks generated (this sprint IS testing/verification — tasks ARE the tests)
- Integration tests in US3 should be gated behind `DATABASE_URL` environment variable (skip gracefully if unset)
- Commit after each phase or logical group
- Stop at any checkpoint to validate independently

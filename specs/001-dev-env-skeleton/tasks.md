---

description: "Task list for Phase 1: Dev Environment + CI/CD + Runnable Skeleton"
---

# Tasks: Dev Environment + CI/CD + Runnable Skeleton

**Input**: Design documents from `specs/001-dev-env-skeleton/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Not requested in Phase 1 spec — manual smoke tests via acceptance criteria suffice.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `services/core-service/src/`
- **Mobile**: `frontends/apps/mobile-driver/`
- **Shared**: `shared/bornemap-types/src/`
- **Infrastructure**: `infrastructure/`
- **CI**: `.github/workflows/`

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create monorepo root files: Cargo.toml (workspace), package.json, pnpm-workspace.yaml, turbo.json, .env.example, .gitignore at repo root
- [ ] T002 [P] Initialize Cargo workspace members (core-service, bornemap-types) in Cargo.toml
- [ ] T003 [P] Initialize pnpm workspace with mobile-driver package in pnpm-workspace.yaml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Create bornemap-types crate with StationId, UserId, PartnerId type aliases and generate_id() in shared/bornemap-types/src/lib.rs
- [ ] T005 [P] Configure .env.example with API_PORT, JWT_SECRET, LOG_LEVEL, LOG_FORMAT at repo root
- [ ] T006 [P] Create Dockerfile for core-service in infrastructure/docker/Dockerfile
- [ ] T007 [P] Create docker-compose.dev.yml for hot reload in infrastructure/docker-compose.dev.yml
- [ ] T008 [P] Create docker-compose.test.yml for CI deterministic execution in infrastructure/docker-compose.test.yml

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Developer environment bootstrap (Priority: P1) 🎯 MVP

**Goal**: Backend service running with health endpoints responding at `/api/v1/health/*`

**Independent Test**: Run `cargo run -p core-service`, then `curl http://localhost:8080/api/v1/health/live` returns `{"status":"alive","service":"core-service"}`

### Implementation for User Story 1

- [ ] T009 [P] [US1] Create core-service Cargo.toml with actix-web, tokio, serde, serde_json, env_logger dependencies in services/core-service/Cargo.toml
- [ ] T010 [P] [US1] Implement HealthResponse struct with serde Serialize in services/core-service/src/main.rs
- [ ] T011 [US1] Implement GET /api/v1/health/live endpoint returning HealthResponse in services/core-service/src/main.rs
- [ ] T012 [US1] Implement GET /api/v1/health/ready endpoint returning HealthResponse in services/core-service/src/main.rs
- [ ] T013 [US1] Configure structured JSON logging (env_logger) with timestamp, level, message, service fields in services/core-service/src/main.rs
- [ ] T014 [US1] Add prerequisite validation check at startup (detect missing runtimes) in services/core-service/src/main.rs
- [ ] T015 [US1] Add port conflict handling with clear error message in services/core-service/src/main.rs

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Mobile-backend connectivity (Priority: P2)

**Goal**: Expo Go mobile app displaying backend health status

**Independent Test**: Launch backend, launch `pnpm --filter mobile-driver start`, open Expo Go — app displays "Core Service: alive"

### Implementation for User Story 2

- [ ] T016 [P] [US2] Create mobile-driver Expo project with app.json and package.json in frontends/apps/mobile-driver/
- [ ] T017 [US2] Implement App.tsx with fetch to /api/v1/health/live and status display in frontends/apps/mobile-driver/App.tsx
- [ ] T018 [US2] Add "Connection Error" retry prompt for unreachable backend in frontends/apps/mobile-driver/App.tsx

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Automated quality gates (Priority: P3)

**Goal**: CI pipelines run linting, tests, and Docker build on every pull request

**Independent Test**: Open a PR — observe GitHub Actions run lint.yml, test.yml, build.yml successfully

### Implementation for User Story 3

- [ ] T019 [P] [US3] Create lint workflow with Rust clippy and frontend eslint in .github/workflows/lint.yml
- [ ] T020 [P] [US3] Create test workflow with cargo test in .github/workflows/test.yml
- [ ] T021 [US3] Create build workflow with Docker image build for core-service in .github/workflows/build.yml

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T022 [P] Update root .gitignore with Rust (target/), Node (node_modules/), .env entries
- [ ] T023 [P] Verify end-to-end workflow per quickstart.md: clone → cargo run → curl health → pnpm start → Expo display

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Depends on US1 backend running but independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - No dependencies on other stories (CI config is standalone)

### Within Each User Story

- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch T009 and T010 together (different files, no dependency):
Task: "Create core-service Cargo.toml with actix-web, tokio, serde, serde_json, env_logger"
Task: "Implement HealthResponse struct with serde Serialize"

# Then chain T011-T015 (same file, sequential):
Task: "Implement health/live endpoint in main.rs"
Task: "Implement health/ready endpoint in main.rs"
Task: "Configure structured JSON logging in main.rs"
```

## Parallel Example: User Story 2

```bash
# T016 (scaffold) is prerequisite for T017-T018:
Task: "Create mobile-driver Expo project with app.json and package.json"
# Then implement app logic:
Task: "Implement App.tsx with health fetch and status display"
Task: "Add Connection Error retry prompt in App.tsx"
```

## Parallel Example: User Story 3

```bash
# All CI workflow files are independent:
Task: "Create lint workflow in .github/workflows/lint.yml"
Task: "Create test workflow in .github/workflows/test.yml"
Task: "Create build workflow in .github/workflows/build.yml"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: `curl http://localhost:8080/api/v1/health/live` returns OK
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Backend health endpoints (MVP!)
3. Add User Story 2 → Mobile app with backend connectivity
4. Add User Story 3 → CI/CD quality gates
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (backend health endpoints)
   - Developer B: User Story 2 (mobile app)
   - Developer C: User Story 3 (CI/CD workflows)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

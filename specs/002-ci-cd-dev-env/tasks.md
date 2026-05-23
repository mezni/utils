---

description: "Task list for Phase 1 — CI/CD & Dev Environment"
---

# Tasks: Phase 1 — CI/CD & Dev Environment

**Input**: Design documents in `specs/002-ci-cd-dev-env/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`
**Tests**: Phase 1 ships CI/CD infrastructure and local dev environment — there are no unit/integration test tasks in this phase. Verification is performed via the `quickstart.md` runbook.
**Organization**: Tasks are grouped by user story (US1 P1, US2 P1, US3 P1, US4 P1, US5 P2, US6 P2) per `spec.md`.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: User-story tag (US1-US6); omitted in Setup, Foundational, Polish
- Every task names an exact file path or a specific GitHub configuration surface

## Path Conventions

- Repository root for top-level files (`Makefile`, `.env.example`, `docker-compose.yml`).
- `.github/workflows/` for GitHub Actions workflow files.
- `nginx/` for NGINX gateway configuration.
- No `src/` or `tests/` — service code lands in Phase 2+.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create directory structure required by all subsequent phases.

- [X] T001 Create `.github/workflows/` and `nginx/` directories (prerequisite for US1, US2, US4, US6)

**Checkpoint**: Both directories exist under the repository root.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Environment configuration and command runner that all user stories depend on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T002 Create `.env.example` with all environment variables from `contracts/environment-vars.md` (FR-009)
- [X] T003 Create `Makefile` with targets: `up`, `down`, `logs`, `test`, `lint`, `openapi` (FR-010)

**Checkpoint**: `.env.example` and `Makefile` exist and satisfy their contracts.

---

## Phase 3: User Story 1 — Every code change is validated by the CI pipeline (Priority: P1) 🎯 MVP

**Goal**: A developer pushes code or opens a PR; the CI pipeline automatically runs lint, unit tests, integration tests, OpenAPI validation, and docker-build — blocking merge until all pass (FR-001, FR-002, FR-003).

**Independent Test**: Push a commit with a lint error — CI fails and reports the violation. Fix and re-push — CI passes. The passing check appears as a required status on the PR.

### Implementation for User Story 1

- [X] T004 [US1] Create `.github/workflows/ci.yml` with six jobs (lint, unit, dco, integration, openapi-bundle, docker-build) per `contracts/ci-pipeline.md`; use GitHub-hosted runners; use service containers for integration dependencies (FR-001, FR-002, FR-003)
- [ ] T005 [US1] Update `main` branch protection ruleset to require Phase 1 CI status checks (lint, unit, dco, integration, openapi-bundle, docker-build) — replaces Phase 0 empty check list (FR-004)

**Checkpoint**: CI pipeline runs on push and PR; all six status checks appear on the PR; branch protection requires them.

---

## Phase 4: User Story 2 — A developer can run the full stack locally with one command (Priority: P1)

**Goal**: A developer with Docker installed clones the repo, runs a single command, and the full service stack starts locally with all containers healthy (FR-005, FR-006, FR-007, FR-008).

**Independent Test**: Clone the repo, run `make up`, wait for health checks, and successfully call `GET /health` on the gateway — all without manual configuration.

### Implementation for User Story 2

- [X] T006 [P] [US2] Create `docker-compose.yml` with all nine services (nginx, keycloak, postgres + postgis, mongodb, rabbitmq, auth-service, core-service, geo-service, analytics-service); configure healthchecks, volumes, networks; use official images for infrastructure services (FR-005, FR-006, FR-007)
- [X] T007 [P] [US2] Create `nginx/default.conf` with path-based routing per `contracts/nginx-routing.md` and `docs/operations/deployment.md`; include /health and /metrics routing to all services (FR-008, FR-013)

**Checkpoint**: `make up` starts all containers; gateway is reachable at `http://localhost`; each service path routes correctly.

---

## Phase 5: User Story 3 — Every service reports its health and operational metrics (Priority: P1)

**Goal**: The NGINX gateway routes /health and /metrics requests to each service; CI validates that health/metrics endpoints exist and respond (FR-011, FR-012, FR-013).

**Independent Test**: With the local stack running, call /health on each service path through the gateway — each returns 200. Call /metrics — returns Prometheus-formatted text.

### Implementation for User Story 3

- [X] T008 [US3] Add health-check validation step to the CI pipeline (`.github/workflows/ci.yml`) that verifies each running service returns 200 on /health and Prometheus-compatible output on /metrics post-deployment (FR-011, FR-012)
- [X] T009 [US3] Add NGINX location blocks for per-service /health and /metrics routing in `nginx/default.conf` (extends T007; FR-013)

**Checkpoint**: CI validates health/metrics; NGINX routes observability endpoints to each service.

---

## Phase 6: User Story 4 — DCO sign-off is enforced automatically on every PR (Priority: P1)

**Goal**: Every commit in a PR is checked for a valid `Signed-off-by:` trailer; unsigned commits block merge (FR-014, FR-015). Deferred from Phase 0 research R-001.

**Independent Test**: Push a commit without `Signed-off-by:` — DCO check fails. Amend with `-s` — DCO passes.

### Implementation for User Story 4

- [X] T010 [US4] Create `.github/workflows/dco.yml` that verifies every commit in a PR (excluding merge commits from target branch) carries a `Signed-off-by:` trailer matching the commit author; register as a required status check on `main` (FR-014, FR-015)

**Checkpoint**: DCO workflow runs on every PR; unsigned commits are blocked; signed commits pass.

---

## Phase 7: User Story 5 — OpenAPI specs are bundled and validated in CI (Priority: P2)

**Goal**: CI bundles OpenAPI specs and validates that they match the actual API endpoints, preventing drift (spec FR-005, SC-007).

**Independent Test**: Change a service route without updating its OpenAPI spec — the openapi-bundle CI job fails. Update the spec — the job passes.

### Implementation for User Story 5

- [X] T011 [US5] Add OpenAPI bundle job to `.github/workflows/ci.yml` (integrated with T004) that bundles per-service specs and fails if drift is detected between code and committed specs; mark services without REST APIs as N/A (FR-002 sub-bullet)

**Checkpoint**: OpenAPI bundle job runs in CI; catches deliberate drift in a test PR.

---

## Phase 8: User Story 6 — Stale branches are automatically flagged for review (Priority: P2)

**Goal**: A weekly scheduled workflow scans branches, identifies those idle >30 days with no open PR, and lists them in a tracking issue for Maintainer review (FR-016, FR-017). Deferred from Phase 0 research R-002.

**Independent Test**: Create an idle branch; the weekly workflow lists it in a `stale-branch` issue. Resolve the branch — it is removed from the issue on the next run.

### Implementation for User Story 6

- [X] T012 [US6] Create `.github/workflows/stale-branches.yml` with weekly cron schedule; use `actions/github-script` to query branches via GitHub API, filter by stale criteria (not main, no commits >30d, no open PR), and create/update a single tracking issue labeled `stale-branch` listing each match with last commit author and date (FR-016, FR-017)

**Checkpoint**: Workflow exists; manually triggered run creates/updates the `stale-branch` issue.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: End-to-end ratification of Phase 1 and final cross-checks that span all stories.

- [ ] T013 [P] Run `quickstart.md` Steps 0 through 8 in order on a fresh clone of the branch; record per-step pass/fail; do not declare ratification until every step passes (SC-001, SC-002, SC-005, SC-006, SC-007, SC-008)
- [ ] T014 [P] Manually re-check FR-012 (no contradiction with Constitution): verify each FR in `spec.md` against Constitution Principles I–VII; record the reviewer and date in the closing PR description
- [ ] T015 [P] Update `specs/002-ci-cd-dev-env/plan.md` "Progress Tracking" section: tick Setup, Foundational, and each user story phase after its tasks pass; tick Constitution Check: post-design (PASS)
- [ ] T016 [P] Update `docs/roadmap.md` Phase 1 row to status "Ratified" with the merge date, once the closing PR has been merged

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1, T001)**: No code dependency — creates directories for workflow files.
- **Foundational (Phase 2, T002-T003)**: Depends on Setup. BLOCKS all user stories in local dev context (Makefile and .env.example are prerequisites for running the stack).
- **User Stories (Phase 3-8)**: 
  - US1 (CI pipeline) can begin once Setup is done.
  - US2 (Local stack) can begin once Setup + Foundational are done.
  - US3 (Health/Metrics) depends on US2 (NGINX config from T007) and US1 (CI pipeline from T004).
  - US4 (DCO) depends only on Setup (needs `.github/workflows/` directory).
  - US5 (OpenAPI) is integrated into US1's CI pipeline — must run after or alongside T004.
  - US6 (Stale branches) depends only on Setup.
- **Polish (Phase 9)**: Depends on all user stories being complete.

### User Story Dependencies

- **US1 (P1)**: No dependency on US2-US6.
- **US2 (P1)**: No dependency on US1 or US3-US6. Depends on Foundational phase.
- **US3 (P1)**: Depends on US2's NGINX config (T007) and US1's CI pipeline (T004).
- **US4 (P1)**: No dependency on US1-US3 or US5-US6.
- **US5 (P2)**: Integrated into US1 — runs alongside T004.
- **US6 (P2)**: No dependency on US1-US5.

### Within Each User Story

- US1: T004 (create CI workflow) → T005 (update branch protection requires CI to exist).
- US2: T006 and T007 are independent file creations for different paths → can run in parallel.
- US3: T008 (CI health check step) → T009 (NGINX health/metrics routing, which extends T007).
- US4: T010 — single workflow file creation.
- US5: T011 — single CI job addition (integrated with T004).
- US6: T012 — single workflow file creation.

### Parallel Opportunities

- T006 and T007 are different files with no shared dependency → run in parallel.
- T013, T014, T015, T016 are different files/tasks in the polish phase → run in parallel.
- US1 (T004), US4 (T010), US6 (T012) all create different workflow files in `.github/workflows/` with no shared state → can run in parallel if multiple contributors exist.

---

## Parallel Example: User Stories Metadata

```text
# After Setup (T001):
# T004, T010, T012 are independent workflow file creations in .github/workflows/.
# T006 and T007 are independent (docker-compose.yml vs nginx/default.conf).

Task: "Create .github/workflows/ci.yml per contracts/ci-pipeline.md"        # T004
Task: "Create .github/workflows/dco.yml"                                    # T010
Task: "Create .github/workflows/stale-branches.yml"                         # T012
Task: "Create docker-compose.yml with all nine services"                    # T006
Task: "Create nginx/default.conf per contracts/nginx-routing.md"            # T007
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Complete T001 (directories).
2. Complete T002, T003 (env + Makefile).
3. Complete T004, T005 (CI pipeline + branch protection).
4. **STOP and VALIDATE**: Run `quickstart.md` Step 4 (CI pipeline triggers on push) on the branch.
5. If clean, open a `feat(phase-1): land US1 — CI pipeline` PR. This PR delivers the Phase 1 MVP: automated validation on every code change.

### Incremental Delivery

1. Land US1 (MVP) → CI validates every subsequent PR.
2. Land US2 + US3 → developers can run the full stack locally with health/metrics.
3. Land US4 → DCO enforcement is automated.
4. Land US5 → OpenAPI drift is caught in CI.
5. Land US6 → stale branches are managed automatically.
6. Land Polish phase → ratify Phase 1 (T013, T014); update plan + roadmap (T015, T016).

### Solo Strategy (realistic for this project)

The roadmap is a **solo build**. Run the phases sequentially in the priority order above (US1 → US2+US3 → US4 → US5 → US6 → Polish). Open one PR per user story group so the CI pipeline from US1 validates US2 onwards.

---

## Notes

- Phase 1 ships CI/CD infrastructure and local dev environment — no application code. The `quickstart.md` runbook is the verification surface.
- T005 is not a text-file edit — it is a **GitHub UI/API configuration action** (update the branch protection ruleset created in Phase 0 to require Phase 1 status checks).
- T008 and T009 depend on service stubs existing (services may be empty at Phase 1); health-check validation in CI should gracefully handle missing services or verify that the routing infrastructure is correct.
- Every file-creating task uses the `Write` tool with absolute paths; verify the file exists before marking the task complete.
- Commit after each logical group (one commit per user story is acceptable; one commit per task is also acceptable). All commits MUST be signed off (`git commit -s`) per FR-016.

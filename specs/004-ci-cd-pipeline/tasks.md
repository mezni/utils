# Tasks: CI/CD Pipeline & Delivery Automation

**Input**: Design documents from `/specs/004-ci-cd-pipeline/`
**Branch**: `004-ci-cd-pipeline`
**Date**: 2026-05-31

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Phase 1: Setup — Scripts Directory

**Purpose**: Create supporting CI scripts directory structure.

- [ ] T001 Create `scripts/ci/` directory for deployment and health check scripts
- [ ] T002 [P] Ensure `.github/workflows/` directory exists

**Checkpoint**: Scripts directory ready for supporting automation.

---

## Phase 2: User Story 1 — Pull Request Validation (Priority: P1) 🎯 MVP

**Goal**: Create `pr-validation.yml` that runs on every PR, executing formatting checks, static analysis, unit tests, build validation, contract validation, and monorepo-aware change detection. Cancels in-progress runs on new pushes.

**Independent Test**: Push a branch with a formatting violation → PR blocked with clear error within 15 minutes.

- [ ] T003 [P] [US1] Create `.github/workflows/pr-validation.yml` with `on: [pull_request, workflow_dispatch]` trigger, concurrency group `pr-${{ github.head_ref }}` with `cancel-in-progress: true`, env vars `CARGO_TERM_COLOR` and `RUSTFLAGS`; include lockfile drift check step (`cargo metadata` verifying Cargo.lock matches Cargo.toml) per FR-003
- [ ] T004 [P] [US1] Add `paths-filter` job using `dorny/paths-filter` to detect changed workspaces (backend: `services/**`, `crates/**`; frontend: `apps/**`, `packages/**`)
- [ ] T005 [P] [US1] Add `lint` job with `rust-toolchain` setup, `cargo fmt --all --check`, `cargo clippy --workspace -- -D warnings`, Node.js setup, `npm ci`, and `eslint` (conditional on frontend changes)
- [ ] T006 [P] [US1] Add `test` job (needs: lint) with `cargo test --workspace` — only if backend affected
- [ ] T007 [P] [US1] Add `build` job (needs: lint) with `cargo build --workspace` and `npm run build` — conditional on affected workspaces
- [ ] T008 [P] [US1] Add `contract-validation` job (needs: lint) with DTO audit — verify no struct DTO/enum outside `crates/contracts/`
- [ ] T009 [US1] Add branch protection status checks to lint, test, build, contract-validation jobs — return appropriate conclusion codes for GitHub branch protection
- [ ] T010 [US1] Verify workflow triggers on PR creation and cancels on new push using a test branch

**Checkpoint**: PR validation blocks invalid merges with clear errors under 15 minutes.

---

## Phase 3: User Story 2 — Container Build & Publish (Priority: P1)

**Goal**: Create `build-images.yml` that builds and publishes all 8 container images to GHCR on every main merge and version tag push. Uses matrix strategy, GHCR cache, and proper tagging.

**Independent Test**: Merge a change to main → images tagged with commit SHA appear in GHCR within 20 minutes.

- [ ] T011 [P] [US2] Create `.github/workflows/build-images.yml` with `on: push: branches: [main]` + `tags: ['v*']` trigger, concurrency `build-${{ github.ref }}` with `cancel-in-progress: true`
- [ ] T012 [P] [US2] Add `login` job with `docker/login-action@v3` to authenticate with GHCR using `secrets.GITHUB_TOKEN`
- [ ] T013 [P] [US2] Add `build` job with `strategy.matrix.service` for all 8 services: `[admin-service, driver-service, clickstream-service, gis-sync-worker, driver-web, admin-dashboard, partner-dashboard]` using `docker/build-push-action@v6`
- [ ] T014 [US2] Configure image tagging in build job: `sha-${{ github.sha }}` for all builds; `${{ github.ref_name }}` for version tag builds; `latest` for main branch builds
- [ ] T015 [US2] Add GHCR cache (`type=gha`) to build step for `mode=max` layer caching
- [ ] T016 [US2] Add failure handling — no partial publish: wrap build in matrix with `fail-fast: false` to allow other services to complete even if one fails
- [ ] T017 [US2] Verify images published to GHCR after main merge with correct tags

**Checkpoint**: Container images publish automatically on main merge with correct tagging.

---

## Phase 4: User Story 3 — Integration Validation (Priority: P2)

**Goal**: Create `integration.yml` that boots the full platform stack via Docker Compose, verifies all health endpoints, validates `/api/v1/*` routing, and runs cross-service communication tests.

**Independent Test**: After a main merge, the full stack boots and all health endpoints return 200 within 25 minutes.

- [ ] T018 [P] [US3] Create `.github/workflows/integration.yml` with `on: push: branches: [main]` + `schedule: cron: '0 6 * * *'` trigger, concurrency `integration-main` with `cancel-in-progress: true`
- [ ] T019 [P] [US3] Add `runtime-boot` job: checkout repo, create `.env` with dev defaults, run `docker compose -f infra/compose/docker-compose.yml -f infra/compose/docker-compose.dev.yml up -d`
- [ ] T020 [P] [US3] Add `health-verify` job (needs: runtime-boot): `curl` all 11 service health endpoints and verify HTTP 200, retry up to 30 times with 10s interval
- [ ] T021 [P] [US3] Add `api-contract-check` job (needs: health-verify): curl all `/api/v1/*` routes and reject non-compliant paths; verify unversioned paths return 404
- [ ] T022 [US3] Add `cross-service-test` job (needs: health-verify): validate DB connectivity (`pg_isready`), RabbitMQ publish/consume, and Keycloak auth token flow
- [ ] T023 [US3] Add 1 retry for flaky test jobs (`health-verify` job with `retry: 1`)
- [ ] T024 [US3] Verify integration workflow passes on main merge — full stack boot + health + contract check

**Checkpoint**: Full-stack integration validated automatically on every main merge.

---

## Phase 5: User Story 4 — Deployment Automation (Priority: P2)

**Goal**: Create `deploy.yml` and supporting scripts (`deploy.sh`, `health-check.sh`) for one-click production deployment with rolling restart, post-deploy validation, and rollback on failure.

**Independent Test**: Trigger deployment from GitHub UI → production services restart in order with post-deploy checks passing.

- [ ] T025 [P] [US4] Create `scripts/ci/deploy.sh` — deployment orchestration: pull latest images, validate `.env` completeness, verify compose integrity, execute rolling restart per group (infra → auth → backend → frontend)
- [ ] T026 [P] [US4] Create `scripts/ci/health-check.sh` — post-deploy health verification: curl health endpoints per service group, exit non-zero on failure, output diagnostic summary
- [ ] T027 [P] [US4] Create `.github/workflows/deploy.yml` with `on: workflow_dispatch` (manual dispatch), inputs for `environment` (production) and `image_tag` (release tag), concurrency `deploy-production` with `cancel-in-progress: false` for sequential queuing
- [ ] T028 [P] [US4] Add `pull-and-validate` job: SSH into production host, pull latest images for tag, validate `.env` completeness, verify `docker compose config --quiet` passes
- [ ] T029 [P] [US4] Add `rolling-restart` job (needs: pull-and-validate): execute `scripts/ci/deploy.sh` via SSH with image tag argument, health check between each service group
- [ ] T030 [US4] Add `post-deploy-verify` job (needs: rolling-restart): execute `scripts/ci/health-check.sh` via SSH, report pass/fail
- [ ] T031 [US4] Add rollback logic: if `post-deploy-verify` fails, restore previous compose file and restart with previous images
- [ ] T032 [US4] Verify deployment via `workflow_dispatch` — observe services restart in order with health checks

**Checkpoint**: Production deployment is one-click with verified health checks and rollback.

---

## Phase 6: User Story 5 — Security & Dependency Audit (Priority: P3)

**Goal**: Create `security-audit.yml` that runs weekly on schedule, scanning Rust dependencies (`cargo audit`), frontend dependencies (`npm audit`), and container images (`trivy`). Reports findings via email alerts.

**Independent Test**: View latest weekly audit run in Actions → vulnerabilities listed with severity, package, and remediation.

- [ ] T033 [P] [US5] Create `.github/workflows/security-audit.yml` with `on: schedule: cron: '0 6 * * 0'` (weekly Sunday) trigger, concurrency `audit-weekly` with `cancel-in-progress: true`
- [ ] T034 [P] [US5] Add `cargo-audit` job: install `cargo-audit`, run `cargo audit`, output to job summary with severity filter for actionable CVEs
- [ ] T035 [P] [US5] Add `npm-audit` job: `npm audit --audit-level=high`, output critical and high findings to job summary
- [ ] T036 [P] [US5] Add `trivy-scan` job: install `aquasecurity/trivy-action`, scan all Dockerfiles in `infra/docker/` and `apps/*/Dockerfile` for base image CVEs
- [ ] T037 [US5] Configure email alert on critical findings via workflow `on: workflow_run` failure notification or `actions/github-script` inline notification
- [ ] T038 [US5] Verify weekly schedule triggers audit — check Actions tab for scheduled run

**Checkpoint**: Weekly security audit scans all dependencies and reports vulnerabilities.

---

## Phase 7: User Story 6 — Release Creation (Priority: P3)

**Goal**: Create `release.yml` that generates GitHub Releases with auto-generated notes, artifact manifest, and image manifest on version tag push.

**Independent Test**: Push `v1.0.0` tag → GitHub Release created with notes, artifact list, and image manifest within 5 minutes.

- [ ] T039 [P] [US6] Create `scripts/ci/manifest.sh` — artifact manifest generator: compute SHA256 hashes of all build artifacts, output JSON manifest with registry paths and tags
- [ ] T040 [P] [US6] Create `.github/workflows/release.yml` with `on: push: tags: ['v*']` trigger, concurrency `release-${{ github.ref }}` with `cancel-in-progress: true`
- [ ] T041 [P] [US6] Add `generate-notes` job: use `softprops/action-gh-release` or `ncipollo/release-action` to create Release with auto-generated release notes from conventional commits
- [ ] T042 [P] [US6] Add `build-manifest` job (needs: generate-notes): call `scripts/ci/manifest.sh` to generate artifact manifest JSON, upload as release asset
- [ ] T043 [US6] Add `image-manifest` job (needs: build-manifest): query GHCR API for all service images at current tag, generate image manifest JSON, upload as release asset
- [ ] T044 [US6] Verify release created with notes, artifact manifest, and image manifest on version tag push

**Checkpoint**: Releases are fully documented with manifests and versioned images.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, branch protection, secrets setup, and verification against all success criteria.

- [ ] T045 Verify all 6 success criteria (SC-001 through SC-006) pass end-to-end
- [ ] T046 Update AGENTS.md to reference EPIC 3 plan
- [ ] T047 Remove or archive old `.github/workflows/ci.yml` from EPIC 2 — replace with new workflow files
- [ ] T048 Add `README.md` section documenting CI/CD pipeline workflow triggers and conventions
- [ ] T049 Run full pipeline test: push → PR validation → merge → integration → build → deploy → release
- [ ] T050 [P] Configure GitHub repository secrets (GHCR_TOKEN, DEPLOY_HOST, DEPLOY_USER, DEPLOY_KEY) in Actions settings — required by FR-015
- [ ] T051 Add workflow duration tracking steps to pr-validation.yml, integration.yml, build-images.yml — emit elapsed wall-clock time as job summary output; verify against FR-017 targets (15/25/20 min)
- [ ] T052 Set `retention-days` on all workflow artifact upload steps: 7 days for PR runs, 30 days for main runs, indefinite for release artifacts — per FR-018

**Checkpoint**: EPIC 3 fully complete — all 6 workflows operational.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately. **MVP scope.**
- **Phase 2 (US1)**: Depends on Phase 1 — workflow file needs .github/workflows/ dir
- **Phase 3 (US2)**: Depends on Phase 1 — independent of US1 (different trigger, different file)
- **Phase 4 (US3)**: Depends on Phase 1 — independent of US1/US2 (integration workflow is standalone)
- **Phase 5 (US4)**: Depends on Phase 3 (US2) — deployment needs published images from build-images.yml
- **Phase 6 (US5)**: Depends on Phase 1 — independent of all other workflows
- **Phase 7 (US6)**: Depends on Phase 3 (US2) — release manifest references GHCR images
- **Phase 8 (Polish)**: Depends on all phases

### Within Each Phase

- [P] tasks within a phase can run in parallel (different files, no dependencies)
- Sequence: workflow file creation → job definitions → verification

### Parallel Opportunities

| Phase | [P] tasks | Can run together |
|-------|-----------|-----------------|
| Setup | T001, T002 | Directory creation |
| US1 | T003–T008 | All job definitions in pr-validation.yml |
| US2 | T011–T013, T015 | Workflow scaffold, login, build matrix, cache config |
| US3 | T018–T021 | Workflow scaffold, boot, health, contract check jobs |
| US4 | T025–T029 | Scripts (deploy.sh, health-check.sh) + deploy.yml jobs |
| US5 | T033–T036 | Workflow scaffold, cargo-audit, npm-audit, trivy jobs |
| US6 | T039–T042 | manifest.sh + release.yml jobs |

---

## Implementation Strategy

### MVP First (Phase 1 + Phase 2)

1. Complete Phase 1: Setup — scripts/ci/ directory
2. Complete Phase 2: US1 — `pr-validation.yml`
3. **MVP delivered**: PR validation blocks invalid merges with lint, test, build, contract checks

### Incremental Delivery

1. Setup → Scripts directory ready
2. US1 → PR validation (first working increment)
3. US2 → Container build & publish
4. US3 → Integration validation
5. US4 → Deployment automation
6. US5 → Security audit
7. US6 → Release creation
8. Polish → Final end-to-end validation

---

## Notes

- [P] tasks = different files, no dependencies
- [USx] label maps task to specific user story for traceability
- No test tasks generated — spec does not request TDD approach
- 7 images total: 4 backend (admin, driver, clickstream, gis-sync) + 3 frontend (driver-web, admin-dashboard, partner-dashboard)
- The existing `.github/workflows/ci.yml` from EPIC 2 must be removed or deprecated in Phase 8 (T047)
- Branch protection must be configured in GitHub repository settings as a post-implementation step

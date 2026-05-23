# Feature Specification: CI/CD & Dev Environment

**Feature Branch**: `002-ci-cd-dev-env`

**Created**: 2026-05-22

**Status**: Draft

**Input**: User description: "phase1_devenv - read phase 1 from docs/roadmap.md, use best practices for github speckit"

**Roadmap reference**: [docs/roadmap.md](../../docs/roadmap.md) §"Phase 1 — CI/CD & Dev Environment"

## Clarifications

### Session 2026-05-22

- Q: Should CI use GitHub-hosted runners or self-hosted runners? → A: GitHub-hosted runners
- Q: Where should built Docker images be pushed? → A: GitHub Container Registry (GHCR)
- Q: How should integration tests run in CI — full docker-compose stack, service containers, or mocked dependencies? → A: GitHub Actions service containers for dependency services (PostgreSQL, MongoDB, RabbitMQ); the service-under-test runs as a regular process.
- Q: When are service Dockerfiles created? → A: Phase 1 creates minimal Dockerfiles for infrastructure services (nginx, keycloak, postgres, mongo, rabbitmq use official images). Service-specific Dockerfiles are created in their respective phases (Phase 2+). CI docker-build builds whatever Dockerfiles exist.
- Q: How is the CI environment configured for integration tests? → A: CI copies `.env.example` to `.env` as the base, then overrides sensitive values from GitHub Actions secrets. Non-sensitive defaults in `.env.example` are the single source of truth.

**Phase 0 deferred scope**: The following items were deferred from Phase 0 and are in scope for this feature:
- DCO automated enforcement as a required status check (research R-001, spec FR-016)
- Stale branch flagging via scheduled workflow (research R-002, spec FR-015)
- Populating the `main` branch required status checks list with Phase 1 CI jobs (branch-protection contract C-BR-3)

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Every code change is validated by the CI pipeline (Priority: P1)

A developer pushes a commit or opens a pull request. The CI pipeline automatically runs linting, unit tests, integration tests, and an OpenAPI validation. If any step fails, the pipeline reports the failure and blocks the merge. A passing pipeline is recorded as a required status check on the PR.

**Why this priority**: Without CI, every merge risks breaking the build or shipping untested code. All downstream phases depend on a green pipeline to enforce the Definition of Done from Principle VII.

**Independent Test**: Push a commit that introduces a lint error. The pipeline MUST fail and report the specific lint violation. Then fix the error and re-push — the pipeline MUST pass. The passing check MUST appear as a required status on the PR.

**Acceptance Scenarios**:

1. **Given** a developer pushes a commit to a feature branch with an open PR, **When** the CI pipeline runs, **Then** it executes lint, unit tests, integration tests, and OpenAPI validation in sequence.
2. **Given** a CI job fails, **When** the developer views the PR status, **Then** the failing check is clearly reported with the failure reason.
3. **Given** all CI jobs pass, **When** the developer views the PR, **Then** the required status checks are green and the PR is eligible for merge.

---

### User Story 2 — A developer can run the full stack locally with one command (Priority: P1)

A developer clones the repository, runs a single command, and the entire service stack (PostgreSQL, MongoDB, RabbitMQ, Keycloak, NGINX, and all four services) starts locally. The developer can interact with the API through the gateway, inspect the databases, and observe logs from all containers.

**Why this priority**: Local development and integration testing are impossible without a reproducible local environment. Every developer from Phase 2 onward needs the full stack to write and test code.

**Independent Test**: A developer with only Docker installed clones the repo, runs `make up`, waits for health checks to pass, and successfully calls `GET /health` on the gateway — all without any manual configuration.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** the developer runs the local environment command, **Then** all containers start, health checks pass within 2 minutes, and the gateway is reachable at `http://localhost`.
2. **Given** the local stack is running, **When** the developer runs the shutdown command, **Then** all containers stop gracefully and resources are cleaned up.

---

### User Story 3 — Every service reports its health and operational metrics (Priority: P1)

Operations and developers can query any service for its liveness/readiness state and Prometheus-compatible metrics. The gateway routes health check requests to the correct services.

**Why this priority**: Principle VI (Observability) mandates health and metrics endpoints for every service. These endpoints must exist before services are deployed so monitoring can start from day one.

**Independent Test**: With the local stack running, call `/health` on each service endpoint through the gateway — each returns `200 OK`. Call `/metrics` on each service — each returns Prometheus-formatted text.

**Acceptance Scenarios**:

1. **Given** all services are running, **When** a health check request is sent to the gateway, **Then** the gateway routes to each service's `/health` endpoint and all return `200 OK` with liveness and readiness status.
2. **Given** all services are running, **When** a metrics request is sent to each service's `/metrics` endpoint, **Then** each returns Prometheus-compatible metrics including request count, error rate, and response duration.

---

### User Story 4 — DCO sign-off is enforced automatically on every PR (Priority: P1)

Every commit in a pull request is automatically checked for a valid `Signed-off-by:` trailer. Pull requests with any commit missing the trailer are blocked from merge. This was mandated in Phase 0 (FR-016) and deferred to Phase 1 for automated enforcement.

**Independent Test**: Push a commit without `Signed-off-by:` to a PR — the DCO check fails and the PR is blocked. Add `Signed-off-by:` via `git commit --amend -s` — the check passes.

**Acceptance Scenarios**:

1. **Given** a pull request where all commits have valid `Signed-off-by:` trailers, **When** the DCO check runs, **Then** it passes and the PR is unblocked.
2. **Given** a pull request where at least one commit lacks a valid `Signed-off-by:` trailer, **When** the DCO check runs, **Then** it fails and the PR is blocked from merge.

---

### User Story 5 — OpenAPI specs are bundled and validated in CI (Priority: P2)

Every pull request that changes service APIs must update the corresponding OpenAPI specification. CI bundles the OpenAPI specs and validates that they match the actual API surface, preventing drift between documentation and implementation.

**Why this priority**: Principle VII DoD item 2 requires OpenAPI specs to be updated for any REST surface change. CI enforcement before merge prevents silent drift.

**Independent Test**: Change a route in a service without updating its OpenAPI spec — the OpenAPI validation job fails. Then update the spec — the job passes.

**Acceptance Scenarios**:

1. **Given** a pull request modifies service code, **When** CI runs the OpenAPI bundle job, **Then** it validates that the bundled OpenAPI spec matches the actual API endpoints.

---

### User Story 6 — Stale branches are automatically flagged for review (Priority: P2)

A scheduled check runs weekly and identifies branches that have been idle for more than 30 days with no open PR. These branches are listed in a tracking issue for Maintainer review. This was mandated in Phase 0 (FR-015 second half) and deferred to Phase 1 for automation.

**Why this priority**: Without automation, stale branches accumulate silently. A weekly issue keeps the problem visible without destroying in-progress work.

**Independent Test**: Simulate an idle branch by creating a branch, making no commits for 30+ days (or by inspecting the workflow's detection logic with a test branch). The weekly workflow lists it in the tracking issue. Maintainers can review and decide to delete or keep each branch.

**Acceptance Scenarios**:

1. **Given** a branch that satisfies all criteria (not `main`, no commits in 30+ days, no open PR), **When** the weekly workflow runs, **Then** the branch is listed in the stale-branch tracking issue.
2. **Given** a tracking issue already exists, **When** the weekly workflow runs again, **Then** the issue body is updated with the current list (branches that are no longer stale are removed; new stale branches are added).

---

### Edge Cases

- What happens if a CI job is temporarily unavailable (e.g., runner outage)? The pipeline SHOULD retry automatically. If the outage persists, the developer can re-trigger the pipeline manually.
- What happens if the local environment uses ports already occupied on the developer's machine? The docker-compose configuration SHOULD document the port mapping and allow override via environment variables.
- What happens if the DCO check encounters a merge commit? Merge commits from the target branch SHOULD be excluded from the DCO check (they carry committer identity, not the PR author's).
- What happens when no branches are stale? The scheduled workflow SHOULD update the issue stating "No stale branches found" rather than leaving an outdated list.
- What happens if the OpenAPI spec is missing for a service that has no REST API? The CI pipeline MUST allow marking OpenAPI validation as N/A per service (e.g., services that are not REST-based).

## Requirements *(mandatory)*

### Functional Requirements

#### CI Pipeline

- **FR-001**: The CI pipeline MUST run automatically on every push to any branch and on every pull request.
- **FR-002**: The CI pipeline MUST include the following jobs that MUST pass before a PR can merge:
  - **Lint**: Enforce code style and static analysis rules for all languages used in changed services.
  - **Unit tests**: Run all unit tests for changed services.
  - **Integration tests**: Run integration tests against real database, queue, and HTTP dependencies. Locally via docker-compose; in CI via GitHub Actions service containers.
  - **OpenAPI bundle**: Bundle and validate OpenAPI specifications; fail if drift exists between committed specs and actual code.
  - **Docker build**: Build all service images; push to registry only on merge to `main`.
- **FR-003**: The CI pipeline MUST produce a status check for each required job that GitHub branch protection can reference.
- **FR-004**: The `main` branch required status checks MUST be populated with the Phase 1 CI jobs (lint, unit, integration, openapi-bundle, docker-build), replacing the Phase 0 empty list.

#### Local Development Environment

- **FR-005**: A single command MUST start the entire service stack locally using container orchestration.
- **FR-006**: A single command MUST stop and clean up the local stack.
- **FR-007**: The local environment MUST include containers for: NGINX gateway, Keycloak, PostgreSQL (with PostGIS), MongoDB, RabbitMQ, auth-service, core-service, geo-service, analytics-service.
- **FR-008**: The NGINX gateway MUST route requests to the correct service based on path prefix.
- **FR-009**: Environment configuration (database URLs, queue URLs, secrets, service ports) MUST be provided via a template file (`.env.example`) that developers copy to `.env`; no secrets or credentials MAY be committed to the repository. CI MUST copy `.env.example` to `.env` as the base configuration and override sensitive values via GitHub Actions secrets.
- **FR-010**: A Makefile (or equivalent command runner) MUST provide convenience targets for: starting the stack (`up`), stopping (`down`), viewing logs (`logs`), running tests (`test`), linting (`lint`), and bundling OpenAPI (`openapi`).

#### Observability

- **FR-011**: Every service MUST expose a `/health` endpoint returning liveness and readiness status.
- **FR-012**: Every service MUST expose a `/metrics` endpoint returning Prometheus-compatible metrics.
- **FR-013**: The NGINX gateway MUST route `/health` and `/metrics` requests to the appropriate backend service.

#### DCO Enforcement

- **FR-014**: A CI job MUST verify that every commit in a pull request (excluding merge commits from the target branch) carries a `Signed-off-by:` trailer matching the commit author. This check MUST be a required status check on `main`.
- **FR-015**: The DCO check MUST block merge if any commit in the PR lacks a valid `Signed-off-by:` trailer.

#### Stale Branch Management

- **FR-016**: A scheduled workflow MUST run weekly on a cron trigger and identify branches (excluding `main`) that satisfy ALL of: no commits in the last 30 days, no open pull request.
- **FR-017**: The stale-branch workflow MUST create or update a single tracking issue labeled `stale-branch` that lists each matching branch, its last commit author, and its last commit date.

### Key Entities

- **CI Pipeline**: The automated workflow that validates every code change through linting, testing, and validation gates.
- **Status Check**: A named CI result that GitHub branch protection can require before merge.
- **Local Stack**: The complete set of containers (services, databases, queue, gateway, identity provider) that a developer runs locally.
- **Health Endpoint**: A service endpoint that reports liveness (is the process alive?) and readiness (is the service able to handle requests?).
- **Metrics Endpoint**: A service endpoint that exposes operational metrics (request count, error rate, duration) in Prometheus text format.
- **OpenAPI Spec**: The documented API surface that MUST stay in sync with implementation.
- **DCO Check**: An automated verification that every commit carries the Developer Certificate of Origin sign-off.
- **Stale-Branch Tracker**: A rolling issue that lists candidate branches for Maintainer review.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: CI pipeline completes from push to final status check in under 10 minutes for a typical change.
- **SC-002**: A developer with Docker installed can start the full stack and reach the gateway in under 2 minutes using only one command and no manual configuration.
- **SC-003**: 100% of services expose a `/health` endpoint that returns 200 OK when the service is running and ready.
- **SC-004**: 100% of services expose a `/metrics` endpoint with Prometheus-compatible output.
- **SC-005**: 100% of pull requests after Phase 1 ships have the DCO status check passing before merge (measured over the first 20 PRs after Phase 1).
- **SC-006**: The stale-branch tracking issue is automatically updated at least once every 8 days.
- **SC-007**: OpenAPI bundle CI job catches at least 90% of API-surface changes that were not accompanied by an OpenAPI spec update (measured by introducing deliberate drift in a test PR and confirming the job fails).
- **SC-008**: 100% of required status checks from Phase 1 CI are populated in the `main` branch protection ruleset by Phase 1 ratification.

## Assumptions

- The project uses GitHub; CI refers to GitHub Actions, and health endpoints are HTTP-based as standard for web services.
- Docker and Docker Compose (or an equivalent container runtime) are available on every developer machine; no local service installation is needed beyond Docker.
- Services are scoped following the Constitution's service boundaries; the CI pipeline covers auth-service, core-service, geo-service, analytics-service, and the frontend.
- The NGINX gateway configuration from `docs/operations/deployment.md` is the source of truth for routing rules; this Phase implements that configuration in the local environment.
- Developer machines are Unix-like (Linux or macOS); Makefile conventions follow Unix standards.
- The DCO check implementation follows research R-001 recommendation (GitHub Action workflow, not the DCO GitHub App).
- The stale-branch workflow follows research R-002 recommendation (weekly cron, tracking issue, GitHub API).
- Phase 0 artefacts (README, CONTRIBUTING, CODEOWNERS, PR template, branch protection) are in place and ratified before Phase 1 CI is configured.
- CI pipelines run on GitHub-hosted runners (Ubuntu latest) for all jobs, including docker-build. Self-hosted runners are not required at Phase 1.
- Docker images are pushed to GitHub Container Registry (GHCR) on merge to `main`. No separate Docker Hub or third-party registry account is needed.
- Infrastructure services (nginx, keycloak, postgres, mongodb, rabbitmq) use official Docker images. Service-specific Dockerfiles (auth, core, geo, analytics) are created in each service's respective phase; Phase 1 includes a docker-build CI job that builds whatever Dockerfiles exist at the time.

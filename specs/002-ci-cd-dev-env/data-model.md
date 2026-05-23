# Data Model: Phase 1 — CI/CD & Dev Environment

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-22

## Note on "data model" for a CI/CD infrastructure feature

This feature ships no database schemas or runtime entities. Its "entities" are **CI/CD infrastructure components and development environment configuration artifacts**. They have shape, identity, relationships, and lifecycle just like data entities.

## Entities

### 1. CIPipeline (GitHub Actions Workflow)

- **Identity**: file at `.github/workflows/ci.yml`.
- **Attributes**:
  - `trigger_on_push` — boolean; MUST run on push to any branch (FR-001).
  - `trigger_on_pull_request` — boolean; MUST run on PR (FR-001).
  - `jobs` — ordered set of job definitions: lint, unit, integration, openapi-bundle, docker-build, dco.
  - `status_checks` — set of status check names that branch protection references (FR-003).
  - `service_containers` — dependency containers (postgres, mongo, rabbitmq) for integration tests.
- **Relationships**:
  - Updates `main` branch protection ruleset status checks (Entity 5 in Phase 0 data model).
  - Consumes `EnvironmentConfiguration` to set test env vars.
  - Produces Docker images published to `DockerRegistry`.
- **Validation rules**:
  - Pipeline MUST complete in under 10 minutes for a typical change (SC-001).
  - Every job MUST produce a status check name (FR-003).
  - Lint and unit MUST run before integration and OpenAPI (research R-006).
- **Lifecycle**: Created in Phase 1. Extended in each later phase that adds a new CI job.

### 2. DCOWorkflow (`.github/workflows/dco.yml`)

- **Identity**: file at `.github/workflows/dco.yml`.
- **Attributes**:
  - `trigger` — MUST run on `pull_request` events (FR-014).
  - `skip_merge_commits` — boolean; merge commits from target branch excluded.
  - `required_trailer` — string: `Signed-off-by:`.
- **Relationships**:
  - Referenced by `main` branch protection ruleset as a required status check.
  - Enforces FR-016 from Phase 0 spec.
- **Validation rules**:
  - MUST fail the status check if ANY commit in the PR (excluding merge commits) lacks `Signed-off-by:` matching the commit author (FR-014, FR-015).
  - MUST pass the status check only when ALL commits in the PR carry the trailer.
- **Lifecycle**: Created in Phase 1. Stable unless DCO policy changes.

### 3. StaleBranchWorkflow (`.github/workflows/stale-branches.yml`)

- **Identity**: file at `.github/workflows/stale-branches.yml`.
- **Attributes**:
  - `schedule` — cron expression for weekly execution (FR-016).
  - `stale_days` — integer: 30 days without commits (FR-016).
  - `exclude_branches` — list: MUST exclude `main`.
  - `issue_labels` — list: MUST include `stale-branch` (FR-017).
  - `issue_title` — string: tracking issue title.
- **Relationships**:
  - Creates/updates a GitHub issue (the `StaleBranchTracker`).
  - Reads branch list via GitHub REST API.
- **Validation rules**:
  - MUST identify branches with ALL of: not `main`, no commits in 30+ days, no open PR (FR-016).
  - MUST create or update a single tracking issue with label `stale-branch` (FR-017).
  - Issue MUST list each matching branch with last commit author and date (FR-017).
  - Issue MUST be updated at least once every 8 days (SC-006).
- **Lifecycle**: Created in Phase 1.

### 4. StaleBranchTracker (GitHub Issue)

- **Identity**: a single GitHub issue labeled `stale-branch`.
- **Attributes**:
  - `branch_list` — list of `(branch_name, last_commit_author, last_commit_date)` tuples.
  - `last_updated` — timestamp of last workflow run.
  - `state` — open while any stale branches exist; body updated on each run.
- **Relationships**:
  - Read by Maintainers during weekly review.
  - Updated by `StaleBranchWorkflow`.
- **Validation rules**:
  - Issue body MUST be overwritten (not appended) each run (research R-007).
  - If no stale branches, body MUST state "No stale branches found."
- **Lifecycle**: Created on first workflow run in Phase 1. Persists as long as the project uses branch-based development.

### 5. LocalStack (Docker Compose)

- **Identity**: file at `docker-compose.yml` and optional `docker-compose.override.yml`.
- **Attributes**:
  - `services` — list of container definitions: nginx, keycloak, postgres, mongodb, rabbitmq, auth-service, core-service, geo-service, analytics-service (FR-007).
  - `volumes` — persistent storage for database containers.
  - `networks` — internal network for inter-service communication.
  - `healthchecks` — per-service health check definitions.
- **Relationships**:
  - Uses `EnvironmentConfiguration` for variable substitution.
  - Referenced by `Makefile` targets (`make up`, `make down`).
  - Service definitions for auth/core/geo/analytics are stubs until their respective phases.
- **Validation rules**:
  - MUST start all services with a single command (FR-005).
  - MUST pass health checks within 2 minutes (SC-002).
  - MUST stop all containers gracefully with a single command (FR-006).
  - Gateway MUST be reachable at `http://localhost` after startup.
- **Lifecycle**: Created in Phase 1. Updated whenever a new service or infrastructure component is added.

### 6. NginxGateway (NGINX Configuration)

- **Identity**: file at `nginx/default.conf`.
- **Attributes**:
  - `routes` — path-to-service mapping table (research R-008).
  - `upstreams` — backend service addresses.
  - `health_check_routing` — special handling for `/health` and `/metrics`.
- **Relationships**:
  - Routes traffic to all backend services.
  - Maps 1:1 with the deployment topology in `docs/operations/deployment.md`.
- **Validation rules**:
  - `/auth/` MUST route to auth-service (research R-008).
  - `/api/core/` MUST route to core-service.
  - `/api/geo/` MUST route to geo-service.
  - `/api/analytics/` MUST route to analytics-service.
  - `/health` and `/metrics` MUST route to each service's respective endpoints (FR-013).
- **Lifecycle**: Created in Phase 1. Extended as new services are added.

### 7. EnvironmentConfiguration (`.env.example`)

- **Identity**: file at `.env.example`.
- **Attributes**:
  - `variables` — set of `KEY=VALUE` pairs with safe defaults for all configurable parameters.
  - `required_secrets` — list of variables that MUST be overridden via GitHub secrets.
  - `documentation` — inline comments describing each variable.
- **Relationships**:
  - Copied to `.env` by developers for local use.
  - Copied to `.env` by CI and overridden with secrets (research R-004).
  - Consumed by `LocalStack` and service processes.
- **Validation rules**:
  - MUST NOT contain actual secrets or credentials (FR-009).
  - MUST document every variable that a service or CI job needs.
  - CI MUST use `.env.example` as the single source of truth (research R-004).
- **Lifecycle**: Created in Phase 1. Updated when new configuration variables are introduced.

### 8. OpenAPISpecValidation (CI Job)

- **Identity**: a CI job within `CIPipeline` that runs on PR.
- **Attributes**:
  - `validation_command` — the tool/script that bundles and validates OpenAPI specs.
  - `failure_threshold` — any diff between code and spec causes failure.
  - `skip_services` — list of services where OpenAPI is N/A (FR-005 edge case).
- **Relationships**:
  - Consumes OpenAPI spec files from each service directory.
  - Produces a required status check for branch protection.
- **Validation rules**:
  - MUST fail if a service's API surface has changed without a corresponding spec update (FR-005).
  - MUST catch at least 90% of drift (SC-007).
  - Services without REST APIs MUST be explicitly skippable.
- **Lifecycle**: Created in Phase 1. Updated when new services are added.

### 9. Makefile (Command Runner)

- **Identity**: file at `Makefile`.
- **Attributes**:
  - `targets` — set of command targets: `up`, `down`, `logs`, `test`, `lint`, `openapi` (FR-010).
- **Relationships**:
  - Invokes `LocalStack` for `up`/`down`/`logs`.
  - Invokes test commands for `test`.
  - Invokes lint commands for `lint`.
  - Invokes OpenAPI tooling for `openapi`.
- **Validation rules**:
  - All six targets MUST exist and be functional (FR-010).
  - `up` MUST start all services (FR-005).
  - `down` MUST stop all services (FR-006).
- **Lifecycle**: Created in Phase 1. Extended as new convenience targets are needed.

### 10. DockerRegistry (GHCR)

- **Identity**: GitHub Container Registry, associated with the repository.
- **Attributes**:
  - `images` — set of published image names (e.g., `ghcr.io/mezni/bornemap/auth-service`).
  - `visibility` — repository-scoped access control (follows repo visibility).
- **Relationships**:
  - Receives images from `CIPipeline.docker-build` job on merge to `main`.
  - Consumed by deployment environments (Phase 11+).
- **Validation rules**:
  - Images MUST be pushed only on merge to `main` (FR-002).
  - Authentication MUST use the workflow token (no separate secrets for read; write requires `GITHUB_TOKEN` with `packages: write` permission).
- **Lifecycle**: Configured in Phase 1 via repository settings.

## State / lifecycle summary

The aggregate state of Phase 1 has two values:

- **Not ratified** — one or more entities missing, validation fails, Phase 1 incomplete.
- **Ratified** — all ten entities exist and satisfy their validation rules. Phase 1 is complete; Phase 2 may begin.

The `quickstart.md` runbook executes the validation rules end-to-end and produces a pass/fail result.

## Out of scope for Phase 1 data model

- Prometheus/Grafana monitoring stack — has metrics endpoints but no monitoring infrastructure in Phase 1 (deferred to Phase 11).
- Service implementation details — each service's data model is defined in its own phase.
- Production deployment configuration — Kubernetes, load balancers, TLS certificates (Phase 11+).
- Issue templates (`.github/ISSUE_TEMPLATE/`) — deferred.
- Security policy (`SECURITY.md`) — deferred.

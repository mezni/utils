# Research: Phase 1 — CI/CD & Dev Environment

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-22

## Purpose

This document resolves technical decisions for the Phase 1 CI/CD and local development environment. All decisions were validated during the `/speckit.clarify` session (recorded in `spec.md` §Clarifications). Phase 0 research (R-001, R-002) provided the foundation; this document adds Phase 1-specific details.

## R-001 — CI runner infrastructure

**Context**: The CI pipeline requires execution environments for lint, unit, integration, OpenAPI bundle, and docker-build jobs.

**Decision**: Use GitHub-hosted runners (Ubuntu latest) for all CI jobs.

**Rationale**:
- Zero maintenance overhead — no runner infrastructure to provision, patch, or monitor
- Free tier (up to 2000 min/month for private repos) sufficient for Phase 1-2 workload
- Ubuntu latest includes Docker, Docker Compose, and common tooling pre-installed
- Native integration with GitHub Actions — no runner registration or token management

**Alternatives considered**:
- **Self-hosted runners** — Rejected. Adds operational overhead (provisioning, updates, availability) that contradicts Phase 1's goal of reducing friction. Would be justified only at higher CI volume or when specialized hardware is needed.
- **Hybrid (GitHub-hosted + self-hosted)** — Rejected at Phase 1. Docker-build to GHCR from GitHub-hosted runners has no egress cost; the hybrid complexity is not justified.

## R-002 — Docker registry

**Context**: FR-002 requires Docker images to be built in CI and pushed on merge to `main`.

**Decision**: Use GitHub Container Registry (GHCR).

**Rationale**:
- Co-located with GitHub Actions runners — lowest push/pull latency
- Same authentication token as the workflow (no extra secrets for read)
- GitHub Packages ecosystem integration: image published alongside source commits
- No separate account or rate limits (unlike Docker Hub's anonymous pull limits)
- Supports org-level access control through GitHub teams

**Alternatives considered**:
- **Docker Hub** — Rejected. Requires separate Docker Hub credentials as GitHub secrets; anonymous pull rate limits (100 pulls/6h per IP) would impact CI reliability; no native GitHub integration.
- **Self-hosted registry** — Rejected. Requires infrastructure to run and maintain. Not justified at Phase 1 scale.

## R-003 — Integration test execution model in CI

**Context**: FR-002 specifies integration tests against real dependencies. The execution model in CI determines how databases and queues are provisioned.

**Decision**: Use GitHub Actions service containers for dependency services (PostgreSQL, MongoDB, RabbitMQ). The service-under-test runs as a regular process.

**Rationale**:
- Service containers are a native GitHub Actions feature — no Docker-in-Docker required
- Faster startup than a full docker-compose stack (only dependency containers start)
- Each job gets isolated dependency instances — no cross-job interference
- Service container health checks are built in (GitHub Actions waits for the container to be ready)
- Local development still uses docker-compose for the full stack; only the CI execution model differs

**Alternatives considered**:
- **Full docker-compose stack in CI** — Rejected. Starts all services (including ones not needed for the test) which adds 60-90s to pipeline time. Higher resource usage on the runner.
- **Mocked dependencies** — Rejected. Does not validate real database behavior (PostGIS queries, MongoDB aggregation, RabbitMQ delivery semantics). Only acceptable for pure unit tests.
- **Docker-in-Docker** — Rejected. Requires privileged mode, more complex configuration, slower than service containers.

## R-004 — CI environment configuration

**Context**: FR-009 mandates environment configuration via `.env.example` without committing secrets. CI needs its own mechanism to configure test environments.

**Decision**: CI copies `.env.example` to `.env` as the base configuration. Sensitive values (GHCR tokens, API keys) are injected via GitHub Actions encrypted secrets. Non-sensitive CI-specific overrides are set via GitHub Actions workflow variables.

**Rationale**:
- Single source of truth: `.env.example` documents every expected variable with safe defaults
- Secrets are never in the repository — they exist only in GitHub's encrypted secrets store
- Developers and CI use the same env template, reducing drift
- CI-specific overrides (e.g., test database names) are explicit in the workflow YAML, not hidden in a separate `.env.ci` file

**Alternatives considered**:
- **Dedicated `.env.ci` file** — Rejected. Creates a second source of truth that inevitably drifts from `.env.example`.
- **All values as GitHub secrets** — Rejected. Unnecessarily tedious — non-sensitive defaults (port numbers, log levels) don't need secret protection.
- **GitHub Actions variables** — Used for non-sensitive CI overrides, complementing secrets for sensitive values.

## R-005 — Dockerfile ownership timeline

**Context**: The CI docker-build job needs Dockerfiles to build. Services are scaffolded in Phase 2+, not Phase 1.

**Decision**: Phase 1 uses official Docker images for infrastructure services (nginx, postgres, mongo, rabbitmq, keycloak). Service-specific Dockerfiles (auth, core, geo, analytics) are created in each service's respective phase. The docker-build CI job builds whatever Dockerfiles exist and skips services that don't have Dockerfiles yet.

**Rationale**:
- Avoids creating empty/scaffold Dockerfiles that would be rewritten in Phase 2+; each service team owns their Dockerfile
- CI pipeline is future-proof: adding a Dockerfile in a later phase automatically includes it in the build
- Infrastructure services don't need custom Dockerfiles — the official images are the production images
- The NGINX gateway does need a custom configuration, which ships as a volume mount or config file, not a custom image

**Alternatives considered**:
- **Phase 1 creates all Dockerfiles** — Rejected. Creates ownership ambiguity; service teams would rewrite them in Phase 2+, wasting effort.
- **Defer docker-build CI job to Phase 2+** — Rejected. The docker-build job validates image build mechanics; deferring it means Phase 1 CI would be incomplete.

## R-006 — CI pipeline job structure

**Context**: FR-002 specifies five CI jobs. The job ordering, parallelism, and failure semantics need to be determined.

**Decision**: Use a sequential fan-out pipeline: lint and unit test run first in parallel; integration and OpenAPI bundle run second (after lint passes); docker-build runs on every PR (build only) and on merge to `main` (build + push).

```text
push/PR ───┬── lint ─────────────────┐
            ├── unit ─────────────────┤
            ├── integration ──────────┤── docker-build (build only on PR; push on main)
            ├── openapi-bundle ───────┤
            └── dco ──────────────────┘
```

**Rationale**:
- Lint and unit tests are fastest and catch most issues early — run first
- Integration and OpenAPI are slower and depend on code passing lint — run after
- Docker build is the slowest job and depends on code being valid — runs in parallel with tests
- DCO check is fast and independent — runs immediately
- Fan-out parallelism keeps total wall-clock time within the 10-minute target (SC-001)

**Alternatives considered**:
- **Fully sequential** — Rejected; would exceed 10-minute target
- **Fully parallel** — Rejected; wastes runner minutes when fast lint jobs fail early
- **Matrix per service** — Rejected at Phase 1; the mono-repo doesn't have clear service boundaries in CI yet (services are stubs)

## R-007 — Stale-branch workflow implementation

**Context**: FR-016/FR-017 require weekly stale-branch detection with a tracking issue. Phase 0 research R-002 defined the high-level approach.

**Decision**: Implement as a scheduled GitHub Actions workflow using the `actions/github-script` action to query the GitHub API and create/update a single tracking issue.

**Rationale**:
- No external dependencies — uses only GitHub API via `actions/github-script`
- Single rolling issue avoids tracker noise (issue body is overwritten weekly)
- `actions/github-script` is maintained by GitHub and has full API access with the workflow token
- Weekly cadence matches the spec and avoids API rate limits

**Alternatives considered**:
- **Third-party action** — Rejected. Adds an external dependency for a simple API call.
- **Separate issue per branch** — Rejected. Creates tracker noise; Maintainers would need to close each issue individually.
- **Manual weekly review** — Rejected as Phase 0 interim only. Phase 1 must automate this.

## R-008 — NGINX gateway routing rules

**Context**: FR-008 requires NGINX to route requests to the correct service based on path prefix.

**Decision**: Use NGINX as a reverse proxy with path-based routing per the deployment topology defined in `docs/operations/deployment.md`. The routing rules are:

| Path prefix | Target service |
|-------------|---------------|
| `/auth/` | auth-service:3000 |
| `/api/core/` | core-service:3001 |
| `/api/geo/` | geo-service:3002 |
| `/api/analytics/` | analytics-service:3003 |
| `/health` | routes to all services' health endpoints |
| `/metrics` | routes to all services' metrics endpoints |

**Rationale**:
- Path-based routing is the standard NGINX pattern for multi-service backends
- Matches the deployment topology defined in the Constitution
- `/health` and `/metrics` special handling enables centralized observability

**Alternatives considered**:
- **Subdomain-based routing** — Rejected. Requires DNS configuration and is harder to replicate locally.
- **Single monolithic endpoint** — Rejected. Violates Principle I (Clean Modular Architecture).

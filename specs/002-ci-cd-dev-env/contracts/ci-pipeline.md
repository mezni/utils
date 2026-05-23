# Contract: CI Pipeline Jobs

**Path**: `.github/workflows/ci.yml`
**Consumers**: every developer; every reviewer; branch protection ruleset
**Source**: spec FR-002, FR-003, FR-004, research R-006

## Required jobs (order and behavior)

| Job | Runs on | Dependencies | Required status check name | Description |
|-----|---------|-------------|---------------------------|-------------|
| `lint` | every push, every PR | none | `lint` | Enforce code style and static analysis for all changed languages |
| `unit` | every push, every PR | none | `unit` | Run all unit tests for changed services |
| `dco` | every PR | none | `dco` | Verify every commit has `Signed-off-by:` trailer |
| `integration` | every push, every PR | `lint` passes | `integration` | Run integration tests against real dependencies via service containers |
| `openapi-bundle` | every push, every PR | `lint` passes | `openapi-bundle` | Bundle and validate OpenAPI specs; fail on drift |
| `docker-build` | every push, every PR | none | `docker-build` | Build all Docker images; push to GHCR only on merge to `main` |

## Runner environment

- **Runner**: `ubuntu-latest` (GitHub-hosted).
- **Service containers** for integration tests: `postgres:16-postgis`, `mongo:7`, `rabbitmq:4-management`.

## Execution rules

- Lint and unit run in parallel (no dependency between them).
- Integration and OpenAPI bundle wait for lint to pass (fail-fast: don't run slow jobs on bad code).
- Docker build runs concurrently with lint/unit (no dependency on other jobs for build-only).
- DCO runs immediately (fast, no dependencies).
- All jobs MUST produce a status check with the name in the table above (FR-003).

## Failure semantics

- Any job failure blocks merge (required status check on `main`).
- If `lint` fails, `integration` and `openapi-bundle` SHOULD be skipped (fail-fast matrix behavior).
- Pipeline is re-triggered on every push to the PR branch.
- A manual re-run button is available in the GitHub Actions UI.

## Non-goals

- Deployment jobs (deferred to Phase 11+).
- Matrix builds per service (deferred to Phase 2+ when services have distinct code).
- Caching (npm, cargo, Docker layers) — Phase 1 adds caching as an optimization, not a requirement.

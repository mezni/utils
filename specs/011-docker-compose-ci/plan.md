# Implementation Plan: Docker Compose and CI/CD

**Branch**: `011-docker-compose-ci` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/011-docker-compose-ci/spec.md`

## Summary

Single `docker-compose.yml` orchestrating PostgreSQL, PostGIS, Driver Service, Admin Service, and frontend apps. Six path-scoped GitHub Actions workflows build and test each service independently. Both Rust services run `sqlx::migrate!` on startup. Frontend apps use `API_BASE_URL` to target the correct backend.

## Technical Context

**Language/Version**: Yaml (Docker Compose v3.8+), Dockerfile (multi-stage), GitHub Actions workflow yaml

**Primary Dependencies**: Docker Compose, GitHub Actions, sqlx migrate

**Storage**: PostgreSQL 17 + PostGIS 3.5 via Docker image `postgis/postgis:17-3.5`

**Testing**: `docker compose up` manual verification, GitHub Actions auto-trigger on PR

**Target Platform**: Linux (Docker on any host, GitHub Actions ubuntu-latest)

**Project Type**: Infrastructure / CI-CD configuration

**Performance Goals**: `docker-compose up` complete under 60s, CI pipeline under 10 minutes

**Constraints**: Must match existing Driver Service + Admin Service Dockerfiles; no secrets in version control

**Scale/Scope**: Single-machine Docker Compose deployment; CI covers 2 Rust services

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Constitution Status**: Template only — not ratified. No binding governance gates.

| Check | Status | Notes |
|-------|--------|-------|
| Documented governance framework? | ⚠️ Template | Constitution still contains placeholder text |
| Architecture constraints apply? | ❌ No | No ratified constraints |
| Design freedom? | ✅ Yes | Follow existing project conventions |

**Decision**: Proceed with no constitution gates. Follow existing patterns.

## Project Structure

### Documentation (this feature)

```text
specs/011-docker-compose-ci/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── docker-compose.md   # Docker Compose service contracts
│   └── ci-workflows.md    # GitHub Actions workflow contracts
└── tasks.md             # Phase 2 output (created by /speckit.tasks)
```

### Source Code (repository root)

```text
./
├── docker-compose.yml          # Service orchestration
├── Dockerfile                  # (optional) root-level convenience
├── .github/
│   └── workflows/
│       ├── driver-service.yml  # Path-scoped: source/services/driver-service/
│       └── admin-service.yml   # Path-scoped: source/services/admin-service/
├── source/
│   ├── services/
│   │   ├── driver-service/
│   │   │   └── Dockerfile
│   │   └── admin-service/
│   │       └── Dockerfile
│   ├── apps/
│   │   ├── dashboard/
│   │   ├── driver-web/
│   │   └── driver-mobile/
│   └── crates/
│       ├── ev-core/
│       └── ev-db/
└── database/
    └── migrations/
```

**Structure Decision**: Docker Compose at repo root (conventional). Workflows under `.github/workflows/`. No new code directories.

## Complexity Tracking

> No Constitution violations to justify. Skip.

# Implementation Plan: Infrastructure Foundation (MVP Runtime Core)

**Branch**: `001-infrastructure-foundation` | **Date**: 2026-05-30 | **Spec**: spec.md

**Input**: Feature specification from `specs/001-infrastructure-foundation/spec.md`

## Summary

Establish a fully reproducible local + production-like runtime environment
using Docker Compose with 5 core infrastructure services: PostgreSQL+PostGIS,
MongoDB, RabbitMQ, Keycloak, and Traefik. The stack boots via a single
`docker compose up` command with zero manual setup. This is an operational
correctness phase — no business logic is introduced.

## Technical Context

**Language/Version**: YAML (Docker Compose v3.8+), Docker Compose plugin v2+

**Primary Dependencies**: Docker Engine 24+, Docker Compose v2+,
official images: postgis/postgis:16-3.4, mongo:7, rabbitmq:4-management,
quay.io/keycloak/keycloak:25, traefik:v3.1

**Storage**: Docker named volumes (pg_data, mongo_data, rabbitmq_data,
keycloak_data), init SQL scripts for PostGIS extensions

**Testing**: Manual verification via `docker compose ps`, health check
endpoints, connectivity test from temporary containers on bornemap-net

**Target Platform**: Linux x86_64 (Docker host — local dev or CI runner)

**Project Type**: Infrastructure / Docker Compose deployment stack

**Performance Goals**: N/A (Phase 1 — infrastructure correctness only, no
business throughput targets)

**Constraints**: All ports must be injectable via env vars; no hardcoded
secrets in compose files; no external DB port exposure; single Docker
network (bornemap-net)

**Scale/Scope**: Single developer machine (4 GB RAM, 10 GB disk minimum)
or single CI runner; 5 containers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Post-Design Re-check**: ✅ PASS — No constitution violations introduced
by design artifacts.

| Principle | Status | Rationale |
|-----------|--------|-----------|
| I. Source of Truth | ✅ PASS | No business data introduced — infrastructure only |
| II. Minimal Service | ✅ PASS | Only infrastructure services (DB, queue, auth, proxy) — not backend services governed by the service limit |
| III. Separation of Concerns | ✅ PASS | PostgreSQL for business data, MongoDB for analytics, RabbitMQ for messaging — aligned with domain ownership boundaries |
| IV. Event Discipline | ✅ PASS | RabbitMQ provisioned but not wired into event pipelines — no events introduced yet |
| V. Operational Simplicity | ✅ PASS | Docker Compose deployment (no K8s), single environment, deterministic build — fully aligned |

**GATE RESULT**: ✅ PASS — No violations. Proceeding to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-infrastructure-foundation/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
./
├── docker-compose.yml       # Main compose file
├── .env.example             # Environment variable template
├── .gitignore               # Ignore .env, Docker build artifacts
├── init/
│   └── postgis/
│       └── init.sql         # CREATE EXTENSION postgis, uuid-ossp
└── scripts/
    └── healthcheck.sh       # Shared health check utilities
```

**Structure Decision**: Infrastructure-only layout — flat root with
`docker-compose.yml`, `.env.example`, `init/` for DB initialization
scripts, and `scripts/` for health check utilities. No code project
structure needed (no application source in this phase).

## Complexity Tracking

> Not needed — Constitution Check passed with no violations.

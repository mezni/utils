# Implementation Plan: Infrastructure Bootstrap

**Branch**: `001-infra-bootstrap` | **Date**: 2026-06-18 | **Spec**: specs/001-infra-bootstrap/spec.md

**Input**: Feature specification from `/specs/001-infra-bootstrap/spec.md`

## Summary

Provision the entire BorneMap backend local development environment as a single Docker Compose stack: Postgres 16+PostGIS, Redis, Keycloak (with `bornemap` realm), and Traefik (with path-based routing). Includes DB schema bootstrap (3 databases, 3 schemas, per-service roles), Keycloak realm export with clients/roles, lightweight stub containers for routing verification, and `.env.example` for credentials.

## Technical Context

**Scripting/Config**: YAML (Docker Compose, Traefik), SQL DDL (Postgres migrations), JSON (Keycloak realm export), Shell (start/stop scripts)

**Primary Dependencies**: Docker Compose v2, Postgres 16+PostGIS image, Redis 7 image, Keycloak 25 image, Traefik v3 image

**Storage**: Docker named volumes (`pgdata`, `keycloak_data`, `redis_data`)

**Testing**: Manual verification per acceptance scenarios (shell-based health checks)

**Target Platform**: Linux (Docker Desktop on macOS/Windows)

**Project Type**: Infrastructure orchestration (dev-environment tooling)

**Performance Goals**: Full stack healthy within 120s of start command

**Constraints**: Must run on developer machines without cloud dependencies, no TLS, no auth middleware

**Scale/Scope**: Single-developer local environment, 4 infrastructure + 3 stub containers, 1 network (shared)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I.1 — No prohibited tech | ✅ PASS | No OCPP, billing, event streams, native modules, or autoscaling |
| I.2 — Exactly 3 services | ✅ PASS | Stub containers are placeholders, not services. Real services added in Sprints 1–2. |
| I.3 — Compile-time safety | ✅ N/A | No Rust or TypeScript code in Sprint 0 |
| I.4 — Read/write separation | ✅ N/A | No application code in Sprint 0 |
| I.5 — No credentials in git | ✅ PASS | Credentials via `.env` file, `.env.example` checked in with placeholders |
| VII — Doc sync after task | ✅ Noted | Will update SYSTEM_STATE.md, roadmap_status.md post-completion |

**No violations.**

## Project Structure

### Documentation (this feature)

```text
specs/001-infra-bootstrap/
├── plan.md              # This file
├── research.md          # Phase 0 — technology decisions
├── data-model.md        # Phase 1 — infrastructure topology model
├── quickstart.md        # Phase 1 — getting started guide
├── contracts/           # Phase 1 — interface contracts
│   ├── docker-compose-topology.md
│   ├── postgres-connection.md
│   ├── keycloak-realm.md
│   └── traefik-routing.md
└── tasks.md             # Phase 2 (created by /speckit.tasks)
```

### Source Code (repository root)

```text
source/infra/
├── docker-compose.yml          # Main service orchestration
├── .env.example                # Documented environment variables
├── postgres/
│   ├── init/
│   │   ├── 01-create-dbs.sql           # platform_db, keycloak_db, analytics_db
│   │   ├── 02-schemas-and-roles.sql    # gis, inventory, users + DB roles
│   │   └── 03-initial-tables.sql       # partners, stations, chargers tables
│   └── Dockerfile                      # (optional, if custom PG config needed)
├── keycloak/
│   └── realm-export/
│       └── bornemap-realm.json         # Realm with clients, roles, mappers
├── traefik/
│   ├── traefik.yml                     # Static config (entrypoints, providers)
│   └── dynamic/
│       └── routing.yml                 # Dynamic routing rules
├── stubs/
│   ├── auth-service/
│   │   └── Dockerfile                  # Lightweight HTTP stub
│   ├── admin-service/
│   │   └── Dockerfile                  # Lightweight HTTP stub
│   └── driver-service/
│       └── Dockerfile                  # Lightweight HTTP stub
└── scripts/
    ├── start.sh                        # Single start command
    ├── stop.sh                         # Clean shutdown
    └── healthcheck.sh                  # Verify all components
```

**Structure Decision**: The `source/infra/` directory follows the monorepo layout defined in the constitution (V.1). All Sprint 0 artifacts live under `source/infra/`. Stub containers are temporary and will be replaced by real services in Sprints 1–2.

## Complexity Tracking

No constitution violations to justify.

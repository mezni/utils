# Implementation Plan: Keycloak Authentication Setup

**Branch**: `013-keycloak-setup` | **Date**: 2026-06-10 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/013-keycloak-setup/spec.md`

## Summary

Add Keycloak as an authentication service in Docker Compose with a pre-configured `ev-platform` realm. The realm includes three roles (`registered_driver`, `partner`, `admin`), five OAuth2 clients (three public with PKCE, two confidential with service accounts), Google and Facebook social login identity providers, and a custom `partner_id` JWT claim mapper. The realm is configured manually on first run, exported to a version-controlled JSON file, and auto-imported on subsequent starts.

## Technical Context

**Language/Version**: Configuration — Docker Compose, Keycloak 24.0 (quay.io/keycloak/keycloak:24.0)

**Primary Dependencies**: Keycloak 24.0 container image, PostgreSQL 17 (shared with existing services via `keycloak` schema), `psql` for schema migration

**Storage**: PostgreSQL `keycloak` schema — realm, user, session, and event data persisted by Keycloak

**Testing**: Docker health check (`curl -f http://localhost:8180/realms/ev-platform`), manual verification via admin console and REST API, `docker compose down -v && docker compose up` clean-import test

**Target Platform**: Linux (Docker Engine 24+), Docker Compose

**Project Type**: Infrastructure/Configuration (Docker Compose service + Keycloak realm config)

**Performance Goals**: Keycloak health check passes within 120 seconds of `docker compose up`, token issuance under 2 seconds

**Constraints**: Port 8180 must be free on host; PostgreSQL must be healthy before Keycloak starts; realm export must be version-controlled

**Scale/Scope**: Single-node dev instance; not intended for production use without hardening

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Constitution Status**: Template only — not ratified. No binding governance gates.

| Check | Status | Notes |
|-------|--------|-------|
| Documented governance framework? | ⚠️ Template | Constitution still contains placeholder text |
| Architecture constraints apply? | ❌ No | No ratified constraints |
| Design freedom? | ✅ Yes | Follow existing project conventions |

**Decision**: Proceed with no constitution gates.

## Project Structure

### Documentation (this feature)

```text
specs/013-keycloak-setup/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (created by /speckit.tasks)
```

### Source Code (repository root)

```text
./
├── docker-compose.yml                   # +keycloak service
├── database/
│   └── migrations/
│       └── 0005_keycloak_schema.sql     # CREATE SCHEMA IF NOT EXISTS keycloak
├── infra/
│   ├── keycloak/
│   │   └── realm-export.json            # Version-controlled realm export
│   └── env/
│       ├── keycloak.env.example
│       ├── driver-service.env.example   # +KEYCLOAK_URL, KEYCLOAK_REALM
│       └── admin-service.env.example    # +KEYCLOAK_URL, KEYCLOAK_REALM
└── docs/
    └── project/
        └── bugs.md                      # Updated (resolved Sprint 2.6 bugs)
```

**Structure Decision**: Keycloak-specific files live under `infra/keycloak/`. Environment variable examples live under `infra/env/`. The database migration goes in the existing `database/migrations/` directory. The Docker Compose service is added to the existing root `docker-compose.yml`.

## Complexity Tracking

> No Constitution violations to justify. Skip.

# Implementation Plan: MVP-1 Infra Kickoff

## Technical Context

- **Monorepo**: pnpm workspaces for TS packages/apps, Cargo workspace for Rust crates
- **Services**: Rust with Actix-web, each with health + readiness endpoints
- **Databases**: PostGIS 15, Postgres 16 (2 instances) via Docker Compose
- **Apps**: Expo SDK 54 (mobile), React + Leaflet (web), React + shadcn/ui (dashboard)
- **Infra**: Docker Compose at `infra/docker-compose.yml`, config via `.env`

All technologies are specified in existing `docs/spec/*` files. No NEEDS CLARIFICATION.

## Constitution Check

- **Section 7 (Prohibitions)**: No violations — no secrets committed, no data stored without schema, no vendor lock-in
- **Section 8 (MVP Phasing)**: MVP-1 scope confirmed — infrastructure only, no business logic
- **GIS Service port :3003** locked per constitution v1.2 — GIS Service is MVP-2, placeholder services use 3000-3002

## Gate Evaluation

| Gate | Status | Notes |
|------|--------|-------|
| Scope boundary | ✅ PASS | All tasks fall within Pre-Sprint 1.0 + Sprints 1.1-1.3 |
| Tech stack compliance | ✅ PASS | Rust + TypeScript as specified |
| source/ exclusion | ✅ PASS | All paths at root level (services/, apps/, infra/) |
| Existing spec alignment | ✅ PASS | Aligned with docs/spec/* (api-contracts, db-schema, docker-compose-map, env-vars) |

## Sprint Breakdown

### Sprint 1.1 — Monorepo Scaffold & Service Shells

| Task | Effort | Dependencies |
|------|--------|--------------|
| Initialize Cargo workspace + pnpm workspaces at project root | 1h | None |
| Scaffold `apps/mobile-driver` (Expo shell) | 1h | pnpm workspace |
| Scaffold `apps/web-driver` (React shell) | 1h | pnpm workspace |
| Scaffold `apps/dashboard` (React + shadcn/ui shell) | 1h | pnpm workspace |
| Scaffold `packages/shared-types`, `shared-ui`, `shared-hooks`, `api-client` | 2h | pnpm workspace |
| Scaffold `crates/db-models`, `crates/validation` | 1h | Cargo workspace |
| Scaffold `services/auth-service` with health endpoint | 2h | Cargo workspace |
| Scaffold `services/driver-service` with health endpoint | 2h | Cargo workspace |
| Scaffold `services/admin-service` with health endpoint | 2h | Cargo workspace |
| Verify all 3 services compile and respond on `/api/v1/health` | 0.5h | All services scaffolded |

### Sprint 1.2 — Databases & Infrastructure

| Task | Effort | Dependencies |
|------|--------|--------------|
| Create `infra/docker-compose.yml` with platform_db (PostGIS) | 1h | None |
| Add keycloak_db container to compose | 0.5h | Compose file exists |
| Add analytics_db container to compose | 0.5h | Compose file exists |
| Create `infra/db/init-platform-db.sql` with gis/inventory/users schemas | 1h | db-schema.md spec |
| Add healthcheck to platform_db container | 0.5h | platform_db defined |
| Create `.env.example` with all MVP-1 vars | 0.5h | env-vars.md spec |
| Wire services to depend on platform_db (condition: service_healthy) | 0.5h | Compose + services exist |
| Verify `docker compose up` starts all containers | 0.5h | All compose tasks |
| Verify services return 200 on `/health/ready` after DB is healthy | 0.5h | Services + DB running |

### Sprint 1.3 — Driver Apps Base Map & Dashboard Shell

| Task | Effort | Dependencies |
|------|--------|--------------|
| mobile-driver: install react-native-maps, render map centered on Tunisia | 2h | App shell from 1.1 |
| web-driver: install Leaflet, render map centered on Tunisia | 1h | App shell from 1.1 |
| dashboard: install shadcn/ui, React Router, blank page shell | 2h | App shell from 1.1 |
| dashboard: branded logged-out state (login prompt) | 1h | Dashboard shell |
| Verify all 3 clients load without errors | 0.5h | All app tasks |

## Generated Artifacts

- `research.md` — Phase 0 research findings
- `data-model.md` — directory structure + service layout
- `contracts/` — health endpoint contract specs
- `quickstart.md` — dev environment setup guide

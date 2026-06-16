# MVP-1 — Infra Kickoff

**Status**: Complete (all 46 tasks implemented)
**Last updated**: 2026-06-15

## Scope
- Empty service shells (Auth, Driver, Admin) with `/api/v1/health` and `/api/v1/health/ready`
- Empty databases: `platform_db` (schemas: gis, inventory, users), `keycloak_db`, `analytics_db`
- Map view (mobile/web driver apps) — no real data, base map only
- Blank dashboard page with routing and auth-gated home

## Tasks / Progress
- [x] Scaffold monorepo structure (services/, apps/, packages/, crates/)
- [x] Auth Service shell + health endpoint + readiness endpoint
- [x] Driver Service shell + health endpoint + readiness endpoint
- [x] Admin Service shell + health endpoint + readiness endpoint
- [x] docker-compose: platform_db, keycloak_db, analytics_db, redis — all with healthchecks
- [x] mobile-driver: base map skeleton (Expo SDK 54)
- [x] web-driver: base map (React + Leaflet, Tunisia center)
- [x] dashboard: full shell with router, login page, home with sidebar, 404 page
- [x] `.env.example` with all MVP-1 vars
- [x] `init-platform-db.sql` with all enums, schemas, tables, indexes
- [x] Cargo workspace with 3 services + 2 crates
- [x] pnpm workspace with 2 apps + 4 packages
- [x] `Makefile` with common dev targets

## Bugs
| ID | Description | Severity | Status | Notes |
|----|--------------|----------|--------|-------|

## Changes & Decisions
- 2026-06-15: Constitution v1.2 finalized; MVP-1 scope confirmed (no mocks, real Rust services from start)
- 2026-06-15: All 46 tasks from speckit spec implemented; awaiting cargo build verification

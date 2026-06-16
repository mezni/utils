# MVP-1 — Infra Kickoff

**Status**: Not Started
**Last updated**: 2026-06-15

## Scope
- Empty service shells (Auth, Driver, Admin) with `/api/v1/health` only
- Empty databases: `platform_db` (schemas: gis, inventory, users), `keycloak_db`, `analytics_db`
- Map view (mobile/web driver apps) — no real data, base map only
- Blank dashboard page

## Tasks / Progress
- [ ] Scaffold monorepo structure (services/, apps/, packages/, crates/)
- [ ] Auth Service shell + health endpoint
- [ ] Driver Service shell + health endpoint
- [ ] Admin Service shell + health endpoint
- [ ] docker-compose: platform_db, keycloak_db, analytics_db (empty, schemas created)
- [ ] mobile-driver: base map (Expo SDK 54)
- [ ] web-driver: base map (React + Leaflet)
- [ ] dashboard: blank page shell

## Bugs
| ID | Description | Severity | Status | Notes |
|----|--------------|----------|--------|-------|

## Changes & Decisions
- 2026-06-15: Constitution v1.2 finalized; MVP-1 scope confirmed (no mocks, real Rust services from start)

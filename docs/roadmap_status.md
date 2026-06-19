# BorneMap — Roadmap Status

> High-level MVP tracking. Updated after every completed session.
> For granular task tracking, see `sprint_backlog.md`.

---

## MVPs

| MVP | Description | Status | Depends on |
|-----|-----------|--------|-----------|
| 0 | Project scaffolding: docs, guardrails, constitution, monorepo layout, skills | 🟢 Complete | — |
| 1 | Admin flow: infra (Keycloak, Postgres, Redis, Traefik) + Auth Service + Admin Service + Dashboard UI | 🟢 Auth Service implemented (44/44 tasks complete) — all core features implemented, Dockerfile created, integration tests stubbed, load testing script ready, security verification procedure documented | MVP-0 |
| 2 | Driver Service: PostGIS spatial reads, materialized views, Redis cache consumption | ⬜ Not started | MVP-1 |
| 3 | Web driver app: React + Leaflet map, station list, favourites | ⬜ Not started | MVP-2 |
| 4 | Mobile driver app: Expo SDK 54, map view, offline cache, favourites | ⬜ Not started | MVP-2 |
| 5 | OSM data pipeline: importer, sync, deduplication | ⬜ Not started | MVP-2 |
| 6 | E2E testing & hardening: Playwright, CI pipeline, bug bash | ⬜ Not started | MVP-3/4 |

---

## Milestones

| Milestone | Target date | Criteria |
|-----------|-----------|--------|
| Sprint 0 infra spec | 🟢 Complete | Docker Compose, Keycloak realm, DB schemas, Traefik routing specified + planned + taskified |
| Sprint 0 infra built | 🟢 Complete | 8-service Docker Compose stack running, Keycloak realm configured, Postgres 3 DBs with schemas, Traefik routing with rewrite-to-root |
| Sprint 1 spec/plan | 🟢 Complete | Auth Service spec, plan, research, data-model, contracts, 44 tasks, analysis with remediation applied, Phase 6 polish complete |
| Auth + Admin infra live | 🟢 Complete | Keycloak realm + Postgres + Redis + Traefik running in Docker |
| Auth working | 🟡 Implemented (pending integration testing) | Login, refresh, logout, profile endpoints implemented with token validation, rate limiting, log redaction, user profile upsert, JWT claims parser, CORS configured |
| Admin CRUD operational | TBD | Partner/station/charger CRUD with transactions, Redis bust, diff-based audit |
| Map visible | TBD | Stations shown on web + mobile maps from real data |
| Partner can add stations | TBD | Dashboard CRUD + cache bust + map update |
| Full mobile experience | TBD | Offline cache, favourites, navigation |
| Public beta | TBD | All core flows tested, CI green |

---

## Legend

| Icon | Meaning |
|------|---------|
| 🟢 Complete | Built, tested, verified |
| 🟡 In progress | Being worked on this session |
| ⬜ Not started | Not yet begun |
| 🔴 Blocked | Waiting on dependency or decision |

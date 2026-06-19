# BorneMap — Roadmap Status

> High-level MVP tracking. Updated after every completed session.
> For granular task tracking, see `sprint_backlog.md`.

---

## MVPs

| MVP | Description | Status | Depends on |
|-----|-----------|--------|-----------|
| 0 | Project scaffolding: docs, guardrails, constitution, monorepo layout, skills | 🟢 Complete | — |
| 1 | Admin flow: infra (Keycloak, Postgres, Redis, Traefik) + Auth Service + Admin Service + Dashboard UI | ⬜ Not started | MVP-0 |
| 2 | Driver Service: PostGIS spatial reads, materialized views, Redis cache consumption | ⬜ Not started | MVP-1 |
| 3 | Web driver app: React + Leaflet map, station list, favourites | ⬜ Not started | MVP-2 |
| 4 | Mobile driver app: Expo SDK 54, map view, offline cache, favourites | ⬜ Not started | MVP-2 |
| 5 | OSM data pipeline: importer, sync, deduplication | ⬜ Not started | MVP-2 |
| 6 | E2E testing & hardening: Playwright, CI pipeline, bug bash | ⬜ Not started | MVP-3/4 |

---

## Milestones

| Milestone | Target date | Criteria |
|-----------|-----------|--------|
| Auth working | TBD | Login, refresh, role-based access working end-to-end |
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

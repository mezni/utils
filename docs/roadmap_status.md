# BorneMap — Roadmap Status

> High-level MVP tracking. Updated after every completed session.
> For granular task tracking, see `sprint_backlog.md`.

---

## MVPs

| MVP | Description | Status | Depends on |
|-----|-----------|--------|-----------|
| 0 | Project scaffolding: docs, guardrails, constitution, monorepo layout | 🟢 Complete | — |
| 1 | Auth Service: login, refresh, profile sync, Keycloak realm | ⬜ Not started | MVP-0 |
| 2 | GIS ingestion & Driver Service: PostGIS, nearby query, materialized views | ⬜ Not started | MVP-1 |
| 3 | Admin Service: partner CRUD, station/charger management | ⬜ Not started | MVP-2 |
| 4 | Web driver app: map, station list, favourites | ⬜ Not started | MVP-2 |
| 5 | Mobile driver app: Expo map, offline cache, favourites | ⬜ Not started | MVP-2 |
| 6 | Dashboard: partner portal, admin panel, analytics | ⬜ Not started | MVP-3 |
| 7 | Traefik gateway: TLS, routing, JWT validation | ⬜ Not started | MVP-1 |
| 8 | Redis cache layer: spatial tile caching, cache bust | ⬜ Not started | MVP-2 |
| 9 | OSM data pipeline: importer, sync, deduplication | ⬜ Not started | MVP-2 |
| 10 | E2E testing & hardening: Playwright, CI pipeline, bug bash | ⬜ Not started | MVP-4/5/6 |

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

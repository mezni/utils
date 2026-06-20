# Sprint 001 — Technology Research

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Backend language | Rust 1.85+ | Constitution mandate; sqlx + georust for PostGIS |
| Frontend framework | React + Leaflet | Constitution mandate; Leaflet is free, no API key |
| Database | PostgreSQL 16 + PostGIS 3.4+ | Constitution mandate; ST_DWithin, GiST, GEOGRAPHY |
| GIS query pattern | Materialized View + Function | Constitution Principle II — never hit base tables |
| ID format | Typed prefix + nanoid(12) | Constitution Principle IV — URL-safe, non-enumerable |
| Sync pattern | Upsert-merge (ON CONFLICT DO UPDATE) | Constitution Principle III — idempotent by design |
| Map library | Leaflet | Free, OSM tiles, lightweight |
| Containerization | Docker Compose | Constitution mandate |
| Auth (sprint scope) | None for drivers; partner auth deferred | Spec assumption — Keycloak integration in later sprint |

See `specs/001-ev-charging-foundation/research.md` for full alternatives considered.

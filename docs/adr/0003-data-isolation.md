# ADR-0003: Data Isolation Strategy

**Status:** Accepted
**Date:** June 2026
**Deciders:** Project team

---

## Context

We need to ensure data isolation across the three services while maintaining the ability to run spatial queries across service boundaries.

## Decision

Adopt schema-level isolation within a single `platform_db`:
- **`users` schema** — owned by auth-service. Contains user profiles.
- **`inventory` schema** — owned by admin-service. Contains partners, stations, chargers, materialized views.
- **`gis` schema** — owned by driver-service. Contains raw OSM import data.
- **`analytics_db`** — separate database owned by admin-service for event logs.

Cross-schema reads permitted ONLY for:
- driver-service reading materialized views (`inventory.mv_stations_geo`, etc.) via a dedicated read-only DB role
- auth-service reading nothing outside `users`

Cross-schema writes are NEVER permitted without service API mediation.

## Consequences

**Positive:**
- Single database simplifies operations
- Schema isolation prevents accidental cross-service writes
- Materialized views allow driver-service to serve geo data without owning it
- No shared mutable state at DB level

**Negative:**
- Single database is a shared failure domain
- Materialized view refresh adds complexity
- Connection pooling considerations

## Compliance

Enforced by `tools/ci_guard.sh` Gate 3 — grep-based schema isolation check.

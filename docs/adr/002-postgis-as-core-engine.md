# ADR-002: PostGIS as Core Spatial Engine

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-1

---

## Context

BorneMap requires spatial queries for station discovery (nearby, map bounds, distance calculation). Options included external geocoding APIs, dedicated geospatial databases, or PostGIS.

## Decision

**PostgreSQL with PostGIS extension** is the core spatial engine.

## Rationale

- Eliminates external service dependency for core geo queries
- Single database reduces operational complexity
- PostGIS offers production-grade spatial indexing (GIST)
- Supports radius queries, bounding box, and distance calculations natively
- Lat/lng columns are the initial format; future upgrade to `GEOGRAPHY(Point, 4326)` for production scale

## Consequences

- platform_db requires PostGIS extension enabled
- All spatial queries run in-database (no application-level geo math)
- Migration path: lat/lng columns → PostGIS geography columns post MVP-2
- GIS data (OSM) is imported and read-only

## Expected Performance

- `/stations/nearby` with radius ≤ 3000m: < 200ms target
- Proper index strategy essential (GIST on geography column)

## Related

- ADR-006: Source Root Monorepo

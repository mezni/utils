# ADR-001: PostgreSQL as single database

**Status:** Accepted
**Date:** 2026-06-09

## Context

BorneMap needs a single database that serves all platform data: inventory, GIS, user accounts, and analytics. Multiple databases would add operational complexity, increase deployment surface, and complicate cross-schema queries.

## Decision

Use PostgreSQL as the single database across all MVPs. Database name: `ev_platform`. Schemas (`inventory`, `gis`, `users`, `analytics`) provide domain separation within the same database. PostGIS extension adds spatial capabilities when needed (MVP-2+).

## Consequences

- Single connection pool to manage per service
- Cross-schema queries possible within a single database transaction
- Schema separation enforces domain boundaries without distributed transaction overhead
- PostGIS extension is available from MVP-2 without additional infrastructure

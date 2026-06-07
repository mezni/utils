# ADR-001: PostgreSQL + PostGIS as Single Database

**Status**: Accepted
**Date**: 2026-06-07
**Related Tasks**: TASK-15 through TASK-24

## Context

The platform needs to store inventory data (partners, stations, chargers), spatial data (GIS, coordinates), user data, and analytics. Multiple database options exist: separate databases per domain, single database with schema separation, or polyglot persistence.

## Decision

Use a single PostgreSQL 16 instance with PostGIS 3.4 extension. Separate domains by PostgreSQL schema (inventory, gis, users, analytics).

## Rationale

- Single database simplifies deployment, backup, and operations — critical for a one-person ops team
- Schema separation provides logical isolation without operational overhead
- PostGIS is the gold standard for spatial queries in PostgreSQL
- Cross-schema joins remain possible for reporting
- ADR-006 (Docker Compose) reinforces this: one Postgres container is simpler to manage

## Consequences

- All data lives in one database — a single point of failure for storage
- Schema-level access control must be enforced at the application layer (no cross-schema writes)
- Migration numbering is global across all schemas
- Future separation into independent databases would require significant refactoring

## Compliance

- Every service runs all migrations on startup via sqlx::migrate!
- Cross-schema access follows the table in section 12 of the constitution
- No additional databases without a new ADR

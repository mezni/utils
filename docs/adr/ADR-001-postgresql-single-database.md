# ADR-001: PostgreSQL as Single Database

**Status**: Accepted

**Date**: 2026-01-01

## Context

BorneMap requires a persistent data store for stations, partners, chargers, users, and analytics. The decision is between:
- Single PostgreSQL database with multiple schemas
- Multiple specialized databases (PostgreSQL for business data, MongoDB for events, etc.)

## Decision

**Use a single PostgreSQL database as the sole data store across all MVPs.**

All data — business, user, GIS, and analytics — lives in `ev_platform`, separated by schema (`inventory`, `users`, `gis`, `analytics`).

## Rationale

- **Operational simplicity**: One database to manage, backup, and monitor. Operable by one person per constitution principle 4.
- **ACID transactions**: Multi-entity operations (e.g., create station with chargers) are atomic across schemas.
- **Schema-based separation**: Domains are logically separated without operational overhead of multiple databases.
- **PostgreSQL capabilities**: PostGIS for spatial queries (MVP-2+), JSON fields for flexible data, triggers for automation (MVP-4), all within one system.
- **Cost**: Single database is cheaper to operate than a polyglot stack.

## Consequences

- All services connect to the same PostgreSQL instance.
- Cross-schema access is governed by constitution section 8, with explicit permission table.
- GIS schema is empty until MVP-4; schema must be created in MVP-1 to reserve it.
- Scaling beyond a single PostgreSQL instance requires a new ADR.

## Supersedes

None. First ADR.

## References

- Constitution section 8: Data Architecture
- Constitution section 5: Domain Separation by Schema

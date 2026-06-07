# ADR-002: Schema Separation over Database Separation

**Status**: Accepted
**Date**: 2026-06-07
**Supersedes**: None

## Context

Options for domain isolation: separate databases per domain, or PostgreSQL schemas within a single database.

## Decision

Use PostgreSQL schemas (inventory, gis, users, analytics) within a single database rather than separate databases.

## Rationale

- Simpler backup: one pg_dump captures everything
- Single connection string for all services
- Cross-domain reporting can join across schemas without database links
- PostgreSQL schemas provide namespace isolation with shared transaction scope
- Avoids connection pool fragmentation across multiple databases

## Consequences

- Access control depends on application-layer enforcement (service middleware)
- A misconfigured service could potentially read another schema
- All schemas share the same database resources (connections, memory, disk)

## Compliance

- Each service declares which schemas it reads and writes
- Cross-schema writes are forbidden except where explicitly permitted
- Any violation is a Class A issue

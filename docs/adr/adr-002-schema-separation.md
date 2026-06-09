# ADR-002: Schema separation

**Status:** Accepted
**Date:** 2026-06-09

## Context

Business data, GIS data, user data, and analytics data have different access patterns, ownership rules, and lifecycle requirements. Mixing them in flat tables creates ownership ambiguity and makes it difficult to enforce access control at the service level.

## Decision

Separate domains by PostgreSQL schema: `inventory` (partners, stations, chargers), `gis` (spatial data), `users` (accounts, profiles, reviews), `analytics` (events, aggregates). Cross-schema writes are forbidden except where explicitly permitted by the constitution.

## Consequences

- Clear ownership: each service has defined read/write boundaries
- Schema-level permissions possible in PostgreSQL
- Cross-schema joins are explicit and intentional
- New domains can be added as new schemas without restructuring existing ones

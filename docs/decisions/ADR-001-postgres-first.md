# ADR-001: PostgreSQL-First with JSONB for Analytics

## Status

Accepted

## Context

The system needs both structured business data (stations, users, reviews)
and semi-structured analytics data (clickstream events, connection
aggregates). MongoDB was initially considered for analytics.

## Decision

Use PostgreSQL for all data, including analytics. JSONB columns with
GIN indexes handle the semi-structured analytics payloads. The
`analytics` schema is separated from business schemas (`inventory`,
`users`, `gis`) for domain isolation.

## Consequences

- Single database to operate and back up
- No cross-database joins or coordination
- JSONB provides sufficient flexibility for analytics schema evolution
- Eliminates operational overhead of a separate MongoDB instance
- GIN indexes on JSONB columns maintain query performance

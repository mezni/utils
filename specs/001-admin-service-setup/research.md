# Research: Admin Service Setup (Sprint 1.1)

**Date**: 2026-06-19 | **Phase**: 0 — Outline & Research

## Decisions

### Decision 1: API Update Semantics (PATCH)
**Decision**: Use PATCH for partial updates instead of PUT.
**Rationale**: PATCH allows clients to send only changed fields, reducing payload size and enabling field-level validation without requiring full entity knowledge.
**Alternatives considered**: PUT (full replacement) rejected because it forces clients to send complete entities and risks accidental field clearing.

### Decision 2: Soft Delete (No Restore)
**Decision**: Soft deletion uses `deleted_at` timestamp. No restore endpoint.
**Rationale**: Simpler API surface. Recovery requires database-level intervention, which is acceptable for validation-phase scale.
**Alternatives considered**: RESTORE endpoint (additional API surface with marginal benefit), time-limited restore (unnecessary complexity for validation phase).

### Decision 3: Identity Prefix Alignment
**Decision**: OPR for operators/partners, STA for stations, CHG for chargers.
**Rationale**: Matches the canonical project constitution. Avoids collision with other entity prefixes (USR for users).
**Alternatives considered**: PRT (non-canonical, rejected during review), STN (non-canonical).

### Decision 4: Stack Selection
**Decision**: Rust (Actix-web) + SQLx for backend, React + shadcn/ui for dashboard.
**Rationale**: Mandated by project constitution. Actix-web provides high-performance async HTTP. SQLx guarantees compile-time query safety. shadcn/ui provides consistent component API for the dashboard.

### Decision 5: Spatial Data Type
**Decision**: `GEOGRAPHY(Point, 4326)` with GIST index.
**Rationale**: GEOGRAPHY type handles geodesic calculations correctly on the WGS84 ellipsoid. GIST enables efficient spatial queries.
**Alternatives considered**: GEOMETRY (rejected — requires manual projection handling, less accurate for distance calculations on global data).

### Decision 6: Database Schema Isolation
**Decision**: All entities live under the `inventory` schema within `platform_db`.
**Rationale**: Constitution mandates inventory schema for admin-service. No cross-schema access allowed. gis, users schemas are owned by other services.

### Decision 7: Lookup Table Seed Values
**Decision**: Seed migrations for all 5 lookup tables.
**Rationale**: lookup tables represent fixed ENUM values that must be present for data integrity. Seed data is part of the migration pipeline:
- current_types: AC, DC
- connector_types: Type2, CCS, CHAdeMO
- access_types: public, restricted, private
- data_sources: manual, osm, partner
- connector_statuses: available, occupied, offline, unknown

## No Unresolved Clarifications

All specification ambiguities were resolved during the clarification phase. No NEEDS CLARIFICATION markers remain. The plan is ready for Phase 1 (Design & Contracts).

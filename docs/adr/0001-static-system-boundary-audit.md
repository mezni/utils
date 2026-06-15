# ADR-0001: Static System Boundary Audit for MVP-1

**Status**: Accepted

**Date**: 2026-06-14

## Context

BorneMap is being built as a phased system. For MVP-1, we need strict boundaries to prevent scope creep and enforce architectural discipline. The constitution defines a "Database-First, Single-Service, GIS-Isolated" approach, but these rules need explicit ADR-level documentation to prevent drift as the codebase grows.

## Decision

Adopt the following static system boundaries for MVP-1:

### Boundary 1: Database → Service
- All geospatial logic MUST live in PostGIS functions
- Rust services MUST NOT implement ST_DWithin, ST_Distance, or any spatial calculation
- Only exception: Tunisia bounding-box validation (simple numeric comparison in geo-core)

### Boundary 2: Schema Isolation
- `gis` schema is read-only for services. No service writes to `gis.*`
- `inventory` schema is writeable only by admin-service (deferred). Driver-service reads only.
- Cross-schema sync uses PL/pgSQL triggers exclusively

### Boundary 3: Service Isolation
- No inter-service HTTP calls in MVP-1
- driver-service is standalone — it talks to postgres and responds to HTTP
- No service discovery, no service mesh, no internal routing

### Boundary 4: Import Isolation
- OSM import script writes directly to `gis.osm_stations` with `source='OSM_IMPORT'`
- Import script does NOT write to `inventory.*`
- Platform-synced records have `source='PLATFORM_SYNC'`

## Alternatives Considered

### Application-level geo-computation in Rust
- Rejected: Duplicates PostGIS logic, loses index utilization, harder to optimize

### Dual-write from driver-service (inventory + gis)
- Rejected: Violates GIS isolation, creates consistency risk, more complex code

### Full service mesh for MVP-1
- Rejected: Premature complexity for a single-service system

## Consequences
- All spatial index optimization happens in PostgreSQL (centralized, tunable)
- Clear ownership: gis schema can be safely regenerated from inventory
- Future services (auth, admin) slot into the same pattern without refactoring
- OSM import path is intentionally separate from inventory to avoid data quality issues

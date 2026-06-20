# Research: EV Charging Platform Foundation

## Decisions

### Backend Language: Rust 1.85+
**Rationale**: Constitution mandates Rust for backend services. Strong type system,
zero-cost abstractions, and excellent PostgreSQL/PostGIS ecosystem (sqlx, georust).
**Alternatives considered**: Go, Python (rejected by constitution).

### Web Frontend: Node.js 22+ with React + Leaflet
**Rationale**: Constitution mandates Node.js for frontends. React for component
architecture, Leaflet for lightweight map rendering without paid API keys.
**Alternatives considered**: Vue, Svelte (possible but React is the project convention).

### Database: PostgreSQL 16 + PostGIS 3.4+
**Rationale**: Constitution mandates PostGIS for all spatial operations. PostGIS
provides ST_DWithin, ST_Distance, GiST indexing, and GEOGRAPHY type for accurate
distance queries.
**Alternatives considered**: MongoDB with GeoJSON (no FK enforcement, weaker spatial
query performance at scale).

### GIS Query Pattern: Materialized View + Function
**Rationale**: Constitution principle II (Spatial-First) mandates MVs for read queries.
`mv_stations_geo` pre-joins stations, chargers, and connectors with power tier
classification. `find_nearby_stations()` function wraps ST_DWithin with consistent
access pattern. Prevents direct base-table access and ensures GiST index usage.

### ID Format: Typed Prefix + Nanoid(12)
**Rationale**: Constitution principle IV. Nanoid provides URL-safe, collision-resistant
IDs without sequential enumeration. Prefixes (PAR-, STA-, CHR-, CON-, JOB-) enable
type identification from the ID alone.
**Alternatives considered**: UUIDv4 (too long), serial integers (enumerable, leaky).

### Sync Engine: Upsert-Merge Pattern
**Rationale**: Constitution principle III (Idempotency). Use ON CONFLICT ... DO UPDATE
for stations matched by osm_id or spatial proximity. Sync is manual-trigger in this
sprint. Each operation recorded in sync_jobs for audit trail.

### Web Map: Leaflet
**Rationale**: Free, no API key required, works with OSM tiles, lightweight.
React-Leaflet provides clean React integration.
**Alternatives considered**: Mapbox GL (paid), Google Maps (paid, heavy).

### Containerization: Docker Compose
**Rationale**: Constitution mandates Docker Compose for local development.
Single docker-compose.yml with all services, PostGIS, and Redis on internal network.
Deterministic initialization via init.sql scripts.

### Authentication: None for driver browsing (sprint scope)
**Rationale**: Spec assumption states no driver auth required for browsing/searching
in this sprint. Partner auth deferred to sprint-002. Keycloak is in the stack
(constitution) but not integrated in this sprint.

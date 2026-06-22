# Implementation Plan: GIS Engine Foundation

**Branch**: `003-gis-engine` | **Date**: 2026-06-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/003-gis-engine/spec.md`

## Summary

Implement a complete GIS engine for BorneMap microservices platform with OSM ingestion pipeline, PostGIS spatial queries, Redis spatial caching, and map rendering contracts. This foundation supports driver-facing spatial search functionality for finding nearby charging stations.

## Technical Context

**Language/Version**: Rust 1.75+ (Cargo-based toolchain)

**Primary Dependencies**: postgis (PostGIS bindings), sqlx (compile-time verified queries), redis (caching), serde, serde_json, actix-web, tokio, geo-types (geospatial types)

**Storage**: PostgreSQL (gis schema with staging and curated tables for charging stations, PostGIS extension for spatial functions), Redis (spatial cache keys: geo:radius:{lat}:{lon}:{radius}, geo:tile:{x}:{y}:{z}), OSM data source (overpass-api.de)

**Testing**: cargo test (unit/integration), cargo clippy (linting), cargo fmt (formatting)

**Target Platform**: Linux server (driver-service), Docker containers (PostgreSQL with PostGIS), Redis container (spatial cache)

**Project Type**: Monorepo with microservices architecture (3 backend services)

**Performance Goals**:
- OSM ingestion: < 10s for 1MB file, deterministic and idempotent
- Spatial query without cache: < 500ms (10,000 stations)
- Spatial query with cache: < 50ms
- Redis cache read: < 50ms, write: < 100ms
- Map rendering API: < 200ms response time

**Constraints**:
- Service topology lock: exactly 3 services on ports 3000, 3001, 3002
- GIS ownership: ONLY driver-service can write to gis schema
- Spatial query safety: NO raw SQL construction, all queries via SQLx
- Redis access: ONLY driver-service can write to Redis
- OSM ingestion: deterministic and idempotent (no duplicate data on retry)
- Contract-first: domain-types → backend → frontend
- SQLx compile-time verification mandatory
- Materialized views for query optimization
- RBAC enforcement on all GIS endpoints

**Scale/Scope**: 3 service modifications, 2 new tables (staging, curated), Redis integration, OSM ingestion pipeline, 13 must-have tasks, 8 should-have tasks

## Constitution Check

### Core Principles Compliance

**1. Service Topology Lock** ✅ COMPLIANT
- No topology changes needed (driver-service remains on 3001)
- No new services or ports added
- Existing 3-service architecture maintained

**2. Identity Dual System** ✅ COMPLIANT
- No mixing of UUID and nanoid in this feature
- GIS data uses nanoid(12) with PREFIX for station IDs (implicit - will be STA-XXXXXXXXXXXX)
- No UUID used in entity tables
- Validation will be enforced via CI gate

**3. Data Ownership** ✅ COMPLIANT
- GIS schema ownership: driver-service (write), admin-service (read-only for nearby search)
- Cross-service writes: admin-service CANNOT write to gis schema (enforced by database roles)
- Driver-service owns: gis schema, OSM ingestion pipeline
- No cross-service writes to gis schema
- CI gate will enforce ownership

**4. Contract-First** ✅ COMPLIANT
- Map rendering API contracts will be defined in domain-types crate first
- Domain-types will NOT depend on backend frameworks
- Backend will implement after contracts are defined

**5. SQLx Compile-Time Verification** ✅ COMPLIANT
- All spatial queries will use SQLx compile-time verification
- NO dynamic SQL string construction
- All queries will be compile-time verified before deployment

**6. CI Enforcement** ✅ COMPLIANT
- 12-stage CI pipeline with hard-stop on any failure
- New CI gates for Sprint 2:
  - CI-2.1: GIS ownership gate (driver-service only writes to gis)
  - CI-2.2: Spatial query safety gate (no raw SQL)
  - CI-2.3: Redis access gate (driver-service only)
  - CI-2.4: OSM reproducibility gate (deterministic and idempotent)
  - CI-2.5: Map API contract gate (responses match contracts)

**7. Forbidden Edges** ✅ COMPLIANT
- No service→service imports needed for this feature (external OSM API only)
- No circular dependencies
- No ui-kit→client-core or shared-domain→services

**8. Single-Writer Analytics** ✅ COMPLIANT
- Driver-service writes to analytics_db (ingestion events, spatial query logs)
- Admin-service reads from analytics_db (no write access)
- No changes needed to existing constraint

**9. Runtime Topology Enforcement** ✅ COMPLIANT
- No new HTTP servers
- No service spawning
- Redis as singleton cache for driver-service only

**10. Migration Drift Detection** ✅ COMPLIANT
- New migrations: 0003_gis_tables.up.sql, 0004_materialized_views.up.sql
- SQLx compile-time verification ensures migrations match code
- CI migration drift detection will validate schema hash

**11. Identity Location Rules** ✅ COMPLIANT
- No UUID usage in GIS entity tables
- GIS data uses nanoid(12) with PREFIX (STA-XXXXXXXXXXXX)
- CI gate will validate no UUID in gis tables

### Service Boundaries

**auth-service (Port 3000)**
- **Owned Data**: users schema (unchanged)
- **Functions**: Authentication, user profile management, Keycloak integration (unchanged)
- **No Access**: gis (unchanged), inventory (unchanged)

**driver-service (Port 3001)**
- **Owned Data**: gis schema (NEW - OSM staging, curated stations), analytics_db (write-only - ingestion events)
- **Functions**: GIS operations, OSM ingestion pipeline, spatial queries, Redis spatial cache read/write, map rendering API
- **No Access**: users (read-only via Keycloak), inventory (unchanged)

**admin-service (Port 3002)**
- **Owned Data**: inventory schema (unchanged), analytics_db (read-only)
- **Functions**: Station CRUD (if needed), inventory sync (unchanged), GIS data reading (unchanged - via API calls)
- **No Access**: gis (write-only via external API calls), users (unchanged)

### Data Domain Definitions

**gis (driver-service owned, write)**:
- `gis.osm_charging_stations_temp` - Staging table for raw OSM data (staging)
- `gis.osm_charging_stations` - Curated table with normalized data (curated)
- Materialized views: `mv_stations_geo`, `mv_stations_summary`
- Spatial functions and indices

**analytics_db (driver-service write, admin-service read)**:
- `telemetry.raw_events` - Ingestion events, spatial query logs
- Admin-service reads: no write access (unchanged)

## Phase 0: Research & Clarifications

### Research Tasks

**R-GIS-1**: OSM Data Format and Ingestion Patterns
- **Task**: Research OpenStreetMap XML format, overpass-api.de API, best practices for OSM data ingestion
- **Goal**: Determine ingestion pipeline design, data normalization strategy, idempotency approach
- **Alternatives**: Direct OSM API polling vs batch exports vs third-party OSM services

**R-GIS-2**: PostGIS Spatial Index Performance
- **Task**: Research PostGIS spatial index types (GiST, SP-GiST), optimization techniques for radius queries
- **Goal**: Determine optimal index strategy for 10,000+ charging stations with radius queries
- **Alternatives**: GiST indexes vs SP-GiST vs Geospatial B-Tree

**R-GIS-3**: Redis Spatial Cache Design
- **Task**: Research Redis spatial data structures, cache key design patterns, TTL strategies
- **Goal**: Determine cache key naming, TTL values, eviction policies
- **Alternatives**: Flat cache keys vs structured Redis GEO vs external geospatial database

**R-GIS-4**: OSM Station Representation
- **Task**: Research OSM charging station tags, standard amenity types, data model design
- **Goal**: Map OSM tags to internal schema fields
- **Alternatives**: Normalize to single table vs hierarchical structures

**R-GIS-5**: Materialized View Strategy
- **Task**: Research PostGIS materialized views, refresh strategies, concurrency handling
- **Goal**: Determine refresh frequency, refresh method, fallback behavior
- **Alternatives**: Manual refresh vs scheduled refresh vs live materialized views

### Assumptions for Research

1. We will use overpass-api.de for OSM data ingestion (stable, open-source)
2. OSM data is batch-imported daily (not real-time)
3. All coordinates use WGS 84 (lat/lon) standard
4. Station IDs will use nanoid(12) with STA- prefix
5. Redis will be used for spatial cache with flat keys (simpler than GEO commands)
6. Materialized views will be refreshed hourly during low-traffic periods
7. Mobile framework: Expo SDK 54 (React Native)
8. Web framework: React + Leaflet
9. Coordinate precision: double precision (not fixed precision)

### Pending Clarifications

1. **[NEEDS CLARIFICATION]** Data Refresh Frequency: Daily vs hourly ingestion?
2. **[NEEDS CLARIFICATION]** Station Approval Workflow: Manual review or automated based on OSM flags?
3. **[NEEDS CLARIFICATION]** Marker Clustering Threshold: 30 markers, 50 markers, or configurable?
4. **[NEEDS CLARIFICATION]** Coordinate Precision: Double precision vs fixed (6 decimal places)?
5. **[NEEDS CLARIFICATION]** Mobile Map Library: Mapbox GL JS vs Google Maps vs native iOS/Android SDKs?

## Phase 1: Design & Contracts

### Data Model

#### Entity: ChargingStation (gis.osm_charging_stations)

**Fields**:
```sql
CREATE TABLE gis.osm_charging_stations (
    id VARCHAR(25) PRIMARY KEY, -- nanoid(12) with "STA-" prefix
    osm_id BIGINT, -- OpenStreetMap node/way ID
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    station_name VARCHAR(255),
    operator VARCHAR(255),
    address TEXT,
    amenity VARCHAR(100), -- from OSM tag
    power VARCHAR(50), -- charging power (kW)
    connector_types TEXT[], -- list of connector types
    is_available BOOLEAN NOT NULL DEFAULT TRUE,
    last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(osm_id)
);

-- GiST index for spatial queries
CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);
```

**Relationships**:
- `osm_id` references OpenStreetMap node/way IDs
- No foreign keys to other tables (standalone entity)

**Validation Rules**:
- latitude: 90 to -90
- longitude: 180 to -180
- geom: Must be Point in SRID 4326
- station_name: Not empty
- power: Must be valid power value (e.g., "7kW", "22kW", "50kW")
- connector_types: Must contain at least one valid type

#### Entity: ChargingStationStaging (gis.osm_charging_stations_temp)

**Fields**:
```sql
CREATE TABLE gis.osm_charging_stations_temp (
    id VARCHAR(25) PRIMARY KEY, -- nanoid(12) with "STA-" prefix
    osm_id BIGINT NOT NULL,
    osm_data JSONB NOT NULL, -- Raw OSM XML as JSON
    import_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(osm_id)
);

CREATE INDEX idx_staging_osm_id ON gis.osm_charging_stations_temp(osm_id);
```

**Relationships**:
- `osm_id` references OpenStreetMap node/way IDs (one-to-one with curated table after ETL)
- `import_timestamp` tracks when data was imported
- `processed` flag indicates if ETL has run

**Validation Rules**:
- osm_id: Must be unique
- osm_data: Must be valid JSONB containing OSM XML tags
- processed: TRUE after ETL runs

### Contracts

#### API Contract: List Stations

**Endpoint**: `GET /api/v1/driver/stations`

**Request Parameters**:
- `page` (integer, optional, default: 1): Page number
- `limit` (integer, optional, default: 20): Items per page
- `lat` (double, required for pagination): Latitude center point
- `lon` (double, required for pagination): Longitude center point
- `radius` (integer, required): Radius in meters

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "STA-abc123456789",
      "name": "Central Park Charging",
      "latitude": 40.7829,
      "longitude": -73.9654,
      "distance": 123.5,
      "amenity": "charging_station",
      "power": "50kW",
      "connector_types": ["Type 2", "CCS"],
      "is_available": true,
      "last_updated": "2026-06-22T10:30:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 100,
    "total_pages": 5
  }
}
```

**Error Responses**:
- 400 Bad Request: Missing or invalid parameters
- 401 Unauthorized: Missing or invalid JWT token

**Contract Version**: v1.0.0

#### API Contract: Get Station by ID

**Endpoint**: `GET /api/v1/driver/stations/{id}`

**Path Parameters**:
- `id` (string, required): Station ID (STA-XXXXXXXXXXXX)

**Response** (200 OK):
```json
{
  "id": "STA-abc123456789",
  "name": "Central Park Charging",
  "latitude": 40.7829,
  "longitude": -73.9654,
  "amenity": "charging_station",
  "power": "50kW",
  "connector_types": ["Type 2", "CCS"],
  "is_available": true,
  "last_updated": "2026-06-22T10:30:00Z"
}
```

**Error Responses**:
- 404 Not Found: Station ID not found
- 401 Unauthorized: Missing or invalid JWT token

**Contract Version**: v1.0.0

#### API Contract: Nearby Stations

**Endpoint**: `GET /api/v1/driver/nearby`

**Request Parameters**:
- `lat` (double, required): Latitude
- `lon` (double, required): Longitude
- `radius` (integer, required): Radius in meters (min: 100, max: 100000)
- `limit` (integer, optional, default: 20, max: 100): Number of results

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "STA-abc123456789",
      "name": "Central Park Charging",
      "latitude": 40.7829,
      "longitude": -73.9654,
      "distance": 123.5,
      "amenity": "charging_station",
      "power": "50kW",
      "connector_types": ["Type 2", "CCS"],
      "is_available": true
    }
  ],
  "query": {
    "lat": 40.7829,
    "lon": -73.9654,
    "radius": 1000
  }
}
```

**Error Responses**:
- 400 Bad Request: Invalid parameters
- 401 Unauthorized: Missing or invalid JWT token

**Contract Version**: v1.0.0

### Quickstart Guide

#### Prerequisites

1. PostgreSQL 16+ with PostGIS extension enabled
2. Redis 7+
3. Rust 1.75+
4. OSM data source (overpass-api.de)

#### Quick Start Steps

1. **Start Dependencies**:
```bash
# PostgreSQL with PostGIS
docker run -d \
  --name bornemap_postgres \
  -e POSTGRES_DB=borne_map \
  -e POSTGRES_USER=borne_map_admin \
  -e POSTGRES_PASSWORD=borne_map_password \
  -p 5432:5432 \
  postgis/postgis:16-alpine

# Redis
docker run -d \
  --name bornemap_redis \
  -p 6379:6379 \
  redis:7-alpine
```

2. **Set Up Database**:
```bash
# Run migrations
./infrastructure/scripts/migrate.sh

# Create GIS schema (driver-service only)
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "CREATE SCHEMA gis;"
psql postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map \
  -c "GRANT ALL PRIVILEGES ON SCHEMA gis TO borne_map_driver;"
```

3. **Start Driver Service**:
```bash
# Set environment variables
export APP_DATABASE_URL="postgresql://borne_map_admin:borne_map_password@localhost:5432/borne_map"
export APP_ANALYTICS_DATABASE_URL="postgresql://borne_map_analytics:borne_map_password@localhost:5432/analytics_db"
export APP_REDIS_URL="redis://localhost:6379"
export APP_SERVER_PORT=3001

# Start the service
cargo run --bin driver-service
```

4. **Test Query**:
```bash
# Query nearby stations (requires JWT token)
curl -H "Authorization: Bearer <JWT_TOKEN>" \
  "http://localhost:3001/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000"
```

5. **Ingest OSM Data** (future):
```bash
# Trigger OSM ingestion (admin-triggered)
curl -X POST \
  -H "Authorization: Bearer <ADMIN_JWT_TOKEN>" \
  "http://localhost:3001/api/v1/gis/ingest"
```

#### Expected Output

- Nearby query returns stations within 1km
- Results ordered by distance from query point
- Cache hit reduces response time to < 50ms
- JWT token required for authentication

## Implementation Phases

### Phase 1: Foundation (S2-001 to S2-005)

**Tasks**:
- S2-002: Create `gis.osm_charging_stations_temp` table (staging)
- S2-003: Create `gis.osm_charging_stations` table (curated)
- S2-009: Set up Redis cache layer (geo:radius, geo:tile keys)
- S2-010: Implement Redis spatial cache read/write in driver-service
- S2-005: Implement PostGIS spatial query engine (nearby, bounding box, radius)
- S2-006: Implement `GET /api/v1/driver/nearby` endpoint
- S2-007: Implement `GET /api/v1/driver/station/:id` endpoint

**Deliverables**:
- Migration files for staging and curated tables
- Redis client integration in driver-service
- PostGIS query builder with SQLx
- REST endpoints for spatial queries

### Phase 2: ETL Pipeline (S2-001, S2-004, S2-012)

**Tasks**:
- S2-001: Implement OSM ingestion pipeline in driver-service (batch import, idempotent)
- S2-004: Implement staging → curated ETL pipeline
- S2-012: Implement GIS data normalization layer (OSM tag → internal schema)

**Deliverables**:
- OSM ingestion service
- ETL transformation logic
- Tag normalization rules

### Phase 3: Optimization (S2-008, S2-013)

**Tasks**:
- S2-008: Create materialized views (mv_stations_geo, mv_stations_summary)
- S2-013: Implement station clustering support in spatial queries

**Deliverables**:
- Materialized view definitions
- Clustering logic for markers

### Phase 4: CI Gates (S2-017 to S2-021)

**Tasks**:
- S2-017: Create CI GIS ownership gate
- S2-018: Create CI spatial query safety gate
- S2-019: Create CI Redis access gate
- S2-020: Create CI OSM reproducibility gate
- S2-021: Create CI map API contract gate

**Deliverables**:
- CI gate scripts
- Integration into ci_guard.sh

### Phase 5: Documentation & Testing

**Tasks**:
- Write integration tests
- Update documentation (SYSTEM_STATE.md, sprint review)
- Verify all exit criteria

## Exit Criteria

Sprint 2 is COMPLETE ONLY IF:
- [ ] OSM ingestion works deterministically (batch, idempotent)
- [ ] PostGIS queries return correct spatial results
- [ ] `/nearby` endpoint fully functional with contract enforced
- [ ] Redis caching operational and isolated to driver-service
- [ ] GIS ownership gate passes
- [ ] Spatial query safety enforced
- [ ] Ingestion reproducibility verified

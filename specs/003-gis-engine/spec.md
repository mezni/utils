# Feature Specification: GIS Engine Foundation

**Feature Number**: 003
**Version**: 1.0.0
**Date**: 2026-06-22
**Status**: Planning
**Dependencies**: Sprint 1 (Identity system live)

---

## Overview

Implement a complete GIS engine for BorneMap microservices platform with OSM (OpenStreetMap) ingestion pipeline, PostGIS spatial queries, Redis spatial caching, and map rendering contracts. This foundation supports driver-facing spatial search functionality for finding nearby charging stations.

## User Scenarios

### Scenario 1: Driver Finds Nearby Chargers

**Context**: A driver with location data wants to find charging stations within 10km of their current location.

**Steps**:
1. Driver service receives driver's current GPS coordinates
2. Driver service queries `GET /api/v1/driver/nearby?lat=X&lon=Y&radius=10000`
3. Service executes PostGIS spatial query to find stations within radius
4. Results returned with station details and distance from driver
5. Results cached in Redis for subsequent requests

**Success Criteria**:
- Query returns stations within specified radius
- Distance calculations accurate to 100m tolerance
- Response time < 500ms (without cache), < 50ms (with cache)

### Scenario 2: Mobile App Displays Map

**Context**: A driver opens the mobile app and sees a map with nearby charging stations marked.

**Steps**:
1. Mobile app fetches map tiles and station markers from driver service
2. App receives JSON with station coordinates, connector types, and availability status
3. App displays markers on map with clustering for dense areas
4. User taps a marker to see station details

**Success Criteria**:
- Map renders correctly with station markers
- Clustering reduces visual clutter (>50 markers grouped)
- Marker popups show accurate station information
- App handles network failures gracefully

### Scenario 3: Admin Verifies Station Data

**Context**: An admin wants to verify that OSM data was correctly imported and curated.

**Steps**:
1. Admin calls ingestion API to trigger OSM batch import
2. System processes OSM XML file, extracts charging station data
3. Data normalized from OSM tags to internal schema
4. Staging table populated with raw OSM data
5. Admin reviews and approves stations for curated table
6. ETL pipeline moves approved stations to curated table

**Success Criteria**:
- Ingestion is deterministic and idempotent
- Staging table contains all OSM tags
- Curated table contains only approved stations
- Spatial queries work correctly on curated data

---

## Functional Requirements

### Core GIS Functions

#### FR-GIS-001: OSM Ingestion Pipeline
**Description**: Implement deterministic, idempotent batch import of OSM charging station data.
**Acceptance Criteria**:
- System can process OSM XML files
- Ingestion is deterministic (same input → same output)
- Supports idempotent re-execution (no duplicate data on retry)
- Validates OSM data integrity (required fields present, coordinates valid)
- Logs ingestion events to analytics_db

#### FR-GIS-002: PostGIS Spatial Queries
**Description**: Implement efficient spatial queries for radius search, bounding box, and nearest neighbors.
**Acceptance Criteria**:
- Supports circular radius queries (e.g., "within 10km")
- Supports bounding box queries (e.g., "within rect X1,Y1 to X2,Y2")
- Supports ordering by distance
- Returns results in < 500ms for 10,000 station dataset
- Queries are SQLx compile-time verified

#### FR-GIS-003: Redis Spatial Cache
**Description**: Implement Redis-based caching layer for spatial queries.
**Acceptance Criteria**:
- Cache keys follow naming convention `geo:radius:{lat}:{lon}:{radius}`
- Cache TTL configurable (default: 5 minutes)
- Cache read hits < 50ms
- Cache write hits < 100ms
- Cache isolation: ONLY driver-service can write to Redis
- Cache invalidation on station updates

#### FR-GIS-004: Map Rendering API
**Description**: Define API contracts for map data consumption (web and mobile).
**Acceptance Criteria**:
- Exposes `/api/v1/driver/stations` endpoint (list with pagination)
- Exposes `/api/v1/driver/stations/{id}` endpoint (single station)
- Exposes `/api/v1/driver/nearby` endpoint (spatial search)
- All endpoints return data matching domain-types DTOs
- API responses are versioned

### Data Management

#### FR-GIS-005: GIS Schema with Staging
**Description**: Create PostgreSQL schemas for staging and curated charging station data.
**Acceptance Criteria**:
- `gis.osm_charging_stations_temp` table for raw OSM data
- `gis.osm_charging_stations` table for curated data
- Both tables use PostGIS geometry columns
- Tables include idempotency tracking
- Materialized views for query optimization

#### FR-GIS-006: ETL Pipeline
**Description**: Implement staging → curated pipeline with validation and approval workflow.
**Acceptance Criteria**:
- ETL runs on-demand (triggered by admin)
- Validates OSM tags against business rules
- Normalizes data from OSM tags to internal schema
- Supports approval workflow (admin reviews, approves)
- Only approved stations move to curated table

### Safety & Compliance

#### FR-GIS-007: GIS Ownership Enforcement
**Description**: Ensure ONLY driver-service can write to gis schema.
**Acceptance Criteria**:
- Database roles restrict gis schema write access to driver-service
- CI gate enforces this rule
- Any write attempt outside driver-service fails
- Audit logging for all gis write operations

#### FR-GIS-008: Spatial Query Safety
**Description**: Prevent raw SQL construction in spatial queries.
**Acceptance Criteria**:
- All spatial queries use SQLx compile-time verification
- No dynamic SQL string construction
- Parameterized queries only
- CI gate validates no non-SQLx queries in driver-service

#### FR-GIS-009: OSM Reproducibility
**Description**: Ensure OSM ingestion is reproducible and deterministic.
**Acceptance Criteria**:
- Ingestion produces identical output for identical input
- Uses idempotency keys
- Logs reproducibility events
- CI gate validates idempotency enforcement

---

## Non-Functional Requirements

### Performance

#### NFR-GIS-001: Query Response Time
**Description**: Spatial queries should complete within acceptable time bounds.
**Criteria**:
- Nearby query without cache: < 500ms (10,000 stations)
- Nearby query with cache: < 50ms
- Station detail query: < 100ms
- OSM ingestion: < 10s for 1MB OSM file

#### NFR-GIS-002: Scalability
**Description**: System should support growing dataset size.
**Criteria**:
- Supports 10,000+ charging stations
- Queries scale linearly with dataset size
- Redis cache supports concurrent reads/writes
- PostGIS indices efficient for spatial queries

### Availability

#### NFR-GIS-003: Data Integrity
**Description**: Spatial data must be accurate and consistent.
**Criteria**:
- Distance calculations accurate to 100m tolerance
- Spatial queries return correct results
- No data drift between staging and curated tables
- Ingestion preserves coordinate precision

### Security

#### NFR-GIS-004: Access Control
**Description**: GIS data access controlled by RBAC.
**Criteria**:
- Only authenticated users can query GIS data
- Role-based access to station CRUD operations
- Driver-service authentication via JWT
- Audit logging for all GIS queries

---

## Assumptions

1. **OSM Data Source**: We will use OpenStreetMap's API (https://overpass-api.de) for fetching charging station data, as it provides stable, recent data.
2. **Coordinate System**: All spatial operations use WGS 84 (lat/lon) coordinate system, consistent with GPS and standard mapping applications.
3. **Station Representation**: A charging station is represented as a single point with associated connector types and amenity tags.
4. **Redis Isolation**: Redis is deployed as a singleton cache service exclusively for driver-service spatial queries.
5. **Batch Ingestion**: OSM data will be batch-imported daily via cron job or admin-triggered process, not real-time streaming.
6. **Materialized Views**: We will use PostGIS materialized views for performance optimization on frequently accessed queries.
7. **Mobile Framework**: Mobile app will use Expo SDK 54 for React Native, enabling cross-platform deployment.
8. **Web Framework**: Web map will use React with Leaflet, maintaining consistency with existing frontend architecture.
9. **Contract-First**: API contracts defined in domain-types crate, with implementation following backend then frontend.

---

## Success Criteria

### System-Level Success

**S2-1**: OSM ingestion pipeline is fully functional, deterministic, and idempotent
- Can process real OSM XML files
- Same input produces identical output on re-run
- Supports batch processing (10+ stations per file)

**S2-2**: Spatial queries return accurate results within performance targets
- Radius queries correct within 100m tolerance
- Bounding box queries correct
- Nearest neighbor queries ordered by distance

**S2-3**: Redis cache operational and isolated
- Cache read/write work correctly
- Cache miss rate < 30% for typical queries
- Cache isolation enforced (no other service writes)

**S2-4**: Map rendering API fully functional
- `/nearby` endpoint returns correct results
- `/stations` endpoint supports pagination
- `/stations/{id}` endpoint returns detailed information
- API responses match domain-types contracts

**S2-5**: All CI gates passing
- GIS ownership gate enforced
- Spatial query safety enforced
- Redis access isolated
- OSM ingestion reproducible
- Map API contracts validated

### User-Experience Success

**UX-1**: Driver can find nearby stations within 10km in under 1 second
- Query initiated from mobile app
- Results displayed within 1 second
- Markers appear on map

**UX-2**: Mobile app displays map with 50+ markers, clustering enabled
- Map renders correctly
- Markers positioned at accurate locations
- Clustering groups dense markers
- Popups show station details

**UX-3**: Admin can verify station data integrity
- Ingestion produces consistent results
- Staging and curated tables match
- Spatial queries accurate

---

## Out of Scope (Explicitly Excluded)

1. Real-time OSM streaming (only batch ingestion)
2. 3D visualization or terrain data
3. Charging session management
4. Payment processing integration
5. Multi-language localization
6. Offline map caching on mobile devices
7. Route planning between stations
8. Advanced clustering algorithms (hierarchical, density-based)
9. Geospatial analytics and heatmaps
10. Station maintenance tracking
11. User location tracking and routing

---

## Dependencies

### Internal Dependencies
- Sprint 1: JWT authentication and RBAC system must be complete
- Domain-types crate: API contracts must be defined before implementation

### External Dependencies
- OpenStreetMap (OSM) data source: https://overpass-api.de
- Redis server: 7+ for spatial caching
- PostgreSQL 16+ with PostGIS extension: https://postgis.net

---

## Risks and Mitigations

### Risk R-GIS-1: OSM Data Quality
**Risk**: OSM data may contain inaccuracies, missing fields, or duplicate entries.
**Impact**: High - can lead to incorrect station data in production.
**Mitigation**:
- Implement data validation in ingestion pipeline
- Create admin review and approval workflow
- Monitor ingestion for anomalies
- Use multiple OSM data sources for verification

### Risk R-GIS-2: Spatial Query Performance
**Risk**: Spatial queries may be slow with large datasets.
**Impact**: Medium - affects user experience.
**Mitigation**:
- Implement Redis spatial cache
- Create materialized views for common queries
- Use appropriate PostGIS indexes (GiST, SP-GiST)
- Profile queries and optimize indices

### Risk R-GIS-3: Redis Isolation Violations
**Risk**: Another service may accidentally write to Redis.
**Impact**: High - breaks spatial caching, data corruption.
**Mitigation**:
- CI gate enforces Redis access only in driver-service
- Database roles restrict Redis keys to driver-service
- Audit logging for Redis operations
- Code review checklist item

### Risk R-GIS-4: Materialized View Refresh Complexity
**Risk**: Materialized view refresh may be slow or fail intermittently.
**Impact**: Medium - affects query performance.
**Mitigation**:
- Schedule refreshes during low-traffic periods
- Implement incremental refresh logic
- Monitor view freshness
- Fallback to regular queries if refresh fails

---

## Open Questions

1. **Data Refresh Frequency**: How often should OSM data be ingested? (Daily, hourly, or triggered by admin?)
2. **Station Approval Workflow**: Manual approval by admin, or automatic based on OSM quality flags?
3. **Cache Size Limits**: What should be the maximum size of Redis spatial cache? What happens when limit reached?
4. **Spatial Query Timeout**: What timeout should we use for spatial queries (longer for large radius)?
5. **Coordinate Precision**: Should we use double precision or fixed precision (e.g., 6 decimal places) for coordinates?
6. **Marker Density Threshold**: What is the threshold for automatic clustering (e.g., 30 markers)?
7. **Mobile Map Rendering**: Should we use Mapbox GL JS, Google Maps, or a different library for mobile map?

---

## Test Strategy

### Unit Tests
- OSM tag normalization logic
- Spatial query builder
- Redis cache read/write
- Data validation and sanitization

### Integration Tests
- OSM ingestion → staging → curated pipeline
- Spatial queries with PostGIS
- Redis cache hit/miss scenarios
- CI gates validation

### End-to-End Tests
- Driver searches nearby stations
- Mobile app displays map with markers
- Admin triggers ingestion and verifies results

### Performance Tests
- Query response time benchmarks
- Redis cache hit/miss rates
- Concurrency testing
- Data ingestion throughput

---

## Glossary

- **OSM**: OpenStreetMap - open-source geographic data
- **PostGIS**: PostgreSQL extension for spatial data
- **GiST**: Generalized Search Tree - PostGIS index type
- **SP-GiST**: Sparse Generalized Search Tree - PostGIS index type
- **Staging Table**: Temporary table for raw OSM data before review
- **Curated Table**: Final, approved table with normalized data
- **ETL**: Extract, Transform, Load - data migration process
- **Materialized View**: Pre-computed query result stored physically
- **Spatial Index**: Database index optimized for spatial queries
- **Idempotency Key**: Unique identifier to ensure operations can be repeated safely
- **Bounding Box**: Rectangle defined by two coordinates (minX, minY) and (maxX, maxY)
- **Radius Search**: Find points within a circular distance from a center point
- **Clustering**: Grouping nearby markers to reduce visual clutter
- **Coordinate System**: Reference system for defining geographic positions (WGS 84)

---

## References

- OpenStreetMap API: https://overpass-api.de/
- PostGIS Documentation: https://postgis.net/documentation/
- Redis Spatial Extensions: https://redis.io/commands/spatial/
- GIS Best Practices: https://developers.google.com/maps/documentation/geojson

# GIS Data & Nearby Discovery — MVP-2 Sprint 2.0

**Version**: 1.0
**Status**: Ready for Implementation
**Last Updated**: 2026-06-16

## 1. Overview

This implementation plan details how to build the GIS data layer for the BorneMap platform. The system will fetch charging station data from OpenStreetMap for the Tunisia region, store it in a spatial database, and provide a nearby discovery API for drivers. Map markers will be displayed on both mobile and web driver apps.

## 2. Technical Context

### 2.1 Stack

| Component | Technology | Notes |
|-----------|-----------|-------|
| Backend Service | Driver Service (Rust + Actix-web) | Existing service from MVP-1 |
| Database | PostGIS (Postgres) | Existing `platform_db` from MVP-1 |
| Import Process | Docker container | Separate from services |
| Mobile App | React Native (Expo SDK 54) | Existing from MVP-1 |
| Web App | React (Vite) | Existing from MVP-1 |

### 2.2 Environment

- Database URL: `${PLATFORM_DB_URL}` from `.env`
- Import process runs as docker-compose service with `import` profile
- API endpoint: `http://driver-service:3001/api/v1/nearby`

### 2.3 Dependencies

- PostGIS (spatial extensions)
- OpenStreetMap public API (data source)
- Existing services from MVP-1 (platform_db, driver-service)

### 2.4 Integration Points

- **Database**: `platform_db` (PostGIS) - `inventory.station` table
- **Driver Service**: Backend API for nearby queries
- **Mobile/Web Apps**: Map display with markers

## 3. Constitution Check

### 3.1 Project Principles

1. **Modularity**: Services communicate via well-defined APIs
   - ✅ Driver Service exposes `/api/v1/nearby` endpoint
   - ✅ Import process runs independently but uses same database

2. **Separation of Concerns**: Each service has a single responsibility
   - ✅ Import process handles data ingestion only
   - ✅ Driver Service handles spatial queries and API responses

3. **Security First**: Authentication and authorization are mandatory
   - ✅ Discovery API requires JWT token (FR8)
   - ✅ Per-user rate limiting enforced (FR14)
   - ✅ Authentication header validation (FR16)

4. **Data Quality**: Accurate and complete data
   - ✅ Import process supports re-running (FR4)
   - ✅ JSON format for structured data (FR2)
   - ✅ Concurrent import prevention (FR5)

### 3.2 Design Gates

- ✅ **Authentication Required**: API requires JWT token (confirmed)
- ✅ **Rate Limiting**: Per-user limits implemented (confirmed)
- ✅ **Error Handling**: Specific error codes defined (confirmed)
- ✅ **Data Import Safety**: Database locks prevent corruption (confirmed)

## 4. Phase 0: Research

### 4.1 Open Questions Resolved

**Question**: Best approach for spatial queries in PostGIS?
**Decision**: Use `ST_DWithin` function on geography type for automatic meter calculations
**Rationale**: Geography type uses meters natively, no unit conversion needed. STABLE function allows query planning optimization.
**Alternatives Considered**:
- Geometry type: Requires manual unit conversion (not recommended)
- Cartesian operations: Less accurate for spherical coordinates

**Question**: How to handle concurrent import processes?
**Decision**: Use PostGIS advisory locks (SQL `SELECT pg_advisory_xact_lock()`)
**Rationale**: Simple, reliable, database-level locking. Transaction-safe by default.
**Alternatives Considered**:
- File locks: Race conditions across containers
- Application-level locks: Complex synchronization logic
- Queue system: Overkill for single import process

**Question**: How to fetch OSM data efficiently?
**Decision**: Overpass API with bounding box query, fetch 5km grid chunks for Tunisia (30.0-37.5, 7.5-11.6)
**Rationale**: Bounding box is smaller than full country, reducing response size. Chunking ensures timeout handling.
**Alternatives Considered**:
- Full country query: Too slow, exceeds timeout
- Single query: All-or-nothing, hard to retry

### 4.2 Best Practices

**Data Import**:
- Use JSON for structure (already confirmed)
- Upsert on OSM ID to support re-runs
- Use database transactions for data integrity

**Spatial Queries**:
- Index `location` with GIST index for performance
- Use geography type for meter-based calculations
- Only return active stations (`status = 'active'`)

**API Design**:
- Require JWT authentication
- Enforce rate limiting (100 queries/minute)
- Return 400/401/500 with specific messages

**Error Handling**:
- Client errors (400): Invalid coordinates, limits exceeded
- Auth errors (401): Missing/invalid token
- Server errors (500): Internal issues (generic message)

### 4.3 Integration Patterns

**Driver Service**:
- Existing service from MVP-1, add new endpoint
- Use sqlx for database queries
- Use existing JWT middleware from MVP-2

**Import Process**:
- Separate Docker container
- Build Python/Node script
- Mount volume for logs
- Clean shutdown on failure

**Mobile/Web Apps**:
- Use existing map components from MVP-1
- Add marker rendering logic
- Implement debouncing for map panning

## 5. Phase 1: Design & Contracts

### 5.1 Data Model

#### Entity: Station (inventory.station)

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | VARCHAR(32) | PK, NOT NULL | Unique identifier (e.g., "sta_xxxxx") |
| `name` | VARCHAR(255) | NOT NULL | Station name |
| `visibility` | VARCHAR(50) | NOT NULL | 'commercial', 'private_home', 'all' |
| `status` | VARCHAR(50) | NOT NULL, DEFAULT 'draft' | 'draft', 'active', 'inactive', 'closed' |
| `location` | GEOGRAPHY(POINT, 4326) | NOT NULL | Lat/lon coordinates |
| `address` | TEXT | NULL | Street address |
| `city` | VARCHAR(100) | NULL | City name |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation |
| `updated_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update |
| `deleted_at` | TIMESTAMPTZ | NULL | Soft delete marker |

**Indexes**:
- `idx_station_location`: GIST on `location`
- `idx_station_status`: btree on `status`
- `idx_station_visibility`: btree on `visibility`

**State Transitions**:
- draft → active (when import complete)
- active → inactive (manual or automated)
- active → closed (manual)
- any → draft (rollback)

#### Entity: Charger (inventory.charger)

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | VARCHAR(32) | PK, NOT NULL | Unique identifier |
| `station_id` | VARCHAR(32) | FK → inventory.station(id) | Reference to parent station |
| `connector_type` | VARCHAR(50) | NOT NULL | 'type1', 'type2', 'ccs', 'chademo', 'other' |
| `connector_count` | INTEGER | NOT NULL, DEFAULT 1 | Number of connectors |
| `power_kw` | DECIMAL(5,2) | NOT NULL | Power rating |
| `status` | VARCHAR(50) | NOT NULL, DEFAULT 'available' | 'available', 'occupied', 'unavailable' |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation |
| `updated_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update |
| `deleted_at` | TIMESTAMPTZ | NULL | Soft delete marker |

**Indexes**:
- `idx_charger_station`: btree on `station_id`
- `idx_charger_status`: btree on `status`

#### Entity: Import Log (gis.import_log)

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | SERIAL | PK | Auto-increment ID |
| `status` | VARCHAR(50) | NOT NULL | 'success', 'failed' |
| `start_time` | TIMESTAMPTZ | NOT NULL | Import started |
| `end_time` | TIMESTAMPTZ | NULL | Import completed |
| `stations_imported` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations processed |
| `stations_updated` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations updated |
| `stations_failed` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations that failed |
| `error_message` | TEXT | NULL | Error details if failed |

**Indexes**:
- `idx_import_log_time`: btree on `start_time` DESC

### 5.2 API Contracts

#### Endpoint: GET /api/v1/nearby

**Purpose**: Retrieve nearby charging stations within specified radius

**Authentication**: Required (JWT token)

**Query Parameters**:

| Parameter | Type | Required | Default | Constraints |
|-----------|------|----------|---------|-------------|
| `lat` | float | Yes | — | -90 to 90 |
| `lon` | float | Yes | — | -180 to 180 |
| `radius_m` | integer | No | 5000 | 1–50000 |
| `max_results` | integer | No | 50 | 1–100 |
| `visibility` | string | No | all | 'commercial', 'private_home', 'all' |

**Request Example**:
```
GET /api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000
Authorization: Bearer <jwt_token>
```

**Response 200 (Success)**:

```json
{
  "stations": [
    {
      "id": "sta_xxxxx",
      "name": "Station Menzah",
      "location": {
        "lat": 36.84,
        "lon": 10.19
      },
      "address": "Rue des Jasmins, Menzah",
      "city": "Tunis",
      "distance_m": 1240,
      "visibility": "commercial",
      "status": "active",
      "chargers": [
        {
          "id": "chg_xxxxx",
          "connector_type": "type2",
          "connector_count": 2,
          "power_kw": 22.0,
          "status": "available"
        }
      ]
    }
  ],
  "count": 1,
  "radius_m": 5000
}
```

**Error 400 (Bad Request)** - Invalid coordinates:
```json
{
  "error": {
    "code": "GEO_001",
    "message": "Coordinates must be within valid geographic ranges (lat: -90 to 90, lon: -180 to 180)",
    "field": "coordinates"
  }
}
```

**Error 400 (Bad Request)** - Radius exceeded:
```json
{
  "error": {
    "code": "GEO_002",
    "message": "Radius must be between 1 and 50000 meters",
    "field": "radius_m"
  }
}
```

**Error 401 (Unauthorized)** - Missing/Invalid Token:
```json
{
  "error": {
    "code": "AUTH_001",
    "message": "Missing or invalid authorization header. Please provide a valid JWT token."
  }
}
```

**Error 429 (Too Many Requests)** - Rate limit exceeded:
```json
{
  "error": {
    "code": "RATE_001",
    "message": "Too many requests. Maximum 100 queries per minute."
  }
}
```

**Error 500 (Internal Server Error)**:
```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "An error occurred while processing your request. Please try again later."
  }
}
```

**Error Codes**:

| Code | HTTP | Scenario | Description |
|------|------|----------|-------------|
| `GEO_001` | 400 | Invalid coordinates | Lat/lon out of valid range |
| `GEO_002` | 400 | Radius exceeded | radius_m > 50000 |
| `GEO_003` | 400 | Max results exceeded | max_results > 100 |
| `AUTH_001` | 401 | Missing/invalid auth | No or invalid JWT token |
| `RATE_001` | 429 | Rate limit exceeded | > 100 queries/minute |
| `INTERNAL_ERROR` | 500 | Server error | Database/connection issues |

#### Endpoint: POST /api/v1/import (Import Process)

**Purpose**: Trigger import of charging station data from OpenStreetMap

**Authentication**: Required (admin role)

**Headers**:
```
Content-Type: application/json
Authorization: Bearer <admin_jwt_token>
```

**Request Body**:
```json
{
  "bbox": {
    "min_lat": 30.0,
    "min_lon": 7.5,
    "max_lat": 37.5,
    "max_lon": 11.6
  }
}
```

**Response 200 (Success)**:
```json
{
  "status": "success",
  "stations_imported": 1250,
  "stations_updated": 340,
  "stations_failed": 0,
  "start_time": "2026-06-16T15:30:00Z",
  "end_time": "2026-06-16T15:45:30Z"
}
```

**Response 400 (Bad Request)**:
```json
{
  "error": {
    "code": "IMPORT_001",
    "message": "Invalid bounding box parameters"
  }
}
```

**Error 401 (Unauthorized)**: Invalid admin token
**Error 500 (Internal Server Error)**: Import process failure

### 5.3 Quickstart Guide

#### Prerequisites

1. Docker Compose v2 installed
2. Local environment with `.env` file configured
3. Access to internet (for OSM API calls)

#### Steps

1. **Start Databases**:
```bash
docker compose --profile infra up -d platform-db
```

2. **Create Required Schemas**:
```bash
# In postgres container
psql -U bornemap -d bornemap -c "CREATE SCHEMA IF NOT EXISTS inventory;"
psql -U bornemap -d bornemap -c "CREATE SCHEMA IF NOT EXISTS gis;"
```

3. **Start Services**:
```bash
docker compose --profile infra --profile services up -d
```

4. **Run Import Process**:
```bash
docker compose --profile import up osm-importer
```

   Expected output:
   ```
   Fetching charging station data for Tunisia...
   Imported 1250 stations
   Updated 340 existing stations
   Import completed successfully
   ```

5. **Verify Data**:
```bash
docker compose exec platform-db psql -U bornemap -d bornemap -c "
  SELECT COUNT(*) FROM inventory.station;
  SELECT * FROM inventory.station LIMIT 3;
"
```

6. **Test Nearby API**:
```bash
curl -X GET "http://localhost:3001/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

7. **Start Driver Apps**:
```bash
cd apps/mobile-driver
npx expo start

cd apps/web-driver
npm run dev
```

#### Development Workflow

**Modifying Import Process**:
```bash
# Make changes to infra/osm-importer/
docker compose --profile import up --build osm-importer
```

**Testing Spatial Queries**:
```bash
# In postgres container
psql -U bornemap -d bornemap -c "
  SELECT id, name, ST_Distance(
    location,
    ST_GeogFromText('SRID=4326;POINT(10.18 36.8)')
  ) AS distance_m
  FROM inventory.station
  WHERE ST_DWithin(
    location,
    ST_GeogFromText('SRID=4326;POINT(10.18 36.8)'),
    5000
  )
  ORDER BY distance_m ASC
  LIMIT 10;
"
```

**Reset Database**:
```bash
docker compose exec platform-db psql -U bornemap -d bornemap -c "DROP SCHEMA inventory CASCADE;"
docker compose exec platform-db psql -U bornemap -d bornemap -c "CREATE SCHEMA inventory;"
# Re-run import
docker compose --profile import up osm-importer
```

## 6. Implementation Phases

### Phase 1: Database Setup (Day 1-2)

- [ ] Create inventory.schema.sql
- [ ] Create gis.nearby() function
- [ ] Create import_log table
- [ ] Add spatial indexes
- [ ] Manual testing of SQL queries

### Phase 2: Data Import Process (Day 3-5)

- [ ] Scaffold osm-importer directory
- [ ] Create Dockerfile
- [ ] Implement OSM API fetcher
- [ ] Implement JSON transformer
- [ ] Implement database upsert logic
- [ ] Add concurrent import prevention
- [ ] Add import logging
- [ ] Create docker-compose import service
- [ ] Test import process end-to-end

### Phase 3: Driver Service API (Day 6-9)

- [ ] Create nearby endpoint handler
- [ ] Implement coordinate validation (GEO_001)
- [ ] Implement rate limiting (RATE_001)
- [ ] Implement pagination
- [ ] Add authentication middleware
- [ ] Add error handling (GEO_002, GEO_003)
- [ ] Create charger query logic
- [ ] Test all error scenarios

### Phase 4: Mobile App Integration (Day 10-13)

- [ ] Create shared types (Station, Charger, NearbyResponse)
- [ ] Create API client function
- [ ] Create useNearby hook
- [ ] Implement marker rendering
- [ ] Add marker clustering
- [ ] Add loading states
- [ ] Add error states
- [ ] Test on device simulator

### Phase 5: Web App Integration (Day 14-15)

- [ ] Same as mobile app (shared code)
- [ ] Test on browser
- [ ] Verify clustering performance

### Phase 6: Testing & Documentation (Day 16-18)

- [ ] Unit tests for spatial queries
- [ ] Integration tests for API endpoints
- [ ] Manual testing of import process
- [ ] Documentation updates
- [ ] Performance tuning

## 7. Success Criteria Validation

| Criterion | Implementation | Verification |
|-----------|----------------|--------------|
| Import fetches Tunisia data | OSM API with bounding box | Query database row count |
| Spatial query returns results | gis.nearby() with ST_DWithin | API test with known coords |
| API returns paginated stations | LIMIT/OFFSET in query | Verify response structure |
| Mobile markers render | useNearby hook + markers | Visual inspection |
| Web markers cluster | marker clustering library | Visual inspection |
| API returns empty array | No stations in radius | Query area with no stations |

## 8. Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| OSM data incomplete | Medium | High | Multiple fetch passes, manual verification |
| Import process slow | Medium | Medium | Incremental updates, optimize queries |
| Spatial index performance | Low | Low | Monitor query times, add indexes |
| Rate limiting implementation | Low | Medium | Test edge cases, use established libraries |
| Marker clustering bugs | Low | Low | Use proven libraries (react-native-maps clustering) |

## 9. Open Questions

None. All clarifications resolved in `/speckit.clarify`.

## 10. Dependencies & Blocking Items

- MVP-1 infrastructure (platform_db with PostGIS) ✅ Complete
- JWT authentication setup (MVP-2 shared code) ✅ Complete
- Driver Service existing codebase ✅ Available

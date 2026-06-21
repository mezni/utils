# Contract: driver-service

**Service**: driver-service
**Port**: 3001
**Responsible for**: GIS, telemetry ingestion, analytics write, nearby search

## Overview

driver-service is the data-intensive service that manages GIS data, ingests telemetry events, writes to analytics_db, and provides nearby search functionality.

## API Endpoints

### Operational Endpoints

All services MUST support these operational endpoints:

#### Health Check

**Endpoint**: `GET /health`

**Response**:
```json
{
  "status": "ok",
  "timestamp": "2026-06-21T12:00:00Z",
  "service": "driver-service"
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0. Will be fully implemented in later sprints.

---

### User APIs

#### Get User Profile

**Endpoint**: `GET /api/v1/users/me`

**Response**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe"
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

### GIS APIs

#### Get Nearby Charging Stations

**Endpoint**: `GET /api/v1/gis/nearby`

**Query Parameters**:
- `latitude` (required): Latitude coordinate
- `longitude` (required): Longitude coordinate
- `radius_km` (optional, default: 10): Search radius in kilometers

**Response**:
```json
{
  "stations": [
    {
      "id": "STA123456789012",
      "name": "Station Alpha",
      "address": "123 Main St",
      "city": "Tunis",
      "province": "Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "is_test": false,
      "chargers_count": 5
    }
  ],
  "count": 1
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

### Telemetry APIs

#### Ingest Telemetry Events (Single)

**Endpoint**: `POST /api/v1/telemetry/events`

**Request Body**:
```json
{
  "schema_version": "1.0.0",
  "idempotency_key": "unique-event-id",
  "event": {
    "type": "charging_start",
    "station_id": "STA123456789012",
    "charger_id": "CHG123456789012",
    "operator_id": "OPR123456789012",
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-06-21T12:00:00Z",
    "data": {
      "energy_kwh": 15.5,
      "port_type": "Type 2"
    }
  }
}
```

**Response**:
```json
{
  "status": "accepted",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Event ingested successfully"
}
```

**Status**: 202 Accepted

**Implementation Note**: Stub implementation for Sprint 0.

---

### Analytics APIs (Read-Only)

#### Get Analytics Summary

**Endpoint**: `GET /api/v1/analytics/summary`

**Response**:
```json
{
  "total_events": 1000,
  "total_energy_kwh": 15000.50,
  "active_stations": 25,
  "date_range": {
    "start": "2026-06-01",
    "end": "2026-06-21"
  }
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

## Service Responsibilities

### Owned APIs

- GIS data queries
- Nearby search
- Telemetry event ingestion
- Analytics read operations

### Operational APIs

- `/health` — Service health check
- `/ready` — Service readiness check
- `/live` — Service liveness check
- `/metrics` — Prometheus metrics (optional)

### READ-ONLY Analytics

- driver-service can READ from analytics_db
- driver-service CANNOT WRITE to analytics_db (enforced by analytics gate)
- admin-service also READS from analytics_db

## Data Access

### Owned Databases

- `platform_db.gis` — GIS data (READ/WRITE, exclusive)
- `analytics_db` — Telemetry and analytics events (WRITE, exclusive)

### NO Direct Access to Other Services

- driver-service CANNOT access platform_db.inventory
- driver-service CANNOT access platform_db.users
- Cross-service data access forbidden

### Keycloak Integration

**Database**: keycloak_db (read-only for application)

**Access Pattern**:
- User authentication via Keycloak (for context in events)
- No direct database queries to keycloak_db
- External API calls to Keycloak for user lookup

## Event System

### Event Ingestion (Driver-Service Only)

**Endpoint**: `POST /api/v1/telemetry/events`

**Event Schema**:
- **Source**: External systems (chargers, user apps)
- **Destination**: analytics_db.telemetry_events
- **Ownership**: driver-service exclusive
- **Validation**: Schema version, idempotency, replay safety
- **Deduplication**: driver-service MUST deduplicate events

**Event Format**:
```json
{
  "schema_version": "1.0.0",
  "idempotency_key": "uuid-or-hash",
  "event": {
    "type": "charging_start|charging_complete|charging_failed",
    "station_id": "STA123456789012",
    "charger_id": "CHG123456789012",
    "operator_id": "OPR123456789012",
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-06-21T12:00:00Z",
    "data": { /* event-specific data */ }
  }
}
```

**Constraints**:
- Schema versioning required
- Idempotency keys prevent duplicates
- Events must be replay-safe
- Deduplication at driver-service level

## Dependencies

### Frontend Dependencies

- `client-core` — Transport layer for API calls
- `domain-types` — Telemetry DTOs and event schemas

### Backend Dependencies

- `shared-infra` — Logging, configuration, HTTP server (via shared crates)
- `shared-domain` — Core entity types and DTOs

### External Dependencies

- PostgreSQL (platform_db.gis, analytics_db)
- Keycloak (for user context lookup, optional)
- Geospatial libraries (for nearby search)

## Constraints

### Identity System

- Users identified by UUID (Keycloak sub)
- Entities identified by nanoid(12) with PREFIX (STA/CHG/OPR/EVT)
- Event `user_uuid` must be UUID
- Event `operator_id` must be OPR-xxxx format
- Event `station_id` must be STA-xxxx format
- Event `charger_id` must be CHG-xxxx format

### Data Ownership

- platform_db.gis owned by driver-service (READ/WRITE)
- analytics_db owned by driver-service (WRITE), admin-service (READ ONLY)
- Cross-service writes forbidden

### Analytics Write Gate

- driver-service IS the only writer to analytics_db
- CI enforcement via 03_validate_analytics_gate.sh
- Static analysis checks for write operations
- Database-level permissions enforced

### Contract-First

- DTOs defined in `domain-types` crate
- No runtime logic in domain-types
- API contracts defined before implementation

### SQLx Compile-Time

- All SQL queries compile-time verified
- No runtime SQL string construction
- No dynamic query generation

## Migration Isolation

### Schema Ownership

- driver-service owns: `platform_db.gis`, `analytics_db`
- Migration files:
  - `migrations/0001_init_gis.up.sql`
  - `migrations/0002_init_analytics.up.sql`
  - `migrations/0003_create_analytics_indexes.up.sql`

### Migration Rules

- Forward-only migrations
- No destructive rollback
- SQLx compile-time verification required
- CI validation required

## Future Implementation

### Sprint 0 (Current)

- Stub health endpoint
- Service skeleton creation
- Basic configuration
- Database schema creation

### Future Sprints

- Full telemetry ingestion pipeline
- Nearby search algorithm
- GIS query optimization
- Analytics dashboard APIs
- Geospatial indexing
- Event deduplication
- Error handling and retry logic

---

**Contract Version**: 1.0.0
**Last Updated**: 2026-06-21
**Status**: Draft (Sprint 0)
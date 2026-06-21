# Contract: admin-service

**Service**: admin-service
**Port**: 3002
**Responsible for**: Inventory management, analytics read, dashboards

## Overview

admin-service provides inventory management APIs, analytics read operations, and dashboard functionality. It reads from platform_db.inventory and analytics_db but does not write to either.

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
  "service": "admin-service"
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0. Will be fully implemented in later sprints.

---

### Inventory APIs

#### Get Charging Stations

**Endpoint**: `GET /api/v1/inventory/stations`

**Query Parameters**:
- `page` (optional, default: 1): Page number
- `page_size` (optional, default: 20): Items per page
- `city` (optional): Filter by city
- `province` (optional): Filter by province
- `is_test` (optional): Filter by test flag

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
  "page": 1,
  "page_size": 20,
  "total": 100
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

#### Get Station Details

**Endpoint**: `GET /api/v1/inventory/stations/:station_id`

**Response**:
```json
{
  "id": "STA123456789012",
  "name": "Station Alpha",
  "address": "123 Main St",
  "city": "Tunis",
  "province": "Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "is_test": false,
  "chargers": [
    {
      "id": "CHG123456789012",
      "port_type": "Type 2",
      "power_kw": 11.0,
      "status": "available"
    }
  ]
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

#### Get Chargers by Station

**Endpoint**: `GET /api/v1/inventory/stations/:station_id/chargers`

**Response**:
```json
{
  "station_id": "STA123456789012",
  "station_name": "Station Alpha",
  "chargers": [
    {
      "id": "CHG123456789012",
      "port_type": "Type 2",
      "power_kw": 11.0,
      "status": "available"
    }
  ],
  "count": 5
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

#### Get Operators

**Endpoint**: `GET /api/v1/inventory/operators`

**Query Parameters**:
- `page` (optional, default: 1): Page number
- `page_size` (optional, default: 20): Items per page
- `is_active` (optional): Filter by active flag
- `search` (optional): Search in name

**Response**:
```json
{
  "operators": [
    {
      "id": "OPR123456789012",
      "name": "Tunisie Energie",
      "contact_email": "contact@tunisieenergie.tn",
      "contact_phone": "+216 71 123 456",
      "is_active": true
    }
  ],
  "page": 1,
  "page_size": 20,
  "total": 10
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

#### Get Operator Details

**Endpoint**: `GET /api/v1/inventory/operators/:operator_id`

**Response**:
```json
{
  "id": "OPR123456789012",
  "name": "Tunisie Energie",
  "contact_email": "contact@tunisieenergie.tn",
  "contact_phone": "+216 71 123 456",
  "is_active": true,
  "stations_count": 5,
  "chargers_count": 25
}
```

**Status**: 200 OK

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

#### Get Analytics Events

**Endpoint**: `GET /api/v1/analytics/events`

**Query Parameters**:
- `page` (optional, default: 1): Page number
- `page_size` (optional, default: 20): Items per page
- `station_id` (optional): Filter by station
- `operator_id` (optional): Filter by operator
- `start_date` (optional): Filter by start date
- `end_date` (optional): Filter by end date
- `status` (optional): Filter by status

**Response**:
```json
{
  "events": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "created_at": "2026-06-21T12:00:00Z",
      "station_id": "STA123456789012",
      "operator_id": "OPR123456789012",
      "start_time": "2026-06-21T12:00:00Z",
      "end_time": "2026-06-21T12:30:00Z",
      "energy_used_kwh": 15.5,
      "status": "completed",
      "payload": { /* additional data */ }
    }
  ],
  "page": 1,
  "page_size": 20,
  "total": 1000
}
```

**Status**: 200 OK

**Implementation Note**: Stub implementation for Sprint 0.

---

#### Get Station Analytics

**Endpoint**: `GET /api/v1/analytics/stations/:station_id`

**Response**:
```json
{
  "station_id": "STA123456789012",
  "station_name": "Station Alpha",
  "total_events": 100,
  "total_energy_kwh": 1500.0,
  "average_energy_kwh": 15.0,
  "status_distribution": {
    "completed": 80,
    "failed": 10,
    "ongoing": 10
  },
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

- Inventory CRUD (Read-only)
- Analytics read operations
- Dashboard queries

### Operational APIs

- `/health` — Service health check
- `/ready` — Service readiness check
- `/live` — Service liveness check
- `/metrics` — Prometheus metrics (optional)

### READ-ONLY Access

- admin-service CAN READ from platform_db.inventory
- admin-service CAN READ from analytics_db
- admin-service CANNOT WRITE to platform_db.inventory
- admin-service CANNOT WRITE to analytics_db
- Cross-service writes forbidden

## Data Access

### Owned Databases

- `platform_db.inventory` — Inventory data (READ/WRITE, exclusive)
- `analytics_db` — Telemetry and analytics events (READ ONLY)

### NO Direct Access to Other Services

- admin-service CANNOT access platform_db.gis
- admin-service CANNOT access platform_db.users
- admin-service CANNOT access keycloak_db
- Cross-service data access forbidden

### Keycloak Integration

**Database**: keycloak_db (read-only for application)

**Access Pattern**:
- No direct database queries to keycloak_db
- No Keycloak integration in admin-service
- User identification handled by driver-service or client

## Event System

### Event Consumption

None (admin-service does not consume events)

### Event Production

None (admin-service does not produce events)

## Dependencies

### Frontend Dependencies

- `client-core` — Transport layer for API calls
- `domain-types` — Inventory DTOs and event schemas

### Backend Dependencies

- `shared-infra` — Logging, configuration, HTTP server (via shared crates)
- `shared-domain` — Core entity types and DTOs

### External Dependencies

- PostgreSQL (platform_db.inventory, analytics_db)
- No Keycloak integration (for MVP)

## Constraints

### Identity System

- Users identified by UUID (Keycloak sub)
- Entities identified by nanoid(12) with PREFIX (STA/CHG/OPR/EVT)
- No mixing of identity systems

### Data Ownership

- platform_db.inventory owned by admin-service (READ/WRITE)
- analytics_db owned by driver-service (WRITE), admin-service (READ ONLY)
- Cross-service writes forbidden

### Analytics Read-Only

- admin-service can READ from analytics_db
- admin-service CANNOT write to analytics_db (enforced by analytics gate)
- CI enforcement via 03_validate_analytics_gate.sh

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

- admin-service owns: `platform_db.inventory`
- Migration file: `migrations/0001_init_inventory.up.sql`

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

- Full inventory CRUD APIs
- Analytics dashboard APIs
- Real-time dashboards
- Inventory search and filtering
- Export capabilities
- Multi-language support

---

**Contract Version**: 1.0.0
**Last Updated**: 2026-06-21
**Status**: Draft (Sprint 0)
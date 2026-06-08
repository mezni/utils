# Data Model: API Versioning

**Phase**: 1 (Design & Contracts)  
**Feature**: API Versioning (`001-backend-and-database`)  
**Created**: 2026-06-08

## Overview

API versioning is primarily a **routing and contract mechanism**, not a data model change. The database schema remains unchanged. Version identity is managed in the API layer via URL routing.

---

## Core Entities

### APIVersion

**Purpose**: Represents a discrete API contract generation. Each version is immutable once released.

**Identity**: 
- `version_number`: Integer (1, 2, 3, ...). Unique identifier within service.
- `service_name`: String (e.g., "bornemap-service", "driver-service"). Identifies which backend service this version belongs to.
- **Natural key**: `(service_name, version_number)`

**Lifecycle**:
1. **Active**: Currently supported; new clients may call it
2. **Deprecated**: Successor released; existing clients supported for 12 months
3. **Retired**: Support window ended; 410 Gone response returned

**Attributes** (metadata only; not persisted in database):
- `released_date`: ISO 8601 timestamp (e.g., 2026-06-08T00:00:00Z)
- `deprecated_date`: ISO 8601 timestamp (when successor released)
- `retirement_date`: ISO 8601 timestamp (deprecated_date + 12 months)
- `breaking_changes`: Text description of what changed from previous version
- `migration_guide`: URL to documentation for upgrading clients

**Example**:
```
APIVersion(version_number=1, service_name="bornemap-service")
  released_date: 2026-06-08
  deprecated_date: null (active)
  retirement_date: null
  breaking_changes: "Initial release. All endpoints versioned at /api/v1/."
  migration_guide: "/docs/api/v1-to-v2-migration"

APIVersion(version_number=2, service_name="bornemap-service")
  released_date: 2026-12-01 (estimated, MVP-2)
  deprecated_date: null (active when released)
  retirement_date: 2027-12-01 (12 months after v2)
  breaking_changes: "Rust backend; new fields on station/charger; removed deprecated fields"
  migration_guide: "/docs/api/v1-to-v2-migration"
```

---

### APIContract

**Purpose**: Describes the immutable request/response schema for a specific version of an endpoint.

**Identity**:
- `endpoint`: String (e.g., "GET /stations", "POST /partners")
- `api_version`: Reference to `APIVersion`
- **Natural key**: `(endpoint, api_version)`

**Attributes** (metadata only):
- `method`: HTTP verb (GET, POST, PUT, DELETE)
- `path_pattern`: URL pattern (e.g., `/api/v1/stations/{id}`)
- `request_schema`: JSON Schema for request body (nullable for GET)
- `response_schema`: JSON Schema for 200 OK response
- `error_responses`: Map of HTTP status → error schema (404, 422, etc.)
- `description`: Text describing what the endpoint does
- `deprecated`: Boolean. True if this endpoint is superseded in next version.

**Example**:
```
APIContract(endpoint="GET /stations", api_version=APIVersion(1, "bornemap-service"))
  method: GET
  path_pattern: /api/v1/stations
  request_schema: { "type": "object", "properties": { "partner_id": { "type": "string", "format": "uuid" } } }
  response_schema: { "type": "array", "items": { "type": "object", "properties": { "id": "uuid", "name": "string", "charger_count": "integer" } } }
  error_responses: { 404: { "type": "object", "properties": { "detail": "string" } } }
  description: "List all stations, optionally filtered by partner"
  deprecated: false
```

---

## Relationships

```
APIVersion (1) ──── (N) APIContract
  v1              ├─ GET /stations
                  ├─ POST /stations
                  ├─ GET /stations/{id}
                  ├─ PUT /stations/{id}
                  ├─ DELETE /stations/{id}
                  ├─ GET /partners
                  ├─ POST /partners
                  ├─ GET /partners/{id}
                  ├─ PUT /partners/{id}
                  ├─ DELETE /partners/{id}
                  ├─ GET /chargers
                  ├─ POST /chargers
                  ├─ GET /chargers/{id}
                  ├─ PUT /chargers/{id}
                  ├─ DELETE /chargers/{id}
                  └─ GET /health (versioned)
```

---

## Schema Stability Rules

### v1 Contract (Sprint 1.1)

v1 contracts are **locked** and immutable:
- No new fields added to responses
- No required fields removed from requests
- No endpoint URLs change
- Response status codes remain the same
- Error messages may improve but meanings unchanged

**Exception**: Bug fixes to return data (e.g., incorrect charger count) are allowed.

### v2+ Contracts (MVP-2 onward)

Each new version may introduce breaking changes:
- New required fields in request/response
- Removed or renamed fields
- New endpoints
- Deprecated endpoints (marked in docs)
- Changed error codes

v1 remains unchanged; v2 clients see v2 schema; v1 clients continue to see v1 schema.

---

## Routing & Runtime Behavior

### Request Flow

```
Client Request:
  GET /api/v1/stations

↓ (FastAPI middleware / URL dispatcher)

Identify Version:
  Extract version number (1) from URL path
  Fetch APIVersion contract for (bornemap-service, version=1)

↓ Route Resolution

Dispatch to Handler:
  FastAPI router finds v1.stations.list_stations()
  Handler returns 200 OK with v1 schema

Response:
  {
    "data": [...stations...],
    "count": 15
  }
  (No "version" field in body)
```

### Invalid Version Request

```
Client Request:
  GET /api/v999/stations

↓ (URL dispatcher / 404 handler)

No contract found for APIVersion(v999)

Response:
  HTTP 404 Not Found
  {
    "detail": "API version v999 not found. Available versions: v1. See /api/docs for details."
  }
```

### Unversioned Request (Explicitly Forbidden)

```
Client Request:
  GET /api/stations

↓ (URL dispatcher)

No route matches /api/stations (no version prefix)

Response:
  HTTP 404 Not Found
  {
    "detail": "API endpoints require version prefix. Use /api/v1/stations instead."
  }
```

---

## Implementation Details (FastAPI)

### Router Organization

```
source/services/bornemap-service/
└── app/
    ├── main.py              # Entry point; registers routers
    ├── routers/
    │   └── v1/              # v1 endpoints; isolated
    │       ├── health.py    # GET /api/v1/health
    │       ├── partners.py  # GET, POST, PUT, DELETE /api/v1/partners
    │       ├── stations.py  # GET, POST, PUT, DELETE /api/v1/stations
    │       └── chargers.py  # GET, POST, PUT, DELETE /api/v1/chargers
    ├── schemas/             # Pydantic models (per-version if needed)
    │   ├── partners.py      # PartnerIn, PartnerOut, etc.
    │   ├── stations.py
    │   └── chargers.py
    └── models/              # SQLAlchemy models (shared across all versions)
        └── inventory.py     # Partner, Station, Charger ORM models
```

### Router Registration (main.py)

```python
from fastapi import FastAPI
from app.routers import v1

app = FastAPI(
    title="BorneMap API",
    description="EV station discovery and management for Tunisia",
    version="1.0.0"  # FastAPI version, not API version
)

# Include v1 routers
app.include_router(
    v1.health.router,
    prefix="/api/v1",
    tags=["health"]
)
app.include_router(
    v1.partners.router,
    prefix="/api/v1",
    tags=["partners"]
)
app.include_router(
    v1.stations.router,
    prefix="/api/v1",
    tags=["stations"]
)
app.include_router(
    v1.chargers.router,
    prefix="/api/v1",
    tags=["chargers"]
)

# Future: when v2 is added in MVP-2
# from app.routers import v2
# app.include_router(v2.health.router, prefix="/api/v2", tags=["health"])
# ... (repeat for all v2 routers)
```

### Endpoint Example (v1/health.py)

```python
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

router = APIRouter()

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint.
    
    Returns:
        - status: "ok" if healthy
        - service: service name ("bornemap-service")
        - db: "ok" if database connected, "error" otherwise
    """
    try:
        # Test DB connectivity
        db.execute("SELECT 1")
        db_status = "ok"
    except Exception:
        db_status = "error"
    
    return {
        "status": "ok",
        "service": "bornemap-service",
        "db": db_status
    }
```

---

## Documentation Generation

### OpenAPI (Swagger)

FastAPI automatically generates OpenAPI spec at `/api/docs`:

```json
{
  "openapi": "3.0.0",
  "info": {
    "title": "BorneMap API",
    "version": "1.0.0"
  },
  "paths": {
    "/api/v1/health": {
      "get": {
        "tags": ["health"],
        "summary": "Health Check",
        "description": "Returns service status and database connectivity",
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string" },
                    "service": { "type": "string" },
                    "db": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/stations": { ... },
    "/api/v1/partners": { ... },
    "/api/v1/chargers": { ... }
  }
}
```

All v1 endpoints grouped under "Endpoints" or with `[v1]` tags for clarity.

---

## Validation Rules

### Request Validation

- All v1 requests are validated against their frozen Pydantic schemas
- Invalid requests return HTTP 422 Unprocessable Entity
- Error response includes field-level validation details

### Response Validation

- v1 responses are validated against their frozen schemas before returning
- Mismatch between handler output and v1 schema is logged as error; 500 returned to client

---

## Migration Strategy (v1 → v2 in MVP-2)

When v2 is introduced in MVP-2:

1. **New router**: Create `app/routers/v2/` with updated endpoints
2. **Register**: Include v2 routers in `main.py` under `/api/v2/` prefix
3. **Documentation**: Mark v1 endpoints deprecated in OpenAPI; link to migration guide
4. **Monitoring**: Track v1 request volume; alert if any requests 6+ months before retirement
5. **Deprecation period**: Support v1 for 12 months after v2 release
6. **Retirement**: Remove v1 routers; respond with 410 Gone to v1 requests

v1 code is never modified; it simply becomes unreachable after 12 months.

---

## Summary

- **APIVersion**: Represents a discrete, immutable API contract
- **APIContract**: Documents each endpoint's v1 schema (locked) vs v2+ schema (may break)
- **Router-based**: Each version isolated in its own module; no shared logic between versions
- **OpenAPI**: Auto-generated; shows all versions with deprecation info
- **12-month support**: v1 supported until v2+12mo; no surprise client breaks
- **Phase 1 Complete**: Ready for `/speckit.tasks` (phase 2)

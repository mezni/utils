# Admin Service API Contract

## Overview

This document defines the contract between the Admin Service and its clients (administrators, partner managers, internal tools).

## Endpoints

### 1. Health Check

**Path**: `GET /api/v1/health`

**Purpose**: Verify service and database connection status.

**Authentication**: None (Sprint 1.4)

**Request**: No parameters

**Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "admin-service",
  "db": "ok"
}
```

**Response (500 Internal Server Error)**:
```json
{
  "error": "Database connection failed"
}
```

**Response (503 Service Unavailable)**:
```json
{
  "error": "Service not running"
}
```

**Success Criteria**:
- Returns 200 with correct JSON when database is available
- Returns 500 when database connection fails
- Returns 503 when service is not running

---

### 2. Partner CRUD

**Partner List** (GET /api/v1/partners):
- **Purpose**: Retrieve all partners (optionally paginated)
- **Authentication**: None (Sprint 1.4)
- **Query Parameters**:
  - `page` (optional, numeric): Page number
  - `page_size` (optional, numeric): Number of items per page
- **Response (200 OK)**:
```json
{
  "partners": [
    {
      "id": "PRT-001",
      "name": "Tunis Power",
      "email": "contact@tunispower.tn",
      "phone": "+216 71 123 456",
      "address": "Tunis, Tunisia"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_pages": 1,
    "total_items": 3
  }
}
```
- **Error (500 Internal Server Error)**: Database query failed

**Partner Details** (GET /api/v1/partners/:id):
- **Purpose**: Retrieve single partner by ID
- **Authentication**: None (Sprint 1.4)
- **Response (200 OK)**: PartnerResponse
- **Error (404 Not Found)**: Partner not found
- **Error (500 Internal Server Error)**: Database query failed

**Create Partner** (POST /api/v1/partners):
- **Purpose**: Create new partner
- **Authentication**: None (Sprint 1.4)
- **Request Body**:
```json
{
  "name": "Carsharing Tunis",
  "email": "support@carsharing.tn",
  "phone": "+216 71 789 012",
  "address": "Tunis, Tunisia"
}
```
- **Response (201 Created)**: Created PartnerResponse with generated ID
- **Error (400 Bad Request)**: Invalid partner data
- **Error (409 Conflict)**: Partner with same email already exists
- **Error (500 Internal Server Error)**: Database constraint violation

**Update Partner** (PUT /api/v1/partners/:id):
- **Purpose**: Update existing partner
- **Authentication**: None (Sprint 1.4)
- **Request Body**: Same as Create Partner
- **Response (200 OK)**: Updated PartnerResponse
- **Error (400 Bad Request)**: Invalid partner data
- **Error (404 Not Found)**: Partner not found
- **Error (409 Conflict)**: Duplicate email with different partner
- **Error (500 Internal Server Error)**: Database constraint violation

**Delete Partner** (DELETE /api/v1/partners/:id):
- **Purpose**: Delete partner
- **Authentication**: None (Sprint 1.4)
- **Response (204 No Content)**: Success
- **Error (404 Not Found)**: Partner not found
- **Error (500 Internal Server Error)**: Database constraint violation (FK references)

---

### 3. Station CRUD

**Station List** (GET /api/v1/stations):
- **Purpose**: Retrieve all stations (optionally paginated)
- **Authentication**: None (Sprint 1.4)
- **Query Parameters**:
  - `page` (optional, numeric): Page number
  - `page_size` (optional, numeric): Number of items per page
  - `partner_id` (optional, string): Filter by partner
- **Response (200 OK)**:
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "partner_id": "PRT-001",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "address": "Belvedere Square, Tunis"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_pages": 1,
    "total_items": 15
  }
}
```
- **Error (500 Internal Server Error)**: Database query failed

**Station Details** (GET /api/v1/stations/:id):
- **Purpose**: Retrieve single station by ID
- **Authentication**: None (Sprint 1.4)
- **Response (200 OK)**: StationResponse
- **Error (404 Not Found)**: Station not found
- **Error (500 Internal Server Error)**: Database query failed

**Create Station** (POST /api/v1/stations):
- **Purpose**: Create new station
- **Authentication**: None (Sprint 1.4)
- **Request Body**:
```json
{
  "partner_id": "PRT-001",
  "name": "New Station",
  "latitude": 36.864702,
  "longitude": 10.158423,
  "address": "Address Line 1"
}
```
- **Response (201 Created)**: Created StationResponse with generated ID
- **Error (400 Bad Request)**: Invalid station data or partner_id doesn't exist
- **Error (404 Not Found)**: Partner not found
- **Error (500 Internal Server Error)**: Database constraint violation

**Update Station** (PUT /api/v1/stations/:id):
- **Purpose**: Update existing station
- **Authentication**: None (Sprint 1.4)
- **Request Body**: Same as Create Station (partner_id can be updated)
- **Response (200 OK)**: Updated StationResponse
- **Error (400 Bad Request)**: Invalid station data
- **Error (404 Not Found)**: Station not found
- **Error (500 Internal Server Error)**: Database constraint violation

**Delete Station** (DELETE /api/v1/stations/:id):
- **Purpose**: Delete station
- **Authentication**: None (Sprint 1.4)
- **Response (204 No Content)**: Success
- **Error (404 Not Found)**: Station not found
- **Error (500 Internal Server Error)**: Database constraint violation (FK references to chargers)

---

### 4. Charger CRUD

**Charger List** (GET /api/v1/chargers):
- **Purpose**: Retrieve all chargers (optionally paginated)
- **Authentication**: None (Sprint 1.4)
- **Query Parameters**:
  - `page` (optional, numeric): Page number
  - `page_size` (optional, numeric): Number of items per page
  - `station_id` (optional, string): Filter by station
  - `status` (optional, string): Filter by status
- **Response (200 OK)**:
```json
{
  "chargers": [
    {
      "id": "CHR-1a2b",
      "station_id": "STN-1a2b",
      "connector_type": "Type 2",
      "power_kw": 22.0,
      "status": "available"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_pages": 1,
    "total_items": 24
  }
}
```
- **Error (500 Internal Server Error)**: Database query failed

**Charger Details** (GET /api/v1/chargers/:id):
- **Purpose**: Retrieve single charger by ID
- **Authentication**: None (Sprint 1.4)
- **Response (200 OK)**: ChargerResponse
- **Error (404 Not Found)**: Charger not found
- **Error (500 Internal Server Error)**: Database query failed

**Create Charger** (POST /api/v1/chargers):
- **Purpose**: Create new charger
- **Authentication**: None (Sprint 1.4)
- **Request Body**:
```json
{
  "station_id": "STN-1a2b",
  "connector_type": "Type 2",
  "power_kw": 22.0,
  "status": "available"
}
```
- **Response (201 Created)**: Created ChargerResponse with generated ID
- **Error (400 Bad Request)**: Invalid charger data or station_id doesn't exist
- **Error (404 Not Found)**: Station not found
- **Error (500 Internal Server Error)**: Database constraint violation

**Update Charger** (PUT /api/v1/chargers/:id):
- **Purpose**: Update existing charger
- **Authentication**: None (Sprint 1.4)
- **Request Body**: Same as Create Charger
- **Response (200 OK)**: Updated ChargerResponse
- **Error (400 Bad Request)**: Invalid charger data
- **Error (404 Not Found)**: Charger not found
- **Error (500 Internal Server Error)**: Database constraint violation

**Delete Charger** (DELETE /api/v1/chargers/:id):
- **Purpose**: Delete charger
- **Authentication**: None (Sprint 1.4)
- **Response (204 No Content)**: Success
- **Error (404 Not Found)**: Charger not found
- **Error (500 Internal Server Error)**: Database constraint violation

---

## Data Types

### ConnectorType (Enum)

| Value | Description |
|-------|-------------|
| Type 2 | Type 2 charging connector |
| CCS | CCS charging connector |
| CHAdeMO | CHAdeMO charging connector |
| GB/T | GB/T charging connector |
| Tesla Supercharger | Tesla Supercharger connector |

### ChargerStatus (Enum)

| Value | Description |
|-------|-------------|
| available | Charger is available |
| unavailable | Charger is unavailable |
| fault | Charger has fault condition |
| maintenance | Charger is under maintenance |

---

## Versioning

**Version**: v1

**Strategy**: Major version bump for breaking changes, minor for new endpoints/fields, patch for bug fixes

**Example**: v1 (current), v2 (if needed)

---

## Error Codes

| Code | Status | Meaning |
|------|--------|---------|
| VALIDATION_ERROR | 400 | Invalid input data |
| ENTITY_NOT_FOUND | 404 | Resource not found |
| DUPLICATE_ENTITY | 409 | Resource already exists |
| CONSTRAINT_VIOLATION | 400 | Database constraint violation |

---

## Testing

**Unit Tests**: Test CRUD operations, validation, error handling

**Integration Tests**: Test all CRUD endpoints with seeded database

**Performance Tests**: Verify <200ms response time for CRUD operations

**Test Files**:
- `tests/integration_test.rs` - Actix-web integration tests
- `tests/sql/test_admin_crud.sql` - SQL for test fixtures and query tests
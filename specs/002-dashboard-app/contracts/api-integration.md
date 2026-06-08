# API Integration Contract: Dashboard App

**Date**: June 8, 2026

**Backend**: Python FastAPI (`source/services/bornemap-service/`)

**Backend Documentation**: `docs/api/bornemap-service.md`

**API Base URL**: `http://localhost:8000`

**API Version**: `/api/v1/`

---

## Overview

The Dashboard App consumes a read-write REST API from the BorneMap FastAPI backend. All endpoints:
- Use the `/api/v1/` prefix (versioning established in Sprint 1.1)
- Return JSON responses
- Use standard HTTP methods (GET, POST, PUT, DELETE)
- Use UUID identifiers for all resources
- Are synchronous (no async jobs)
- Require no authentication in MVP-1

---

## Health Check

Used to verify API connectivity and database status.

### GET /api/v1/health

**Purpose**: Verify API is running and database is accessible

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/health
```

**Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "ok"
}
```

**Error Handling**: If API is unreachable or returns non-200, show ErrorState with retry button on Dashboard.

---

## Partners Endpoints

### GET /api/v1/partners

**Purpose**: Fetch all partners (used to populate dropdown on Stations screen)

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/partners
```

**Query Parameters**: None

**Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "ElectroMob Tunisia",
      "created_at": "2026-06-08T10:30:00Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "name": "Charging Network Africa",
      "created_at": "2026-06-08T10:35:00Z"
    }
  ]
}
```

**TypeScript Type**:
```typescript
interface Partner {
  id: string;        // UUID
  name: string;
  created_at: string; // ISO8601
}
```

**Dashboard Usage**: 
- Populate Partner filter dropdown on Stations screen
- Populate Partner selection dropdown on Station create/edit form

---

### POST /api/v1/partners

**Purpose**: Create a new partner

**Request**:
```bash
curl -X POST http://localhost:8000/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "New Charging Co"
  }'
```

**Request Body**:
```json
{
  "name": "string (required, 1-255 chars)"
}
```

**Response (201 Created)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440002",
    "name": "New Charging Co",
    "created_at": "2026-06-08T10:40:00Z"
  }
}
```

**Error Cases**:
- **400 Bad Request**: Missing `name` field
- **422 Unprocessable Entity**: Name validation failed (empty or >255 chars)

**Dashboard Usage**: 
- When user submits Partner create form
- Add returned partner to Partners table
- Close modal
- Show success indication

---

### GET /api/v1/partners/{id}

**Purpose**: Fetch single partner details

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "ElectroMob Tunisia",
    "created_at": "2026-06-08T10:30:00Z"
  }
}
```

**Error Cases**:
- **404 Not Found**: Partner ID doesn't exist

**Dashboard Usage**: 
- Pre-fill edit modal with current partner name

---

### PUT /api/v1/partners/{id}

**Purpose**: Update partner name

**Request**:
```bash
curl -X PUT http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Updated Partner Name"
  }'
```

**Request Body**:
```json
{
  "name": "string (1-255 chars)"
}
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Updated Partner Name",
    "created_at": "2026-06-08T10:30:00Z"
  }
}
```

**Error Cases**:
- **404 Not Found**: Partner ID doesn't exist
- **422 Unprocessable Entity**: Validation failed

**Dashboard Usage**: 
- When user saves changes to partner
- Update table row immediately
- Close modal

---

### DELETE /api/v1/partners/{id}

**Purpose**: Delete a partner and associated stations/chargers (cascade)

**Request**:
```bash
curl -X DELETE http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response (204 No Content)**:
```
[Empty body]
```

**Error Cases**:
- **404 Not Found**: Partner ID doesn't exist
- **409 Conflict**: If backend prevents cascade (currently not enforced; decision documented in backend)

**Dashboard Usage**: 
- When user confirms delete in modal
- Remove partner row from table
- Show confirmation if stations exist (optional)

---

## Stations Endpoints

### GET /api/v1/stations

**Purpose**: Fetch all stations with optional partner filter

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/stations
curl -X GET http://localhost:8000/api/v1/stations?partner_id=550e8400-e29b-41d4-a716-446655440000
```

**Query Parameters**:
- `partner_id` (optional): Filter by partner UUID

**Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440010",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Tunis Central Station",
      "address": "1 Avenue Bourguiba, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1963,
      "charger_count": 4,
      "available_count": 3,
      "created_at": "2026-06-08T10:45:00Z",
      "updated_at": "2026-06-08T10:45:00Z"
    }
  ]
}
```

**TypeScript Type**:
```typescript
interface Station {
  id: string;             // UUID
  partner_id: string;     // UUID
  name: string;
  address: string;
  latitude: number;       // -90 to 90
  longitude: number;      // -180 to 180
  charger_count: number;  // Computed
  available_count: number; // Computed
  created_at: string;     // ISO8601
  updated_at: string;     // ISO8601
}
```

**Dashboard Usage**: 
- Load Stations table on page mount
- Apply partner filter when user selects from dropdown
- Display name, address, partner name (join), charger_count in table

---

### POST /api/v1/stations

**Purpose**: Create a new station

**Request**:
```bash
curl -X POST http://localhost:8000/api/v1/stations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "New Station",
    "address": "123 Main St, City",
    "latitude": 36.5,
    "longitude": 10.2,
    "partner_id": "550e8400-e29b-41d4-a716-446655440000"
  }'
```

**Request Body**:
```json
{
  "name": "string (required, 1-255 chars)",
  "address": "string (required, 1-500 chars)",
  "latitude": "number (required, -90 to 90)",
  "longitude": "number (required, -180 to 180)",
  "partner_id": "string (required, UUID format)"
}
```

**Response (201 Created)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440011",
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "New Station",
    "address": "123 Main St, City",
    "latitude": 36.5,
    "longitude": 10.2,
    "charger_count": 0,
    "available_count": 0,
    "created_at": "2026-06-08T11:00:00Z",
    "updated_at": "2026-06-08T11:00:00Z"
  }
}
```

**Error Cases**:
- **400 Bad Request**: Missing required fields
- **422 Unprocessable Entity**: Validation failed (name, address, coordinate ranges)
- **404 Not Found**: partner_id doesn't exist

**Dashboard Usage**: 
- When user submits Station create form
- Add returned station to Stations table
- Close modal
- Re-fetch partner stats if overview is visible

---

### GET /api/v1/stations/{id}

**Purpose**: Fetch station detail with full charger list

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/stations/550e8400-e29b-41d4-a716-446655440010
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440010",
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Tunis Central Station",
    "address": "1 Avenue Bourguiba, Tunis",
    "latitude": 36.8065,
    "longitude": 10.1963,
    "charger_count": 4,
    "available_count": 3,
    "created_at": "2026-06-08T10:45:00Z",
    "updated_at": "2026-06-08T10:45:00Z",
    "chargers": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440020",
        "station_id": "550e8400-e29b-41d4-a716-446655440010",
        "connector_type": "Type2",
        "power_kw": 22,
        "status": "available",
        "created_at": "2026-06-08T10:50:00Z",
        "updated_at": "2026-06-08T10:50:00Z"
      }
    ]
  }
}
```

**TypeScript Type**:
```typescript
interface StationWithChargers extends Station {
  chargers: Charger[];
}
```

**Error Cases**:
- **404 Not Found**: Station ID doesn't exist

**Dashboard Usage**: 
- Pre-fill edit modal with station data
- (Driver apps use this for station detail view)

---

### PUT /api/v1/stations/{id}

**Purpose**: Update station name, address, or coordinates

**Request**:
```bash
curl -X PUT http://localhost:8000/api/v1/stations/550e8400-e29b-41d4-a716-446655440010 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Updated Station Name",
    "address": "456 New St, City",
    "latitude": 36.81,
    "longitude": 10.20
  }'
```

**Request Body**: (all fields optional)
```json
{
  "name": "string (1-255 chars)",
  "address": "string (1-500 chars)",
  "latitude": "number (-90 to 90)",
  "longitude": "number (-180 to 180)"
}
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440010",
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Updated Station Name",
    "address": "456 New St, City",
    "latitude": 36.81,
    "longitude": 10.20,
    "charger_count": 4,
    "available_count": 3,
    "created_at": "2026-06-08T10:45:00Z",
    "updated_at": "2026-06-08T11:05:00Z"
  }
}
```

**Error Cases**:
- **404 Not Found**: Station ID doesn't exist
- **422 Unprocessable Entity**: Validation failed

**Dashboard Usage**: 
- When user saves station edit
- Update table row immediately

---

### DELETE /api/v1/stations/{id}

**Purpose**: Delete station and associated chargers (cascade)

**Request**:
```bash
curl -X DELETE http://localhost:8000/api/v1/stations/550e8400-e29b-41d4-a716-446655440010
```

**Response (204 No Content)**:
```
[Empty body]
```

**Error Cases**:
- **404 Not Found**: Station ID doesn't exist

**Dashboard Usage**: 
- When user confirms delete
- Remove station row from table
- Decrement partner charger counts if displayed

---

## Chargers Endpoints

### GET /api/v1/chargers

**Purpose**: Fetch all chargers with optional station filter

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/chargers
curl -X GET http://localhost:8000/api/v1/chargers?station_id=550e8400-e29b-41d4-a716-446655440010
```

**Query Parameters**:
- `station_id` (optional): Filter by station UUID

**Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440020",
      "station_id": "550e8400-e29b-41d4-a716-446655440010",
      "connector_type": "Type2",
      "power_kw": 22,
      "status": "available",
      "created_at": "2026-06-08T10:50:00Z",
      "updated_at": "2026-06-08T10:50:00Z"
    }
  ]
}
```

**TypeScript Type**:
```typescript
interface Charger {
  id: string;             // UUID
  station_id: string;     // UUID
  connector_type: string;
  power_kw: number;
  status: "available" | "in_use" | "maintenance";
  created_at: string;     // ISO8601
  updated_at: string;     // ISO8601
}
```

**Dashboard Usage**: 
- Load Chargers table on page mount
- Apply station filter when user selects from dropdown
- Display with station name joined from stations table

---

### POST /api/v1/chargers

**Purpose**: Create a new charger

**Request**:
```bash
curl -X POST http://localhost:8000/api/v1/chargers \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "550e8400-e29b-41d4-a716-446655440010",
    "connector_type": "Type2",
    "power_kw": 22,
    "status": "available"
  }'
```

**Request Body**:
```json
{
  "station_id": "string (required, UUID)",
  "connector_type": "string (required, enum)",
  "power_kw": "number (required, positive)",
  "status": "string (required, available|in_use|maintenance)"
}
```

**Response (201 Created)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440021",
    "station_id": "550e8400-e29b-41d4-a716-446655440010",
    "connector_type": "Type2",
    "power_kw": 22,
    "status": "available",
    "created_at": "2026-06-08T11:10:00Z",
    "updated_at": "2026-06-08T11:10:00Z"
  }
}
```

**Error Cases**:
- **400 Bad Request**: Missing required fields
- **422 Unprocessable Entity**: Validation failed (connector_type, power_kw, status)
- **404 Not Found**: station_id doesn't exist

**Dashboard Usage**: 
- When user submits Charger create form
- Add returned charger to Chargers table
- Close modal
- Update station charger_count if visible

---

### GET /api/v1/chargers/{id}

**Purpose**: Fetch single charger details

**Request**:
```bash
curl -X GET http://localhost:8000/api/v1/chargers/550e8400-e29b-41d4-a716-446655440020
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440020",
    "station_id": "550e8400-e29b-41d4-a716-446655440010",
    "connector_type": "Type2",
    "power_kw": 22,
    "status": "available",
    "created_at": "2026-06-08T10:50:00Z",
    "updated_at": "2026-06-08T10:50:00Z"
  }
}
```

**Error Cases**:
- **404 Not Found**: Charger ID doesn't exist

**Dashboard Usage**: 
- Pre-fill edit modal with charger data

---

### PUT /api/v1/chargers/{id}

**Purpose**: Update charger status (primary use case) or other fields

**Request**:
```bash
curl -X PUT http://localhost:8000/api/v1/chargers/550e8400-e29b-41d4-a716-446655440020 \
  -H "Content-Type: application/json" \
  -d '{
    "status": "maintenance"
  }'
```

**Request Body**: (all fields optional)
```json
{
  "connector_type": "string",
  "power_kw": "number",
  "status": "string (available|in_use|maintenance)"
}
```

**Response (200 OK)**:
```json
{
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440020",
    "station_id": "550e8400-e29b-41d4-a716-446655440010",
    "connector_type": "Type2",
    "power_kw": 22,
    "status": "maintenance",
    "created_at": "2026-06-08T10:50:00Z",
    "updated_at": "2026-06-08T11:15:00Z"
  }
}
```

**Error Cases**:
- **404 Not Found**: Charger ID doesn't exist
- **422 Unprocessable Entity**: Validation failed

**Dashboard Usage**: 
- When user saves charger edit (especially status change)
- Update table row immediately
- Refresh station charger_count if needed

---

### DELETE /api/v1/chargers/{id}

**Purpose**: Delete a charger

**Request**:
```bash
curl -X DELETE http://localhost:8000/api/v1/chargers/550e8400-e29b-41d4-a716-446655440020
```

**Response (204 No Content)**:
```
[Empty body]
```

**Error Cases**:
- **404 Not Found**: Charger ID doesn't exist

**Dashboard Usage**: 
- When user confirms delete
- Remove charger row from table
- Update station charger_count

---

## Error Response Format

All error responses use this format:

```json
{
  "detail": "Error message or array of field-specific errors"
}
```

**Example Validation Error (422)**:
```json
{
  "detail": [
    {
      "loc": ["body", "latitude"],
      "msg": "ensure this value is less than or equal to 90",
      "type": "value_error.number.not_le",
      "ctx": {"limit_value": 90}
    }
  ]
}
```

**Dashboard Handling**:
- Extract and display field-specific messages inline on forms
- Log to console for debugging
- Show generic "An error occurred" toast for unhandled errors

---

## Summary

**Total Endpoints**: 16
- Partners: 4 (list, create, read, update, delete)
- Stations: 5 (list, create, read, update, delete)
- Chargers: 5 (list, create, read, update, delete)
- Health: 1 (health check)

**Key Patterns**:
- All endpoints return 200/201 on success
- Delete returns 204 No Content
- All POST endpoints return created resource
- All list endpoints support GET
- No pagination required (MVP-1 scope)
- No authentication required (MVP-1 scope)
- All IDs are UUIDs

**Backend Reference**: `docs/api/bornemap-service.md` (100+ pages with all details)

Ready for Dashboard frontend implementation.

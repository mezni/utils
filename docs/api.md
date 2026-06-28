# BorneMap API Contract — v1

## 1. Global API Rules

- **Base URL:** `/api/v1`
- **Content-Type:** `application/json`

### 1.1 Success Response

```json
{
  "data": {},
  "meta": {},
  "error": null
}
```

### 1.2 Error Response

```json
{
  "data": null,
  "meta": {},
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message",
    "details": {}
  }
}
```

### 1.3 HTTP Status Codes

| Code | Usage |
|------|-------|
| 200 | Success |
| 201 | Created |
| 400 | Bad request |
| 404 | Not found |
| 500 | Internal error |

## 2. Common Conventions

### 2.1 ID Format

Entities use prefixed IDs:
- `PRT_xxx` — Partner
- `STN_xxx` — Station
- `CON_xxx` — Connector

### 2.2 Pagination (Lists)

**Request:**
```
?page=1&limit=50
```

**Response:**

```json
{
  "data": [],
  "meta": {
    "page": 1,
    "limit": 50,
    "total": 120
  },
  "error": null
}
```

### 2.3 Connector Types (ENUM)

```
CCS2, TYPE2, CHADEMO, NACS, TESLA, GB_T, UNKNOWN
```

### 2.4 Station Status (ENUM)

```
ONLINE, OFFLINE, MAINTENANCE, UNKNOWN
```

## 3. Admin Service API

### 3.1 Partners

#### Create Partner
```
POST /api/v1/partners
```

**Request:**
```json
{
  "name": "Tesla Tunisia"
}
```

**Response (201):**
```json
{
  "data": {
    "id": "PRT_01ABC",
    "name": "Tesla Tunisia",
    "created_at": "2026-01-01T00:00:00Z"
  },
  "meta": {},
  "error": null
}
```

#### List Partners
```
GET /api/v1/partners?page=1&limit=50
```

**Response:**
```json
{
  "data": [
    {
      "id": "PRT_01ABC",
      "name": "Tesla Tunisia"
    }
  ],
  "meta": {
    "page": 1,
    "limit": 50,
    "total": 1
  },
  "error": null
}
```

### 3.2 Stations

#### Create Station
```
POST /api/v1/stations
```

**Request:**
```json
{
  "partner_id": "PRT_01ABC",
  "name": "Station Tunis Centre",
  "address": "Tunis, Tunisia",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**Response (201):**
```json
{
  "data": {
    "id": "STN_01XYZ",
    "partner_id": "PRT_01ABC",
    "name": "Station Tunis Centre",
    "address": "Tunis, Tunisia",
    "latitude": 36.8065,
    "longitude": 10.1815
  },
  "meta": {},
  "error": null
}
```

#### List Stations
```
GET /api/v1/stations?page=1&limit=50&partner_id=PRT_01ABC
```

#### Get Station by ID
```
GET /api/v1/stations/{id}
```

**Response:**
```json
{
  "data": {
    "id": "STN_01XYZ",
    "partner_id": "PRT_01ABC",
    "name": "Station Tunis Centre",
    "address": "Tunis, Tunisia",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "status": "ONLINE",
    "connectors": [
      {
        "id": "CON_01A",
        "type": "CCS2",
        "power_kw": 50
      }
    ]
  },
  "meta": {},
  "error": null
}
```

#### Update Station
```
PUT /api/v1/stations/{id}
```

#### Delete Station
```
DELETE /api/v1/stations/{id}
```

### 3.3 Connectors

#### Create Connector
```
POST /api/v1/connectors
```

**Request:**
```json
{
  "station_id": "STN_01XYZ",
  "type": "CCS2",
  "power_kw": 50
}
```

#### List Connectors
```
GET /api/v1/connectors?station_id=STN_01XYZ
```

#### Delete Connector
```
DELETE /api/v1/connectors/{id}
```

## 4. Driver Service API (Public / Read-Only)

### 4.1 Nearby Stations
```
GET /api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius=5000
```

**Response:**
```json
{
  "data": [
    {
      "id": "STN_01XYZ",
      "partner": "Tesla Tunisia",
      "name": "Station Tunis Centre",
      "address": "Tunis, Tunisia",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "distance": 1200,
      "status": "ONLINE",
      "connectors": [
        { "type": "CCS2", "power_kw": 50 },
        { "type": "TYPE2", "power_kw": 22 }
      ]
    }
  ],
  "meta": {
    "radius": 5000,
    "count": 1
  },
  "error": null
}
```

**Internal Mandatory Flow:**
```
GIS: ST_DWithin(lat, lng, radius)
        ↓
station_ids + distance
        ↓
EV schema join: stations + partners + connectors
        ↓
DTO mapping
        ↓
response
```

## 5. Auth API (Phase 2)

```
POST /api/v1/auth/register
POST /api/v1/auth/login
GET  /api/v1/auth/me
```

**Login Response:**
```json
{
  "data": {
    "token": "JWT_TOKEN"
  },
  "meta": {},
  "error": null
}
```

## 6. Validation Rules

| Field | Rule |
|-------|------|
| latitude | -90 to 90 |
| longitude | -180 to 180 |
| station name | 1–150 chars |
| address | 1–250 chars |
| connector power_kw | > 0 and < 1000 |
| connector type | Must be valid enum |
| partner name | 1–100 chars |

## 7. Performance Rules

Nearby API MUST:
- Use `ST_DWithin` for spatial filtering
- Use GiST index on geometry
- Sort by distance ASC
- Apply optional limit (default 20–50)

## 8. Architectural Guarantees

| Service | Writes | Reads | Forbidden |
|---------|--------|-------|-----------|
| Admin Service | `ev` only | `ev` optionally | ❌ gis |
| Driver Service | ❌ Never | `gis.nearby_stations()` + ev joins | ❌ writes, ❌ direct GIS SQL |
| GIS Layer | ❌ Never | Computes spatial filtering, returns station IDs + distance only | ❌ business logic |

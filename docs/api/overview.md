# API Overview

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 API PRINCIPLES

- **Versioned from day one** → `/api/v1/*`
- **No undocumented endpoints**
- **No business logic in frontend**
- **driver-service is the only MVP-1 backend**
- **All responses must be stable JSON contracts**

---

## 📋 BASE PATH

`/api/v1`

---

## 🚫 NON-NEGOTIABLE RULES

- No additional endpoints
- No shape modifications without version bump
- No frontend bypass
- No service expansion in MVP-1

---

## 📊 API ENDPOINTS

### MVP-1 Service: driver-service

**Only backend service for MVP-1**

---

### 1. Station Discovery API

#### Get All Stations

**Endpoint:** `GET /api/v1/stations`

**Description:** Returns all active stations (no geo filtering)

**Response:**
```json
[
  {
    "id": "STA-001",
    "name": "Tunis Center Station",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "status": "active",
    "power_kw": 120,
    "connector_types": ["CCS", "Type2"]
  }
]
```

---

#### Nearby Stations (CORE MVP FEATURE)

**Endpoint:** `GET /api/v1/stations/nearby?lat=36.8&lng=10.2&radius=5000`

**Description:** Find stations within specified radius of location

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| lat | float | Yes | - | User latitude |
| lng | float | Yes | - | User longitude |
| radius | int | No | 5000 | Search radius in meters |

**Response:**
```json
[
  {
    "id": "STA-002",
    "name": "Airport Station",
    "latitude": 36.847,
    "longitude": 10.217,
    "distance": 842,
    "status": "active"
  }
]
```

**Behavior Rules:**
- Sorted by distance ASC (nearest first)
- Only active stations returned
- Max radius enforced server-side (10km default)
- Distance is integer in meters

---

#### Get Station by ID

**Endpoint:** `GET /api/v1/stations/{id}`

**Description:** Get detailed information about a specific station

**Response:**
```json
{
  "id": "STA-002",
  "name": "Airport Station",
  "latitude": 36.847,
  "longitude": 10.217,
  "status": "active",
  "power_kw": 120,
  "connector_types": ["CCS", "Type2"],
  "chargers": [
    {
      "id": "CHR-001",
      "type": "CCS",
      "power_kw": 60,
      "status": "available"
    }
  ]
}
```

---

### 2. Analytics API (MINIMAL MVP-1)

#### Event Ingestion

**Endpoint:** `POST /api/v1/events`

**Description:** Send analytics events to the system

**Request Body:**
```json
{
  "type": "StationOpened",
  "user_id": "USR-001",
  "station_id": "STA-002",
  "payload": {
    "source": "map"
  }
}
```

**Event Types:**
- `MapViewed` - User opened the map
- `StationOpened` - User viewed station details
- `NearbySearchExecuted` - User searched for nearby stations

**Rules:**
- Async fire-and-forget allowed
- Must not block UI
- Always append-only in DB

---

## 🧭 ERROR FORMAT (STANDARDIZED)

**All endpoints MUST return consistent error format:**

```json
{
  "error": {
    "code": "STATION_NOT_FOUND",
    "message": "Station not found"
  }
}
```

**Common Error Codes:**

| Error Code | Message | HTTP Status |
|------------|---------|-------------|
| STATION_NOT_FOUND | Station not found | 404 |
| INVALID_RADIUS | Search radius exceeds maximum | 400 |
| INVALID_COORDINATES | Invalid latitude or longitude | 400 |
| INVALID_PARAM | Invalid query parameter | 400 |
| INTERNAL_ERROR | Internal server error | 500 |

---

## 📦 COMMON RESPONSE RULES

### Success
- Always JSON
- No envelope wrapper (no `{data: ...}`)
- Direct response body

### Failure
- Always error object
- Never raw strings
- Consistent error format

---

## ⚡ PERFORMANCE RULES

### Nearby Endpoint Optimization
- Use PostGIS for geospatial queries
- Optimized index queries
- Distance calculations efficient

### Response Times
- **Target:** < 200ms for all endpoints
- **Nearby search:** < 200ms target
- **Station detail:** < 150ms target
- **All requests:** < 500ms max

### Radius Enforcement
- **Max radius:** 10km (enforced server-side)
- **Min radius:** 100m (recommended)
- **Default radius:** 5km for MVP-1

---

## 🔐 AUTH RULE (MVP-1 SIMPLIFIED)

**NO authentication required for MVP-1 endpoints**

- `user_id` is optional in analytics only
- `auth-service` introduced in MVP-3 only
- No JWT tokens for MVP-1
- No rate limiting for MVP-1 (basic protection)

---

## 📱 FRONTEND INTEGRATION RULE

**ALL API calls MUST go through:**

`@bm/api-client`

**Never:**
- `fetch()`
- `axios` directly
- Inline HTTP logic

---

## 🎯 API CONTRACT SUMMARY

**MVP-1 API Surface:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/stations` | GET | Get all active stations |
| `/api/v1/stations/nearby` | GET | Find stations within radius |
| `/api/v1/stations/{id}` | GET | Get station details |
| `/api/v1/events` | POST | Send analytics events |

---

## 🔄 API IMPLEMENTATION NOTES

### Rust Implementation (driver-service)

**Key Implementation Details:**
- Use Rust for performance
- PostgreSQL + PostGIS for geospatial queries
- Async HTTP server (Tokio, Axum)
- Efficient JSON serialization (serde_json)
- Proper error handling and validation

**Database Queries:**
- Use PostGIS functions for distance calculations
- Optimize with proper indexes
- Parameterized queries for security
- Connection pooling

**Performance Considerations:**
- Batch queries where possible
- Limit result sets appropriately
- Cache frequently accessed data (future MVP)
- Async processing for heavy operations

---

## 🎯 MVP-1 API CONSTRAINTS

### Scope Boundaries

**ALLOWED:**
- Station discovery operations
- Basic analytics event tracking
- Simple geo queries
- Direct response formats

**FORBIDDEN:**
- Authentication endpoints
- User management
- Admin operations
- Complex business logic
- Rate limiting
- Caching (MVP-1)

### Versioning

**Version 1 (Current):**
- All endpoints use `/api/v1/*` path
- Stable JSON contracts
- No breaking changes expected

**Version 2:**
- Only if breaking changes required
- New endpoints would use `/api/v2/*`
- Migration plan required

---

## 🧠 CORE PRINCIPLE

**API is not a feature layer. It is the contract between reality (DB) and experience (UI).**

---

*This API specification defines the complete MVP-1 service contract for station discovery and basic analytics.*
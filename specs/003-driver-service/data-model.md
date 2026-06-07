# Data Model: Driver Service

## Overview

This sprint defines the API request/response data structures for the Driver Service. The service exposes two endpoints (health check and stations nearby) with simple, typed Rust structs for request parameters and response bodies.

## API Request Schemas

### Health Check Request

```rust
pub struct HealthCheckRequest {}
```

**Fields**: None

**Purpose**: Health check endpoint doesn't require request parameters.

---

### Nearby Query Request

```rust
pub struct NearbyStationsRequest {
    pub lat: f64,
    pub lng: f64,
    pub radius_km: f64,
}
```

**Fields**:
- `lat: f64` - Latitude of the query point (degrees, -90 to 90)

**Validation Rules**:
- Must be between -90 and 90 (inclusive)
- Must be non-negative

---

## API Response Schemas

### Health Check Response (200 OK)

```rust
pub struct HealthCheckResponse {
    pub status: String,
    pub service: String,
    pub db: String,
}
```

**Fields**:
- `status: String` - Service status: "ok" (always for this version)
- `service: String` - Service name: "driver-service"
- `db: String` - Database connection status: "ok" or "error"

**Example**:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

**Error Response (500 Internal Server Error)**:
```rust
pub struct HealthCheckErrorResponse {
    pub error: String,
}

// Example
{
  "error": "Database connection failed"
}
```

---

### Nearby Stations Response (200 OK)

```rust
pub struct NearbyStationsResponse {
    pub stations: Vec<StationResponse>,
}

pub struct StationResponse {
    pub id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub distance_km: f64,
}
```

**Fields**:
- `stations: Vec<StationResponse>` - Array of stations within radius
- `StationResponse.id: String` - NanoID (STN-...)
- `StationResponse.name: String` - Station name
- `StationResponse.latitude: f64` - Latitude in degrees
- `StationResponse.longitude: f64` - Longitude in degrees
- `StationResponse.distance_km: f64` - Distance from query point in kilometers

**Example**:
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "distance_km": 1.2
    },
    {
      "id": "STN-2c3d",
      "name": "Hammamet Station",
      "latitude": 36.846200,
      "longitude": 10.180000,
      "distance_km": 2.5
    }
  ]
}
```

---

### Error Response (400 Bad Request)

```rust
pub struct ValidationErrorResponse {
    pub error: String,
}
```

**Example**:
```json
{
  "error": "Invalid parameters: latitude must be between -90 and 90"
}
```

---

### Error Response (500 Internal Server Error)

```rust
pub struct InternalServerErrorResponse {
    pub error: String,
}
```

**Example**:
```json
{
  "error": "Database query failed"
}
```

---

## Validation Rules

### NearbyStationsRequest

| Field | Rule | Error Message |
|-------|------|---------------|
| lat | Must be numeric | "Invalid parameter: lat must be a number" |
| lng | Must be numeric | "Invalid parameter: lng must be a number" |
| radius_km | Must be numeric | "Invalid parameter: radius_km must be a number" |
| lat | Must be between -90 and 90 | "Invalid parameter: latitude must be between -90 and 90" |
| lng | Must be between -180 and 180 | "Invalid parameter: longitude must be between -180 and 180" |
| radius_km | Must be >= 0.1 | "Invalid parameter: radius_km must be at least 0.1" |
| radius_km | Must be <= 100 | "Invalid parameter: radius_km must be at most 100" |

---

## Data Relationships

- **DriverService** is a standalone service with no external entity dependencies
- **NearbyStationsResponse** maps to `inventory.station` table (via database query)
- **StationResponse** mirrors `inventory.station` fields (id, name, latitude, longitude)
- **distance_km** is a computed field, not stored in database

---

## Validation Patterns

- Numeric validation: Use Rust's `f64` type and range checks
- Range validation: Check lat/lng ranges before query
- Error handling: Return 400 Bad Request for invalid parameters, 500 for database errors

---

## Future Extensions

| Feature | Future Sprint | Impact on Data Model |
|---------|---------------|---------------------|
| Pagination | Sprint 2.x | Add `page` and `page_size` parameters |
| Filters (partner_id, status) | Sprint 2.x | Add filter fields to request |
| Station details (charger count) | Sprint 2.x | Add `charger_count: i32` to `StationResponse` |
| Authentication | Sprint 2.x | Add `Authorization: Bearer <token>` header, return 401 Unauthorized |
| Detailed errors | Sprint 2.x | Add `code` field (e.g., "INVALID_PARAMETER", "DATABASE_ERROR") |
| Rate limiting | Sprint 2.x | No schema impact, header-based |
| Filtering by date range | Sprint 2.x | Add `start_date`, `end_date` parameters |
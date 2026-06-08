# Data Model: Admin Service

## Overview

This sprint defines the API request/response data structures for the Admin Service. The service exposes two endpoints (health check and CRUD operations for partners, stations, and chargers) with simple, typed Rust structs for request parameters and response bodies.

## API Request Schemas

### Health Check Request

```rust
pub struct HealthCheckRequest {}
```

**Fields**: None

**Purpose**: Health check endpoint doesn't require request parameters.

---

### Partner Request

```rust
pub struct PartnerRequest {
    pub name: String,
    pub email: String,
    pub phone: String,
    pub address: String,
}
```

**Fields**:
- `name: String` - Partner name (required)
- `email: String` - Partner email (required, unique)
- `phone: String` - Partner phone (required)
- `address: String` - Partner address (required)

**Validation Rules**:
- All fields must be non-empty
- Email must be valid format
- Phone must be valid format
- Email must be unique (no duplicates)

---

### Station Request

```rust
pub struct StationRequest {
    pub partner_id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub address: String,
}
```

**Fields**:
- `partner_id: String` - Partner ID (required, FK reference)
- `name: String` - Station name (required)
- `latitude: f64` - Latitude in degrees (required)
- `longitude: f64` - Longitude in degrees (required)
- `address: String` - Station address (required)

**Validation Rules**:
- All fields must be non-empty
- partner_id must exist in inventory.partner table
- latitude: -90 to 90
- longitude: -180 to 180
- Name must be unique per partner

---

### Charger Request

```rust
pub struct ChargerRequest {
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}
```

**Fields**:
- `station_id: String` - Station ID (required, FK reference)
- `connector_type: String` - Connector type (required)
- `power_kw: f64` - Power rating in kW (required)
- `status: String` - Status (required)

**Validation Rules**:
- All fields must be non-empty
- station_id must exist in inventory.station table
- connector_type must be one of: Type 2, CCS, CHAdeMO, GB/T, Tesla Supercharger
- power_kw must be >= 0
- status must be one of: available, unavailable, fault, maintenance

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
- `service: String` - Service name: "admin-service"
- `db: String` - Database connection status: "ok" or "error"

**Example**:
```json
{
  "status": "ok",
  "service": "admin-service",
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

### Partner Response

```rust
pub struct PartnerResponse {
    pub id: String,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub address: String,
}
```

**Fields**:
- `id: String` - Partner NanoID (PRT-...)
- `name: String` - Partner name
- `email: String` - Partner email
- `phone: String` - Partner phone
- `address: String` - Partner address

---

### Station Response

```rust
pub struct StationResponse {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub address: String,
}
```

**Fields**:
- `id: String` - Station NanoID (STN-...)
- `partner_id: String` - Partner ID (FK reference)
- `name: String` - Station name
- `latitude: f64` - Latitude in degrees
- `longitude: f64` - Longitude in degrees
- `address: String` - Station address

---

### Charger Response

```rust
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
}
```

**Fields**:
- `id: String` - Charger NanoID (CHR-...)
- `station_id: String` - Station ID (FK reference)
- `connector_type: String` - Connector type
- `power_kw: f64` - Power rating in kW
- `status: String` - Status

---

### Partner List Response

```rust
pub struct PartnerListResponse {
    pub partners: Vec<PartnerResponse>,
    pub pagination: Option<Pagination>,
}
```

**Fields**:
- `partners: Vec<PartnerResponse>` - Array of partners
- `pagination: Option<Pagination>` - Pagination info (optional)

---

## Error Response Schemas

### Partner Error Response

```rust
pub struct PartnerErrorResponse {
    pub error: String,
}
```

**Examples**:
- 404 Not Found: `{"error": "Partner not found"}`
- 400 Bad Request: `{"error": "Invalid partner data: email is required"}`
- 409 Conflict: `{"error": "Partner with this email already exists"}`
- 500 Internal Server Error: `{"error": "Database error: constraint violation"}`

---

## Data Relationships

- **Partner**: Parent entity for stations and chargers
- **Station**: Parent entity for chargers
- **Partner → Station**: One-to-many relationship
- **Station → Charger**: One-to-many relationship
- **PartnerList**: Returns all partners (optionally paginated)

---

## Validation Patterns

### Partner Validation
- Email format: Use regex validation
- Phone format: Use regex validation
- Unique email: Database constraint

### Station Validation
- FK partner_id: Must exist in inventory.partner
- Coordinates: Range validation (-90 to 90, -180 to 180)
- Unique name per partner: Database constraint

### Charger Validation
- FK station_id: Must exist in inventory.station
- connector_type: Enum validation (Type 2, CCS, CHAdeMO, GB/T, Tesla Supercharger)
- power_kw: Range validation (>= 0)
- status: Enum validation (available, unavailable, fault, maintenance)

---

## Pagination (Optional Future Enhancement)

```rust
pub struct Pagination {
    pub page: u32,
    pub page_size: u32,
    pub total_pages: u32,
    pub total_items: u32,
}
```

**Usage**: For partner list endpoints when pagination is implemented

---

## CRUD Operations Summary

| Operation | Endpoint | Request | Response |
|-----------|----------|---------|----------|
| Create Partner | POST /api/v1/partners | PartnerRequest | 201 Created + PartnerResponse |
| Get Partner | GET /api/v1/partners/:id | - | 200 OK + PartnerResponse |
| List Partners | GET /api/v1/partners | - | 200 OK + PartnerListResponse |
| Update Partner | PUT /api/v1/partners/:id | PartnerRequest | 200 OK + PartnerResponse |
| Delete Partner | DELETE /api/v1/partners/:id | - | 204 No Content |
| Create Station | POST /api/v1/stations | StationRequest | 201 Created + StationResponse |
| Get Station | GET /api/v1/stations/:id | - | 200 OK + StationResponse |
| List Stations | GET /api/v1/stations | - | 200 OK + StationListResponse |
| Update Station | PUT /api/v1/stations/:id | StationRequest | 200 OK + StationResponse |
| Delete Station | DELETE /api/v1/stations/:id | - | 204 No Content |
| Create Charger | POST /api/v1/chargers | ChargerRequest | 201 Created + ChargerResponse |
| Get Charger | GET /api/v1/chargers/:id | - | 200 OK + ChargerResponse |
| List Chargers | GET /api/v1/chargers | - | 200 OK + ChargerListResponse |
| Update Charger | PUT /api/v1/chargers/:id | ChargerRequest | 200 OK + ChargerResponse |
| Delete Charger | DELETE /api/v1/chargers/:id | - | 204 No Content |

---

## Future Extensions

| Feature | Future Sprint | Impact on Data Model |
|---------|---------------|---------------------|
| Authentication | Sprint 2.x | Add `Authorization: Bearer <token>` header, return 401 Unauthorized |
| Pagination | Sprint 1.5+ | Add `page` and `page_size` parameters, return Pagination object |
| Filters | Sprint 2.x | Add filter fields to requests (partner_id, status, etc.) |
| Station location auto-creation | Sprint 1.5+ | Automatic GIS sync when station created/updated |
| Detailed errors | Sprint 2.x | Add `code` field (e.g., "VALIDATION_ERROR", "DUPLICATE_ENTITY") |
| Audit logging | Sprint 2.x | Add `created_at`, `updated_at`, `updated_by` fields |
| Bulk operations | Sprint 2.x | Add POST /api/v1/partners/batch endpoint |
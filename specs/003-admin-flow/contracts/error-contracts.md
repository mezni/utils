# Error Contracts: Admin Service Core Operations

## Overview

This document defines the comprehensive error handling contracts for the Admin Service, including error types, status codes, error codes, and response formats.

---

## Error Architecture

### Error Types

**Source**: `auth-service/src/error.rs`

```rust
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Forbidden")]
    Forbidden,

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal server error")]
    InternalError,
}
```

**Trait**: `ResponseError` for automatic HTTP response generation

### Response Format

All errors follow this JSON structure:

```json
{
  "error": "<error_code>",
  "details": {
    // Optional: Additional error context
  }
}
```

**Status Code**: Generated from `AuthError::status_code()`

---

## Error Codes and Status Codes

| Error Type | Status Code | Error Code | Description |
|------------|-------------|------------|-------------|
| Validation Error | 400 Bad Request | `validation_error` | Invalid input data |
| Unauthorized | 401 Unauthorized | `unauthorized` | Missing or invalid auth |
| Forbidden | 403 Forbidden | `forbidden` | Insufficient permissions |
| Conflict (Duplicate) | 409 Conflict | `duplicate_request` | Idempotency check failed (no key) |
| Constraint Violation | 409 Conflict | `constraint_violation` | DB constraint failed |
| Entity Not Found | 404 Not Found | `not_found` | Entity doesn't exist |
| Entity Deleted | 410 Gone | `entity_deleted` | Entity soft-deleted |
| Internal Error | 500 Internal Server Error | `internal_error` | Unexpected server error |
| Redis Bust Failure | 200 OK (non-fatal) | — | Cache bust failed (not an error) |

---

## Detailed Error Responses

### 1. Validation Error (400 Bad Request)

**Error Code**: `validation_error`

**Description**: Request body contains invalid or missing required fields.

**Example 1: Missing Required Field**
```json
{
  "error": "validation_error",
  "details": {
    "field": "name",
    "message": "name is required"
  }
}
```

**Example 2: Invalid Enum Value**
```json
{
  "error": "validation_error",
  "details": {
    "field": "network_type",
    "message": "invalid network_type value. Valid values: INDIVIDUAL, COMPANY"
  }
}
```

**Example 3: Invalid UUID Format**
```json
{
  "error": "validation_error",
  "details": {
    "field": "idempotency_key",
    "message": "invalid UUID format"
  }
}
```

**Example 4: Geographic Location Invalid**
```json
{
  "error": "validation_error",
  "details": {
    "field": "location",
    "message": "invalid location format. Must be GeoJSON Point with coordinates [longitude, latitude]"
  }
}
```

**Example 5: Count Validation Failed**
```json
{
  "error": "validation_error",
  "details": [
    {
      "field": "count_available",
      "message": "count_available cannot exceed count_total (got 5, max is 3)"
    },
    {
      "field": "count_total",
      "message": "count_total must be at least 1"
    }
  ]
}
```

---

### 2. Unauthorized (401 Unauthorized)

**Error Code**: `unauthorized`

**Description**: Request is missing authentication or authentication failed.

**Example 1: Missing Bearer Token**
```json
{
  "error": "unauthorized"
}
```

**Example 2: Invalid Bearer Token**
```json
{
  "error": "unauthorized"
}
```

**Example 3: Expired Token**
```json
{
  "error": "unauthorized"
}
```

---

### 3. Forbidden (403 Forbidden)

**Error Code**: `forbidden`

**Description**: User authenticated but lacks required permissions.

**Example 1: Role-Based Authorization**
```json
{
  "error": "forbidden",
  "details": {
    "message": "Partner scope restriction: partner cannot mutate resources owned by another partner",
    "required_role": "role:admin",
    "current_roles": "role:partner"
  }
}
```

**Example 2: Entity Ownership Violation**
```json
{
  "error": "forbidden",
  "details": {
    "message": "Partner 456 cannot mutate station OPR-789 (station belongs to partner 123)",
    "caller_partner_id": "456",
    "target_partner_id": "123"
  }
}
```

**Example 3: Cross-Partner Mutation Attempt**
```json
{
  "error": "forbidden",
  "details": {
    "message": "Partner 999 cannot create station for partner 111",
    "caller_partner_id": "999",
    "requested_partner_id": "111"
  }
}
```

---

### 4. Duplicate Request (409 Conflict)

**Error Code**: `duplicate_request`

**Description**: POST request made without Idempotency-Key header.

**Example**:
```json
{
  "error": "duplicate_request"
}
```

**Context**: The request should be retried with a valid `Idempotency-Key` header.

---

### 5. Constraint Violation (409 Conflict)

**Error Code**: `constraint_violation`

**Description**: Database constraint was violated during write operation.

**Example 1: Unique Constraint Violation**
```json
{
  "error": "constraint_violation",
  "details": {
    "constraint": "unique_partner_name",
    "message": "partner with name 'Partner Alpha' already exists",
    "violated_constraint": "partners_name_key",
    "value": "Partner Alpha"
  }
}
```

**Example 2: Foreign Key Violation**
```json
{
  "error": "constraint_violation",
  "details": {
    "constraint": "fk_station_partner",
    "message": "station must reference an existing partner",
    "violated_constraint": "inventory_stations_partner_id_fkey",
    "value": "OPR-xxxxxxxxxxxxxxxxxxxxxx"
  }
}
```

**Example 3: Uniqueness Constraint Violation**
```json
{
  "error": "constraint_violation",
  "details": {
    "constraint": "unique_connector",
    "message": "station STA-123 already has a charger with connector_type_id=2 and current_type_id=2",
    "violated_constraint": "unique_connector",
    "target_id": "STA-123",
    "connector_type_id": 2,
    "current_type_id": 2
  }
}
```

**Example 4: Check Constraint Violation**
```json
{
  "error": "constraint_violation",
  "details": {
    "constraint": "count_validation",
    "message": "count_available (5) cannot exceed count_total (3)",
    "violated_constraint": "inventory_chargers_count_check",
    "count_available": 5,
    "count_total": 3
  }
}
```

---

### 6. Entity Not Found (404 Not Found)

**Error Code**: `not_found`

**Description**: Requested entity doesn't exist.

**Example 1: Partner Not Found**
```json
{
  "error": "not_found",
  "details": {
    "entity_type": "partner",
    "entity_id": "OPR-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Partner OPR-xxxxxxxxxxxxxxxxxxxxxx not found"
  }
}
```

**Example 2: Station Not Found**
```json
{
  "error": "not_found",
  "details": {
    "entity_type": "station",
    "entity_id": "STA-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Station STA-xxxxxxxxxxxxxxxxxxxxxx not found"
  }
}
```

**Example 3: Charger Not Found**
```json
{
  "error": "not_found",
  "details": {
    "entity_type": "charger",
    "entity_id": "CHG-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Charger CHG-xxxxxxxxxxxxxxxxxxxxxx not found"
  }
}
```

**Example 4: Station Not Found for Charger**
```json
{
  "error": "not_found",
  "details": {
    "entity_type": "charger",
    "entity_id": "CHG-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Station STA-xxxxxxxxxxxxxxxxxxxxxx (charger's parent) not found"
  }
}
```

---

### 7. Entity Deleted (410 Gone)

**Error Code**: `entity_deleted`

**Description**: Requested entity exists but has been soft-deleted.

**Example 1: Partner Soft-Deleted**
```json
{
  "error": "entity_deleted",
  "details": {
    "entity_type": "partner",
    "entity_id": "OPR-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Partner OPR-xxxxxxxxxxxxxxxxxxxxxx has been deleted",
    "deleted_at": "2026-06-19T10:00:00Z"
  }
}
```

**Example 2: Station Soft-Deleted**
```json
{
  "error": "entity_deleted",
  "details": {
    "entity_type": "station",
    "entity_id": "STA-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Station STA-xxxxxxxxxxxxxxxxxxxxxx has been deleted",
    "deleted_at": "2026-06-19T10:00:00Z"
  }
}
```

**Example 3: Charger Soft-Deleted**
```json
{
  "error": "entity_deleted",
  "details": {
    "entity_type": "charger",
    "entity_id": "CHG-xxxxxxxxxxxxxxxxxxxxxx",
    "message": "Charger CHG-xxxxxxxxxxxxxxxxxxxxxx has been deleted",
    "deleted_at": "2026-06-19T10:00:00Z"
  }
}
```

**Note**: This is distinct from "Not Found" (404) because the entity existed but has been marked as deleted.

---

### 8. Internal Server Error (500 Internal Server Error)

**Error Code**: `internal_error`

**Description**: Unexpected server error occurred.

**Example**:
```json
{
  "error": "internal_error"
}
```

**Context**: This indicates a bug in the application or infrastructure failure. The error message should not contain sensitive information (per constitution: "No raw SQL strings — sqlx::query! macros only").

**Common Causes**:
- Database connection failure
- Transaction rollback failure
- Unexpected exception in repository layer
- Authorization middleware misconfiguration

**Example with Additional Context** (development only):
```json
{
  "error": "internal_error"
}
```

---

### 9. Redis Cache Bust Failure (200 OK - Non-Fatal)

**Error Type**: Non-fatal, logged as warning

**Description**: Redis cache bust failed but database operation succeeded.

**Example Response**:
```json
{
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Partner Alpha",
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T11:00:00Z"
}
```

**Headers**:
```
HTTP/1.1 200 OK
Content-Type: application/json
X-Cache-Bust-Failed: true
```

**Structured Warning Log**:
```json
{
  "level": "warn",
  "timestamp": "2026-06-19T11:00:00Z",
  "component": "redis_cache_bust",
  "operation": "delete",
  "key": "stations:tile:1:1:1",
  "error": "connection refused",
  "source": "admin-service",
  "trace_id": "abc-123-def"
}
```

**Failure Policy**:
- Redis bust failure does NOT rollback the database transaction (per constitution)
- Logs structured warning (level: "warn")
- Sets `X-Cache-Bust-Failed: true` header on response
- Returns successful response to client
- Stale data corrects on next successful write or TTL expiry

---

## Error Handling Best Practices

### Client-Side Error Handling

**Example 1: Validation Error**
```javascript
// POST /api/v1/admin/partner
fetch('/api/v1/admin/partner', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`,
    'Idempotency-Key': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
  },
  body: JSON.stringify({
    name: '',
    network_type: 'COMPANY'
  })
})
.then(response => {
  if (response.status === 400) {
    return response.json().then(error => {
      console.error('Validation error:', error);
      // Display error to user
    });
  }
  // Handle other status codes...
});
```

**Example 2: Forbidden Error**
```javascript
fetch('/api/v1/admin/partner', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`,
    'Idempotency-Key': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
  },
  body: JSON.stringify({
    name: 'Partner Alpha',
    network_type: 'COMPANY'
  })
})
.then(response => {
  if (response.status === 403) {
    return response.json().then(error => {
      console.error('Forbidden:', error.details.message);
      // Redirect to dashboard with error message
    });
  }
  // Handle other status codes...
});
```

**Example 3: Duplicate Request**
```javascript
fetch('/api/v1/admin/partner', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`,
    'Idempotency-Key': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
  },
  body: JSON.stringify({
    name: 'Partner Alpha',
    network_type: 'COMPANY'
  })
})
.then(response => {
  if (response.status === 409 && response.headers.get('Idempotency-Replayed') === 'true') {
    console.info('Request was replayed (duplicate)');
    // Use cached response
  } else if (response.status === 409) {
    console.error('Duplicate request (missing Idempotency-Key)');
    // Prompt user to retry with Idempotency-Key header
  }
  // Handle other status codes...
});
```

---

## Error Response Headers

| Header | Value | Purpose |
|--------|-------|---------|
| `X-Cache-Bust-Failed: true` | true/false | Indicates Redis cache bust failed (non-fatal) |
| `Idempotency-Replayed: true` | true/false | Indicates request was replayed with existing key |

---

## Error Code Reference

| Error Code | Status | Type | Should Client Retry? |
|------------|--------|------|---------------------|
| `validation_error` | 400 | Client error | Yes, fix validation |
| `unauthorized` | 401 | Client error | Yes, re-authenticate |
| `forbidden` | 403 | Client error | Yes, check permissions |
| `duplicate_request` | 409 | Client error | Yes, add Idempotency-Key |
| `constraint_violation` | 409 | Client error | Yes, fix data (e.g., unique name) |
| `not_found` | 404 | Client error | No, resource doesn't exist |
| `entity_deleted` | 410 | Client error | No, handle gracefully |
| `internal_error` | 500 | Server error | No, server will retry |

---

## Summary

This error contract defines:
- 8 error types with unique status codes and error codes
- Clear error messages with optional `details` field
- Consistent JSON response format
- Structured logging for Redis cache bust failures
- Client-side retry guidelines
- Non-fatal cache bust failures (200 OK with warning header)

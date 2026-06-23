# API STANDARDS

**Last Updated**: 2026-06-23  
**Status**: Active  
**Version**: 1.0.0

---

## 1. Overview

This document defines the API standards for all BorneMap services. These standards ensure consistency, predictability, and maintainability across all endpoints.

### Core Principles

1. **Consistency**: All endpoints follow the same patterns
2. **Clarity**: Response format is explicit and unambiguous
3. **Composability**: Clients can reliably combine endpoints
4. **Discoverability**: API is self-documenting

---

## 2. Base Rules

### 2.1 API Versioning

- All APIs MUST use versioned base path: `/api/v1`
- Version MUST be in the path, not headers
- Current version: `v1`
- New versions increment: `v2`, `v3`, etc.

### 2.2 Resource Naming

- Use **plural nouns** for resources
- Use **lowercase** with hyphens for multi-word resources

✅ **Correct**:
```
/api/v1/partners
/api/v1/stations
/api/v1/chargers
/api/v1/charge-sessions
```

❌ **Incorrect**:
```
/api/v1/partner              # singular
/api/v1/Stations             # capitalized
/api/v1/charger_sessions     # underscore
```

### 2.3 HTTP Methods

| Method | Meaning | Idempotent | Safe |
|--------|---------|-----------|------|
| **GET** | Retrieve resource(s) | Yes | Yes |
| **POST** | Create new resource | No | No |
| **PUT** | Replace entire resource | Yes | No |
| **PATCH** | Update resource fields | No | No |
| **DELETE** | Delete resource | Yes | No |

### 2.4 Status Codes

| Code | Meaning | Use Case |
|------|---------|----------|
| **200** | OK | Successful GET, PUT, PATCH |
| **201** | Created | Successful POST (new resource) |
| **204** | No Content | Successful DELETE |
| **400** | Bad Request | Invalid input, validation error |
| **401** | Unauthorized | Missing/invalid credentials |
| **403** | Forbidden | Authenticated but not authorized |
| **404** | Not Found | Resource doesn't exist |
| **409** | Conflict | Business rule violation |
| **500** | Server Error | Unexpected error |
| **503** | Service Unavailable | Database down, maintenance |

---

## 3. Standard Response Format

### 3.1 Success Response

All successful responses follow this format:

```json
{
  "success": true,
  "data": {
    "id": "PRT-abc123def456",
    "name": "ACME Corp",
    "email": "contact@acme.com",
    "status": "ACTIVE",
    "created_at": "2026-06-23T10:30:00Z",
    "updated_at": "2026-06-23T10:30:00Z"
  },
  "error": null
}
```

**Rules**:
- `success` MUST be `true`
- `data` MUST contain the resource or list
- `error` MUST be `null`
- Response MUST be valid JSON

### 3.2 Error Response

All error responses follow this format:

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "INVALID_INPUT",
    "message": "Email address is required",
    "timestamp": "2026-06-23T10:30:00Z"
  }
}
```

**Rules**:
- `success` MUST be `false`
- `data` MUST be `null`
- `error` MUST contain `code` and `message`
- `code` MUST be screaming_snake_case
- Include `timestamp` for debugging

### 3.3 List Response

```json
{
  "success": true,
  "data": [
    { "id": "PRT-...", "name": "...", "status": "..." },
    { "id": "PRT-...", "name": "...", "status": "..." }
  ],
  "error": null,
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 150,
    "pages": 8
  }
}
```

**Rules**:
- List endpoints include `pagination` metadata
- Default limit: 20
- Maximum limit: 100

### 3.4 Empty Response

```json
{
  "success": true,
  "data": null,
  "error": null
}
```

**Use for**: DELETE operations, operations with no return value

---

## 4. Request Format

### 4.1 Request Headers

All requests SHOULD include:

```http
Content-Type: application/json
Accept: application/json
```

### 4.2 Request Body

```json
{
  "name": "ACME Corp",
  "email": "contact@acme.com"
}
```

**Rules**:
- Use camelCase for field names (mapped from snake_case in database)
- Validate all required fields
- Return 400 Bad Request for validation errors
- Include field-specific error messages

### 4.3 Request Validation

```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Validation failed",
    "details": {
      "email": "Invalid email format",
      "name": "Name is required"
    },
    "timestamp": "2026-06-23T10:30:00Z"
  }
}
```

---

## 5. Endpoint Patterns

### 5.1 CRUD Operations

#### Create (POST)

```
POST /api/v1/partners
Content-Type: application/json

{
  "name": "ACME Corp",
  "email": "contact@acme.com"
}

Response: 201 Created
{
  "success": true,
  "data": {
    "id": "PRT-abc123def456",
    "name": "ACME Corp",
    "email": "contact@acme.com",
    "status": "ACTIVE",
    "created_at": "2026-06-23T10:30:00Z",
    "updated_at": "2026-06-23T10:30:00Z"
  },
  "error": null
}
```

#### Read (GET)

```
GET /api/v1/partners/PRT-abc123def456

Response: 200 OK
{
  "success": true,
  "data": {
    "id": "PRT-abc123def456",
    "name": "ACME Corp",
    "email": "contact@acme.com",
    "status": "ACTIVE",
    "created_at": "2026-06-23T10:30:00Z",
    "updated_at": "2026-06-23T10:30:00Z"
  },
  "error": null
}
```

#### Update (PATCH)

```
PATCH /api/v1/partners/PRT-abc123def456
Content-Type: application/json

{
  "name": "ACME Corporation",
  "status": "INACTIVE"
}

Response: 200 OK
{
  "success": true,
  "data": {
    "id": "PRT-abc123def456",
    "name": "ACME Corporation",
    "email": "contact@acme.com",
    "status": "INACTIVE",
    "created_at": "2026-06-23T10:30:00Z",
    "updated_at": "2026-06-23T10:35:00Z"
  },
  "error": null
}
```

#### Delete (DELETE)

```
DELETE /api/v1/partners/PRT-abc123def456

Response: 204 No Content
(or with soft delete confirmation)

Response: 200 OK
{
  "success": true,
  "data": { "deleted": true },
  "error": null
}
```

#### List (GET)

```
GET /api/v1/partners?page=1&limit=20&status=ACTIVE

Response: 200 OK
{
  "success": true,
  "data": [
    { "id": "PRT-...", "name": "...", "status": "..." },
    { "id": "PRT-...", "name": "...", "status": "..." }
  ],
  "error": null,
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 150,
    "pages": 8
  }
}
```

### 5.2 Query Parameters

| Parameter | Type | Required | Example |
|-----------|------|----------|---------|
| `page` | integer | No | `?page=1` |
| `limit` | integer | No | `?limit=20` |
| `sort_by` | string | No | `?sort_by=name` |
| `sort_order` | asc/desc | No | `?sort_order=asc` |
| `filter_*` | varies | No | `?filter_status=ACTIVE` |

### 5.3 Filtering

```
GET /api/v1/partners?filter_status=ACTIVE&filter_email=acme

Filter format: ?filter_<field>=<value>

Multiple filters: AND logic
```

### 5.4 Sorting

```
GET /api/v1/partners?sort_by=name&sort_order=asc

Valid fields: name, created_at, updated_at, status
Order: asc | desc (default: asc)
```

### 5.5 Pagination

```
GET /api/v1/partners?page=2&limit=50

Defaults: page=1, limit=20
Maximum limit: 100
```

---

## 6. Identifier Rules

### 6.1 External IDs

- Partners: `PRT-<12 chars>`
- Stations: `STA-<12 chars>`
- Chargers: `CHR-<12 chars>`

✅ **Correct**: `PRT-abc123def456`
❌ **Wrong**: `12345` (numeric), `prt-...` (lowercase prefix)

### 6.2 ID Exposure

- **ONLY expose external IDs in API**
- Internal numeric IDs MUST NOT be exposed
- Never expose UUIDs or random identifiers

✅ **Correct**: `"id": "PRT-abc123def456"`
❌ **Wrong**: `"uuid": "550e8400-e29b-41d4-a716-446655440000"`

### 6.3 ID in URLs

- ID goes in URL path
- Example: `GET /api/v1/partners/PRT-abc123def456`
- Case-sensitive (exact match required)

---

## 7. Error Codes Reference

### 7.1 Common Error Codes

| Code | Status | Meaning |
|------|--------|---------|
| `INVALID_INPUT` | 400 | Request body validation failed |
| `RESOURCE_NOT_FOUND` | 404 | Resource doesn't exist |
| `BUSINESS_RULE_VIOLATION` | 409 | Business logic constraint violated |
| `UNAUTHORIZED` | 401 | Missing/invalid authentication |
| `FORBIDDEN` | 403 | Lacks permission for resource |
| `DUPLICATE_ENTRY` | 409 | Unique constraint violated |
| `INTERNAL_ERROR` | 500 | Unexpected server error |
| `SERVICE_UNAVAILABLE` | 503 | Database/external service down |

---

## 8. Backward Compatibility

### 8.1 Versioning Strategy

- Add fields to response: OK (clients ignore unknown fields)
- Remove fields from response: Break - new version required
- Change field types: Break - new version required
- Change HTTP methods: Break - new version required

### 8.2 Deprecation Process

1. Announce deprecation in release notes
2. Support old endpoint for 2+ versions
3. Document migration path
4. Remove in major version bump

---

## 9. Security

### 9.1 Input Validation

- ✅ Validate all inputs on server
- ✅ Reject oversized payloads (max 1MB)
- ✅ Sanitize string inputs
- ✅ Type-check numeric inputs

### 9.2 Rate Limiting

- Plan: Implement rate limiting per IP/user
- Current: No rate limiting (for MVP)
- Future: 1000 requests/hour per IP

### 9.3 CORS

```rust
// Allowed origins
let allowed_origins = vec![
    "http://localhost:3000",
    "https://dashboard.bornemap.com",
];
```

---

## 10. Documentation

### 10.1 OpenAPI Specification

All endpoints documented in:
- `specs/001-ev-dashboard/contracts/api.yaml` (OpenAPI 3.0)

### 10.2 Auto-Generated Docs

- Swagger UI: `/api/docs`
- ReDoc: `/api/redoc`
- OpenAPI JSON: `/api/openapi.json`

---

## 11. Examples

### 11.1 Complete Flow: Create Partner

**Request**:
```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ACME Corp",
    "email": "contact@acme.com"
  }'
```

**Response**:
```json
{
  "success": true,
  "data": {
    "id": "PRT-7f8g9h0i1j2k",
    "name": "ACME Corp",
    "email": "contact@acme.com",
    "status": "ACTIVE",
    "created_at": "2026-06-23T10:30:00Z",
    "updated_at": "2026-06-23T10:30:00Z"
  },
  "error": null
}
```

### 11.2 Complete Flow: Error Response

**Request** (invalid email):
```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ACME Corp",
    "email": "not-an-email"
  }'
```

**Response**:
```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "INVALID_INPUT",
    "message": "Validation failed",
    "details": {
      "email": "Must be valid email format"
    },
    "timestamp": "2026-06-23T10:30:00Z"
  }
}
```

---

## 12. Checklist for New Endpoints

- ✅ Uses `/api/v1` base path
- ✅ Uses plural resource name
- ✅ Returns standard response format
- ✅ Uses correct HTTP status codes
- ✅ Validates all inputs
- ✅ Documents in OpenAPI spec
- ✅ Handles errors gracefully
- ✅ Supports filtering/sorting (for GET list)
- ✅ Supports pagination (for GET list)
- ✅ Never exposes internal IDs

---

## 13. See Also

- [Architecture](./architecture.md) - Presentation layer specification
- [Conventions](./conventions.md) - ID and naming conventions
- [Error Taxonomy](./error-taxonomy.md) - Error handling strategy
- [specs/001-ev-dashboard/contracts/api.yaml](../../specs/001-ev-dashboard/contracts/api.yaml) - OpenAPI specification
- [docs/epics/E001-dashboard-core/api.md](../../docs/epics/E001-dashboard-core/api.md) - API endpoint details

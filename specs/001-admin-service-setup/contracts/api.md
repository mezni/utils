# API Contract: Admin Service (Sprint 1.1)

**Date**: 2026-06-19 | **Phase**: 1 — Design & Contracts

**Source of Truth**: `api/openapi/admin.yaml`

## Base URL

```
http://localhost:3002
```

## Authentication

Authorization is deferred until the Auth Service sprint. All endpoints are accessible without authentication in Sprint 1.1.

## Endpoints

### GET /health

Health check endpoint. Returns service status.

**Response** (200):
```json
{
  "status": "healthy",
  "service": "admin-service",
  "version": "1.0.0"
}
```

---

### POST /partners

Create a new partner/operator.

**Request Body**:
```json
{
  "name": "Demo Operator",
  "network_type": "COMPANY",
  "support_phone": "+21612345678",
  "support_email": "contact@demo.tn"
}
```

**Response** (201):
```json
{
  "id": "OPR-k8F3aZ91LmQx",
  "name": "Demo Operator",
  "network_type": "COMPANY",
  "support_phone": "+21612345678",
  "support_email": "contact@demo.tn",
  "is_verified": false,
  "created_at": "2026-06-19T12:00:00Z",
  "updated_at": "2026-06-19T12:00:00Z"
}
```

**Errors**: 400 (validation), 409 (duplicate name)

---

### GET /partners

List all partners (excluding soft-deleted).

**Query Parameters**:
- `page` (int, default: 1)
- `limit` (int, default: 20, max: 100)

**Response** (200):
```json
{
  "data": [
    {
      "id": "OPR-k8F3aZ91LmQx",
      "name": "Demo Operator",
      "network_type": "COMPANY",
      "is_verified": false,
      "station_count": 0,
      "created_at": "2026-06-19T12:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

---

### GET /partners/{id}

Get a single partner by ID.

**Path Parameters**:
- `id` (string): Partner ID in format OPR-*

**Response** (200):
```json
{
  "id": "OPR-k8F3aZ91LmQx",
  "name": "Demo Operator",
  "network_type": "COMPANY",
  "support_phone": "+21612345678",
  "support_email": "contact@demo.tn",
  "is_verified": false,
  "created_at": "2026-06-19T12:00:00Z",
  "updated_at": "2026-06-19T12:00:00Z"
}
```

**Errors**: 404 (not found)

---

### PATCH /partners/{id}

Partially update a partner. Only send fields that need to change. Null values clear optional fields.

**Request Body** (all fields optional):
```json
{
  "name": "Updated Operator",
  "support_phone": null
}
```

**Response** (200): Updated partner object (same schema as GET)

**Errors**: 400 (validation), 404 (not found)

---

### DELETE /partners/{id}

Soft-delete a partner. Sets `deleted_at` timestamp. No restore endpoint available.

**Response**: 204 (No Content)

**Errors**: 404 (not found)

---

### POST /stations

Create a new charging station.

**Request Body**:
```json
{
  "partner_id": "OPR-k8F3aZ91LmQx",
  "name": "Downtown Charging Hub",
  "address": "123 Main St, Tunis",
  "location": {
    "lat": 36.8065,
    "lon": 10.1815
  }
}
```

**Response** (201):
```json
{
  "id": "STA-9xQa2Lp0VmZk",
  "partner_id": "OPR-k8F3aZ91LmQx",
  "name": "Downtown Charging Hub",
  "address": "123 Main St, Tunis",
  "location": {"lat": 36.8065, "lon": 10.1815},
  "created_at": "2026-06-19T12:00:00Z",
  "updated_at": "2026-06-19T12:00:00Z"
}
```

**Errors**: 400 (validation, invalid coordinates), 404 (partner_id not found)

---

### GET /stations

List all stations (excluding soft-deleted).

**Query Parameters**:
- `page` (int, default: 1)
- `limit` (int, default: 20, max: 100)
- `partner_id` (string, optional filter)

**Response** (200): Paginated station list

---

### GET /stations/{id}

Get a single station by ID.

**Response** (200): Full station object with partner name

---

### PATCH /stations/{id}

Partially update a station.

**Request Body** (all fields optional):
```json
{
  "name": "Updated Station Name"
}
```

**Response** (200): Updated station object

---

### DELETE /stations/{id}

Soft-delete a station. Propagates `deleted_at` to all associated chargers (logical cascade).

**Response**: 204 (No Content)

---

### POST /chargers

Create a new charger at a station.

**Request Body**:
```json
{
  "station_id": "STA-9xQa2Lp0VmZk",
  "connector_type": "CCS",
  "current_type": "DC",
  "power_kw": 150.0,
  "voltage": 400,
  "amperage": 375,
  "count_available": 1,
  "count_total": 1
}
```

**Response** (201): Charger object with CHG-* ID

---

### GET /chargers

List all chargers (excluding soft-deleted).

**Query Parameters**:
- `page` (int, default: 1)
- `limit` (int, default: 20)
- `station_id` (string, optional filter)

---

### GET /chargers/{id}

Get a single charger by ID.

---

### PATCH /chargers/{id}

Partially update a charger.

---

### DELETE /chargers/{id}

Soft-delete a charger.

**Response**: 204 (No Content)

---

## Error Response Format

All error responses follow this schema:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request body",
    "details": {
      "field": "name",
      "reason": "Name is required"
    }
  }
}
```

### Error Codes

| HTTP Status | Code | Description |
|-------------|------|-------------|
| 400 | VALIDATION_ERROR | Invalid input data |
| 404 | NOT_FOUND | Resource does not exist |
| 409 | CONFLICT | Resource conflict (duplicate) |
| 500 | INTERNAL_ERROR | Unexpected server error |

## Rate Limiting

No rate limiting for Sprint 1.1. Will be added in a future sprint with Auth Service.

## Versioning

Base path is `/api/v1`. Future breaking changes will use `/api/v2`.

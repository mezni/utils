# Data Model: Admin Service

**Date**: 2026-06-09 | **Branch**: `010-admin-service` | **Spec**: [spec.md](./spec.md)

## Entity Relationships

```text
Partner 1───* Station 1───* Charger
Station 1───* StationAvailability (append-only)
```

## Domain Entities

### Partner

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | TEXT (PK) | Auto-generated | Prefix `PRT` via `ev_core::generate_id("PRT", ...)` |
| name | TEXT | Yes | 1–255 chars |
| type | TEXT | Yes | One of: `business`, `personal` |
| is_verified | BOOLEAN | Default: false | When true, stations visible to drivers |
| is_live | BOOLEAN | Default: false | When true, stations visible to drivers |
| is_active | BOOLEAN | Default: true | When false, excluded from driver queries |
| created_at | TIMESTAMPTZ | Auto | Set on creation |
| created_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |
| updated_at | TIMESTAMPTZ | Auto | Updated on change |
| updated_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |

**Validation**:
- name: non-empty, max 255 chars
- type: must be `business` or `personal`
- id: unique, auto-generated

**State transitions**: None (flags are independent — can be toggled in any order)

**Soft delete**: Set `is_active = false` rather than hard delete for partners (cascading to stations/chargers visibility)

---

### Station

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | TEXT (PK) | Auto-generated | Prefix `STN` via `ev_core::generate_id("STN", ...)` |
| partner_id | TEXT (FK) | Yes | References `partner.id` |
| name | TEXT | Yes | 1–255 chars |
| address | TEXT | No | Free text |
| latitude | DOUBLE PRECISION | Yes | -90 to 90 |
| longitude | DOUBLE PRECISION | Yes | -180 to 180 |
| location | GEOMETRY(Point, 4326) | Auto | Set by trigger from lat/lng |
| created_at | TIMESTAMPTZ | Auto | Set on creation |
| created_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |
| updated_at | TIMESTAMPTZ | Auto | Updated on change |
| updated_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |

**Validation**:
- name: non-empty, max 255 chars
- latitude: -90 to 90
- longitude: -180 to 180
- partner_id: must reference existing, active partner

**Cascade**: Deleting a station cascades to its chargers and availability records

---

### Charger

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | TEXT (PK) | Auto-generated | Prefix `CHG` via `ev_core::generate_id("CHG", ...)` |
| station_id | TEXT (FK) | Yes | References `station.id` |
| connector_type | TEXT | Yes | One of: `type2`, `type3`, `ccs`, `chademo` |
| power_kw | NUMERIC | Yes | > 0 |
| status | TEXT | Default: `offline` | One of: `available`, `in_use`, `maintenance`, `offline` |
| created_at | TIMESTAMPTZ | Auto | Set on creation |
| created_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |
| updated_at | TIMESTAMPTZ | Auto | Updated on change |
| updated_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |

**Validation**:
- connector_type: must be one of `type2`, `type3`, `ccs`, `chademo`
- power_kw: > 0
- status: must be one of `available`, `in_use`, `maintenance`, `offline`
- station_id: must reference existing station

**Cascade**: Deleting a charger removes its availability records (if any)

---

### StationAvailability

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | TEXT (PK) | Auto-generated | Prefix `SA` via `ev_core::generate_id("SA", ...)` |
| station_id | TEXT (FK) | Yes | References `station.id` |
| status | TEXT | Yes | One of: `available`, `partial`, `unavailable` |
| updated_by | TEXT | Yes | From `X-Partner-Id` or `"admin"` |
| updated_at | TIMESTAMPTZ | Auto | Timestamp of this record |

**Validation**:
- status: must be one of `available`, `partial`, `unavailable`
- station_id: must reference existing station

**Append-only**: Each update creates a new record. Latest record per station determined by `updated_at DESC`.

## API Request/Response Types

### Partner

**CreatePartnerRequest**:
```json
{
  "name": "string",
  "type": "business|personal",
  "is_verified": false,
  "is_live": false,
  "is_active": true
}
```

**UpdatePartnerRequest** (partial):
```json
{
  "name": "string",
  "type": "business|personal",
  "is_verified": true,
  "is_live": true,
  "is_active": true
}
```

**PartnerResponse**:
```json
{
  "id": "PRT001",
  "name": "string",
  "type": "business",
  "is_verified": true,
  "is_live": true,
  "is_active": true,
  "created_at": "2026-06-09T12:00:00Z",
  "created_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z",
  "updated_by": "admin"
}
```

**PartnerListResponse**:
```json
{
  "data": [PartnerResponse, ...],
  "total": 10,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

### Station

**CreateStationRequest**:
```json
{
  "partner_id": "PRT001",
  "name": "string",
  "address": "string",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**UpdateStationRequest** (partial):
```json
{
  "name": "string",
  "address": "string",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**StationResponse**:
```json
{
  "id": "STN001",
  "partner_id": "PRT001",
  "name": "string",
  "address": "string",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "created_at": "2026-06-09T12:00:00Z",
  "created_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z",
  "updated_by": "admin"
}
```

### Charger

**CreateChargerRequest**:
```json
{
  "station_id": "STN001",
  "connector_type": "ccs",
  "power_kw": 150.0,
  "status": "offline"
}
```

**UpdateChargerRequest** (partial):
```json
{
  "connector_type": "ccs",
  "power_kw": 200.0,
  "status": "available"
}
```

**ChargerResponse**:
```json
{
  "id": "CHG001",
  "station_id": "STN001",
  "connector_type": "ccs",
  "power_kw": 150.0,
  "status": "offline",
  "created_at": "2026-06-09T12:00:00Z",
  "created_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z",
  "updated_by": "admin"
}
```

### StationAvailability

**CreateAvailabilityRequest**:
```json
{
  "status": "available"
}
```

**AvailabilityResponse**:
```json
{
  "id": "SA001",
  "station_id": "STN001",
  "status": "available",
  "updated_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z"
}
```

### Error

```json
{
  "error": {
    "code": "not_found",
    "message": "Partner PRT999 not found"
  }
}
```

**Error codes**: `not_found`, `validation_error`, `bad_request`, `conflict`, `internal_error`, `db_error`

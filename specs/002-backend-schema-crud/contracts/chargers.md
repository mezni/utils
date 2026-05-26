# API Contracts: Chargers

**Base Path**: `/api/v1/stations/{station_id}/chargers`

All charger endpoints are scoped under a parent station.

## Create Charger

`POST /api/v1/stations/{station_id}/chargers`

**Request Body**:
```json
{
  "connector_type_id": "CNT-a1b2c3d4e5f6",
  "power_kw": 22.0,
  "current_type": "AC"
}
```

Station must exist and not be soft-deleted.

**Success Response** (201 Created):
```json
{
  "id": "CHG-x7q3m1k9p2v4",
  "station_id": "STN-k4m2n9p1q5v8",
  "connector_type_id": "CNT-a1b2c3d4e5f6",
  "power_kw": 22.0,
  "current_type": "AC",
  "status": "available",
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `404 Not Found`: Station does not exist
- `422 Unprocessable Entity`: Validation error (connector_type_id not found, invalid power_kw, missing fields)

---

## List Chargers

`GET /api/v1/stations/{station_id}/chargers`

**Query Parameters**:
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `cursor` | string | null | Pagination cursor |
| `limit` | integer | 50 | Page size (max 100) |

**Success Response** (200 OK):
```json
{
  "data": [
    {
      "id": "CHG-x7q3m1k9p2v4",
      "station_id": "STN-k4m2n9p1q5v8",
      "connector_type_id": "CNT-a1b2c3d4e5f6",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available",
      "created_at": "2026-05-26T10:00:00.123456Z",
      "updated_at": "2026-05-26T10:00:00.123456Z"
    }
  ],
  "pagination": {
    "next_cursor": null,
    "has_more": false
  }
}
```

**Error Responses**:
- `404 Not Found`: Station does not exist

---

## Update Charger

`PATCH /api/v1/stations/{station_id}/chargers/{id}`

**Request Body** (partial update):
```json
{
  "status": "faulted",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

`updated_at` is required for optimistic locking.

**Success Response** (200 OK): Full charger with advanced `updated_at`.

**Error Responses**:
- `404 Not Found`: Charger or station does not exist
- `409 Conflict`: Concurrent modification
- `422 Unprocessable Entity`

---

## Delete Charger (Permanent)

`DELETE /api/v1/stations/{station_id}/chargers/{id}`

Chargers are permanently deleted (no soft-delete).

**Success Response** (204 No Content): Empty body

**Error Responses**:
- `404 Not Found`: Charger does not exist

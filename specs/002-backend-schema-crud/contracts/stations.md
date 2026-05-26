# API Contracts: Stations

**Base Path**: `/api/v1/stations`

## Create Station

`POST /api/v1/stations`

**Request Body**:
```json
{
  "owner_id": "USR-p2v4x7q3m1k9",
  "name": "Tunis Central Station",
  "address": "12 Avenue Habib Bourguiba",
  "city": "Tunis",
  "longitude": 10.1815,
  "latitude": 36.8065
}
```

`owner_id` must reference a user with `role IN ('partner', 'admin')`. Driver-role users are rejected.

**Success Response** (201 Created):
```json
{
  "id": "STN-k4m2n9p1q5v8",
  "owner_id": "USR-p2v4x7q3m1k9",
  "name": "Tunis Central Station",
  "address": "12 Avenue Habib Bourguiba",
  "city": "Tunis",
  "longitude": 10.1815,
  "latitude": 36.8065,
  "is_operational": true,
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `409 Conflict`: N/A (no unique constraint beyond ID)
- `422 Unprocessable Entity`: Validation error (owner not partner/admin, invalid coordinates, missing fields)

---

## List Stations

`GET /api/v1/stations`

**Query Parameters**:
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `cursor` | string | null | Pagination cursor |
| `limit` | integer | 50 | Page size (max 100) |
| `include_test` | boolean | false | Include test records |

**Partner-scoped behavior**: When the request is authenticated as a partner, only
stations owned by that partner are returned. The `owner_id` is injected from the
JWT claims automatically — no query parameter needed.

**Success Response** (200 OK):
```json
{
  "data": [
    {
      "id": "STN-k4m2n9p1q5v8",
      "owner_id": "USR-p2v4x7q3m1k9",
      "name": "Tunis Central Station",
      "address": "12 Avenue Habib Bourguiba",
      "city": "Tunis",
      "longitude": 10.1815,
      "latitude": 36.8065,
      "is_operational": true,
      "is_test": false,
      "created_at": "2026-05-26T10:00:00.123456Z",
      "updated_at": "2026-05-26T10:00:00.123456Z"
    }
  ],
  "pagination": {
    "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNS0yNlQxMDowMDowMC4xMjM0NTZaIiwiaWQiOiJTVE4tazRtMm45cDFxNXY4In0",
    "has_more": true
  }
}
```

---

## Get Station

`GET /api/v1/stations/{id}`

**Success Response** (200 OK): Same shape as single item in list.

**Error Responses**:
- `404 Not Found`: Station does not exist or is soft-deleted
- `403 Forbidden`: Partner accessing station owned by another partner

---

## Update Station

`PATCH /api/v1/stations/{id}`

**Request Body** (partial update):
```json
{
  "name": "Tunis Central (Updated)",
  "is_operational": false,
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

`updated_at` is required for optimistic locking.

**Success Response** (200 OK): Full station with advanced `updated_at`.

**Error Responses**:
- `404 Not Found`
- `409 Conflict`: Concurrent modification
- `422 Unprocessable Entity`

---

## Remove Station (Soft-Delete)

`DELETE /api/v1/stations/{id}`

On removal, all associated chargers are permanently deleted (cascade).

**Success Response** (204 No Content): Empty body

**Error Responses**:
- `404 Not Found`

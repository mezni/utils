# API Contracts: Connector Types

**Base Path**: `/api/v1/connector-types`

## Create Connector Type

`POST /api/v1/connector-types`

**Request Body**:
```json
{
  "name": "Type 2 AC",
  "description": "IEC 62196 Type 2 connector for AC charging up to 43 kW"
}
```

**Success Response** (201 Created):
```json
{
  "id": "CNT-a1b2c3d4e5f6",
  "name": "Type 2 AC",
  "description": "IEC 62196 Type 2 connector for AC charging up to 43 kW",
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `409 Conflict`: Name already exists
- `422 Unprocessable Entity`: Validation error

---

## List Connector Types

`GET /api/v1/connector-types`

**Query Parameters**:
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `cursor` | string | null | Pagination cursor |
| `limit` | integer | 50 | Page size (max 100) |
| `include_test` | boolean | false | Include test records |

**Success Response** (200 OK):
```json
{
  "data": [
    {
      "id": "CNT-a1b2c3d4e5f6",
      "name": "Type 2 AC",
      "description": "IEC 62196 Type 2 connector for AC charging up to 43 kW",
      "is_test": false,
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

---

## Get Connector Type

`GET /api/v1/connector-types/{id}`

**Success Response** (200 OK): Same shape as single item in list.

**Error Responses**:
- `404 Not Found`

---

## Update Connector Type

`PATCH /api/v1/connector-types/{id}`

**Request Body** (partial update):
```json
{
  "description": "Updated description",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

`updated_at` is required for optimistic locking.

**Success Response** (200 OK): Full connector type with advanced `updated_at`.

**Error Responses**:
- `404 Not Found`
- `409 Conflict`: Name conflict or concurrent modification
- `422 Unprocessable Entity`

---

## Remove Connector Type (Soft-Delete)

`DELETE /api/v1/connector-types/{id}`

Removal is blocked if any charger references this connector type.

**Success Response** (204 No Content): Empty body

**Error Responses**:
- `404 Not Found`
- `409 Conflict`: Connector type is referenced by existing chargers

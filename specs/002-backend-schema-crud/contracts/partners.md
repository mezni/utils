# API Contracts: Partner Profiles

**Base Path**: `/api/v1/partners`

## Create Partner Profile

`POST /api/v1/partners`

**Request Body**:
```json
{
  "user_id": "USR-p2v4x7q3m1k9",
  "classification": "business",
  "display_name": "TunisEV Solutions",
  "tax_id": "TN123456789",
  "contact_phone": "+216 71 234 567"
}
```

`user_id` must reference a user with `role = 'partner'`.

**Success Response** (201 Created):
```json
{
  "id": "PRT-k4m2n9p1q5v8",
  "user_id": "USR-p2v4x7q3m1k9",
  "classification": "business",
  "display_name": "TunisEV Solutions",
  "tax_id": "TN123456789",
  "contact_phone": "+216 71 234 567",
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `409 Conflict`: User already has a partner profile
- `422 Unprocessable Entity`: Validation error (user not partner role, missing fields)

---

## List Partner Profiles

`GET /api/v1/partners`

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
      "id": "PRT-k4m2n9p1q5v8",
      "user_id": "USR-p2v4x7q3m1k9",
      "classification": "business",
      "display_name": "TunisEV Solutions",
      "tax_id": "TN123456789",
      "contact_phone": "+216 71 234 567",
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

## Get Partner Profile

`GET /api/v1/partners/{id}`

**Success Response** (200 OK): Same shape as single item in list.

**Error Responses**:
- `404 Not Found`: Partner profile does not exist or is soft-deleted

---

## Update Partner Profile

`PATCH /api/v1/partners/{id}`

**Request Body** (partial update):
```json
{
  "display_name": "TunisEV Solutions SA",
  "contact_phone": "+216 71 999 999",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

`updated_at` is required for optimistic locking.

**Success Response** (200 OK): Full partner profile with advanced `updated_at`.

**Error Responses**:
- `404 Not Found`
- `409 Conflict`: Concurrent modification
- `422 Unprocessable Entity`

---

## Remove Partner Profile (Soft-Delete)

`DELETE /api/v1/partners/{id}`

**Success Response** (204 No Content): Empty body

**Error Responses**:
- `404 Not Found`

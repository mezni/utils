# API Contracts: Users

**Base Path**: `/api/v1/users`

## Create User

`POST /api/v1/users`

**Request Body**:
```json
{
  "email": "admin@bornemap.tn",
  "username": "admin1",
  "password": "securepass123",
  "role": "admin"
}
```

**Success Response** (201 Created):
```json
{
  "id": "USR-m1k9p2v4x7q3",
  "email": "admin@bornemap.tn",
  "username": "admin1",
  "role": "admin",
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `409 Conflict`: Email or username already exists
- `422 Unprocessable Entity`: Validation error (invalid email, short password, missing fields)

---

## List Users

`GET /api/v1/users`

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
      "id": "USR-m1k9p2v4x7q3",
      "email": "admin@bornemap.tn",
      "username": "admin1",
      "role": "admin",
      "is_test": false,
      "created_at": "2026-05-26T10:00:00.123456Z",
      "updated_at": "2026-05-26T10:00:00.123456Z"
    }
  ],
  "pagination": {
    "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNS0yNlQxMDowMDowMC4xMjM0NTZaIiwiaWQiOiJVU1ItbTFrOXAydjR4N3EzIn0",
    "has_more": true
  }
}
```

---

## Get User

`GET /api/v1/users/{id}`

**Success Response** (200 OK):
```json
{
  "id": "USR-m1k9p2v4x7q3",
  "email": "admin@bornemap.tn",
  "username": "admin1",
  "role": "admin",
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

**Error Responses**:
- `404 Not Found`: User does not exist or is soft-deleted

---

## Update User

`PATCH /api/v1/users/{id}`

**Request Body** (partial update — only included fields are modified):
```json
{
  "username": "admin_updated",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

`updated_at` is required for optimistic locking.

**Success Response** (200 OK):
```json
{
  "id": "USR-m1k9p2v4x7q3",
  "email": "admin@bornemap.tn",
  "username": "admin_updated",
  "role": "admin",
  "is_test": false,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:05:00.789012Z"
}
```

**Error Responses**:
- `404 Not Found`: User does not exist or is soft-deleted
- `409 Conflict`: Concurrent modification (updated_at mismatch)
- `422 Unprocessable Entity`: Validation error

---

## Remove User (Soft-Delete)

`DELETE /api/v1/users/{id}`

**Success Response** (204 No Content): Empty body

**Error Responses**:
- `404 Not Found`: User does not exist or already soft-deleted

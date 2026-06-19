# Contract: GET /api/v1/auth/me

## Request

```http
GET /api/v1/auth/me HTTP/1.1
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

### Fields

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `Authorization` | header | yes | Bearer access token |

## Response: 200 OK

```json
{
  "id": "USR-abc123...",
  "email": "user@example.com",
  "first_name": "John",
  "last_name": "Doe",
  "roles": ["role:admin"],
  "created_at": "2026-06-19T00:00:00Z",
  "updated_at": "2026-06-19T00:00:00Z"
}
```

## Response: 401 Unauthorized

```json
{
  "error": "invalid_token"
}
```

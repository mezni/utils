# Contract: POST /api/v1/auth/logout

## Request

```http
POST /api/v1/auth/logout HTTP/1.1
Content-Type: application/json

{
  "refresh_token": "eyJhbGciOiJSUzI1NiIs..."
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `refresh_token` | string | yes | Refresh token to revoke |

## Response: 200 OK

```json
{
  "message": "logged_out"
}
```

Idempotent: returns 200 even if the token was already expired or revoked.

## Response: 400 Bad Request

```json
{
  "error": "validation_error",
  "details": [
    { "field": "refresh_token", "message": "refresh_token is required" }
  ]
}
```

## Response: 503 Service Unavailable

```json
{
  "error": "auth_unavailable"
}
```

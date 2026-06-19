# Contract: POST /api/v1/auth/refresh

## Request

```http
POST /api/v1/auth/refresh HTTP/1.1
Content-Type: application/json

{
  "refresh_token": "eyJhbGciOiJSUzI1NiIs..."
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `refresh_token` | string | yes | Valid refresh token from a previous login or refresh |

## Response: 200 OK

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 300,
  "refresh_expires_in": 1800,
  "token_type": "Bearer"
}
```

## Response: 400 Bad Request

```json
{
  "error": "validation_error",
  "details": [
    { "field": "refresh_token", "message": "refresh_token is required" }
  ]
}
```

## Response: 401 Unauthorized

```json
{
  "error": "token_expired"
}
```

## Response: 503 Service Unavailable

```json
{
  "error": "auth_unavailable"
}
```

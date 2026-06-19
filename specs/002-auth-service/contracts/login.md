# Contract: POST /api/v1/auth/login

## Request

```http
POST /api/v1/auth/login HTTP/1.1
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "secret"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `email` | string | yes | User email address |
| `password` | string | yes | User password (never logged) |

## Response: 200 OK

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 300,
  "token_type": "Bearer"
}
```

## Response: 400 Bad Request

```json
{
  "error": "validation_error",
  "details": [
    { "field": "email", "message": "email is required" }
  ]
}
```

## Response: 401 Unauthorized

```json
{
  "error": "invalid_credentials"
}
```

## Response: 503 Service Unavailable

```json
{
  "error": "auth_unavailable"
}
```

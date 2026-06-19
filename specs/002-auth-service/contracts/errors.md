# Error Contracts — Auth Service

Standardised error responses across all Auth Service endpoints.

| HTTP Status | `error` code | Applicable Endpoints | Description |
|-------------|--------------|---------------------|-------------|
| 400 | `validation_error` | login, refresh, logout | Missing or malformed request body |
| 401 | `invalid_credentials` | login | Email/password combination is incorrect |
| 401 | `token_expired` | refresh | Refresh token is expired or revoked |
| 503 | `auth_unavailable` | login, refresh, logout | Keycloak is unreachable or returned an error |

## Response Body Format

### 400 validation_error

```json
{
  "error": "validation_error",
  "details": [
    { "field": "<field_name>", "message": "<human-readable message>" }
  ]
}
```

### 401 invalid_credentials / token_expired

```json
{
  "error": "invalid_credentials"
}
```

```json
{
  "error": "token_expired"
}
```

### 503 auth_unavailable

```json
{
  "error": "auth_unavailable"
}
```

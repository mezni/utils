# API Contracts: Identity Core (MVP-2)

## Base URL

All endpoints are under `/api/v1/` prefix and routed through Traefik.

| Route | Service | Priority |
|-------|---------|----------|
| `/api/v1/auth/*` | auth-service | 100 |
| `/api/v1/*` (non-auth) | driver-service | 100 (same priority, PathPrefix match differentiates) |

---

## Endpoints

### POST /api/v1/auth/register

Register a new driver account.

**Request**:
```json
{
  "email": "user@example.com",
  "password": "securePassword123!",
  "first_name": "Jane",
  "last_name": "Driver"
}
```

**Response** `201 Created`:
```json
{
  "usr_id": "USR-a1b2c3d4e5f6",
  "email": "user@example.com",
  "first_name": "Jane",
  "last_name": "Driver",
  "realm": "bm-drivers",
  "status": "ACTIVE",
  "created_at": "2026-06-15T10:00:00Z"
}
```

**Response** `409 Conflict`:
```json
{
  "error": "email_already_in_use",
  "message": "An account with this email already exists"
}
```

**Response** `422 Unprocessable Entity` (validation):
```json
{
  "error": "validation_failed",
  "message": "Invalid email format",
  "fields": {
    "email": "must be a valid email address",
    "password": "must be at least 8 characters"
  }
}
```

**Validation rules**:
- `email`: required, valid email format, max 255 chars
- `password`: required, min 8 chars, max 128 chars
- `first_name`: optional, max 100 chars
- `last_name`: optional, max 100 chars

---

### POST /api/v1/auth/login

Initiate the OIDC authorization flow. Redirects the user agent to Keycloak.

**Request** (form-encoded or JSON):
```json
{
  "email": "user@example.com",
  "password": "securePassword123!"
}
```

**Note**: For BFF pattern, this endpoint accepts credentials server-side (not direct from mobile app) and performs the PKCE flow with Keycloak. The mobile app calls this endpoint which acts as the OIDC client.

**⚠️ BFF abstraction**: The auth-service receives credentials and exchanges them with Keycloak via a confidential client grant (internal detail). Client applications never handle Keycloak tokens or client secrets directly. This satisfies FR-011 (browser redirect flow) while enabling a first-party mobile API pattern.

**Response** `200 OK`:
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 300,
  "token_type": "Bearer",
  "usr_id": "USR-a1b2c3d4e5f6"
}
```

**Set-Cookie**: `bm_session=<session_id>; HttpOnly; Secure; SameSite=Lax; Max-Age=3600`

**Response** `401 Unauthorized`:
```json
{
  "error": "invalid_credentials",
  "message": "Invalid email or password"
}
```

**Response** `429 Too Many Requests`:
```json
{
  "error": "rate_limited",
  "message": "Too many login attempts. Try again later."
}
```

---

### POST /api/v1/auth/logout

Invalidate the current session.

**Request**: Cookie-based (no body needed)

**Response** `200 OK`:
```json
{
  "message": "Logged out successfully"
}
```

**Response** `401 Unauthorized` (no valid session):
```json
{
  "error": "not_authenticated",
  "message": "No active session"
}
```

---

### POST /api/v1/auth/refresh

Refresh an expiring access token.

**Request**:
```json
{
  "refresh_token": "eyJhbGciOiJSUzI1NiIs..."
}
```

**Response** `200 OK`:
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "refresh_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 300,
  "token_type": "Bearer"
}
```

---

### GET /api/v1/auth/me

Get the current authenticated user's identity.

**Request**: Cookie or `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "usr_id": "USR-a1b2c3d4e5f6",
  "email": "user@example.com",
  "first_name": "Jane",
  "last_name": "Driver",
  "realm": "bm-drivers",
  "roles": ["registered_driver"],
  "status": "ACTIVE"
}
```

---

### POST /api/v1/auth/admin/accounts

Create a partner account (admin only).

**Request**:
```json
{
  "email": "partner@org.com",
  "first_name": "Partner",
  "last_name": "User",
  "realm": "bm-control",
  "roles": ["partner"]
}
```

**Response** `201 Created`:
```json
{
  "usr_id": "USR-z9y8x7w6v5u4",
  "email": "partner@org.com",
  "realm": "bm-control",
  "status": "ACTIVE",
  "roles": ["partner"]
}
```

---

### PATCH /api/v1/auth/admin/accounts/{usr_id}/status

Change account status (admin only).

**Request**:
```json
{
  "status": "DISABLED"
}
```

**Response** `200 OK`:
```json
{
  "usr_id": "USR-a1b2c3d4e5f6",
  "status": "DISABLED",
  "disabled_at": "2026-06-15T12:00:00Z"
}
```

---

### GET /api/v1/health

Health check (no auth required).

**Response** `200 OK`:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "checks": {
    "database": "ok",
    "keycloak": "ok",
    "jwks_cache": "ok"
  }
}
```

---

## Identity Claims (token payload)

Services that consume the JWT will see claims in this shape:

```json
{
  "sub": "a1b2c3d4-... (Keycloak UUID)",
  "email": "user@example.com",
  "preferred_username": "user@example.com",
  "realm": "bm-drivers",
  "usr_id": "USR-a1b2c3d4e5f6",
  "roles": ["registered_driver"],
  "status": "ACTIVE",
  "iat": 1686835200,
  "exp": 1686835500,
  "iss": "https://keycloak/realms/bm-drivers",
  "aud": "bm-drivers-client"
}
```

---

## Error Response Format (consistent across all endpoints)

```json
{
  "error": "error_code",
  "message": "Human-readable description",
  "request_id": "req-uuid-correlation-id"
}
```

HTTP status codes used: 200, 201, 400, 401, 404, 409, 422, 429, 500, 503.

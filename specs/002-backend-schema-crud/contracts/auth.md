# API Contracts: Authentication

**Base Path**: `/api/v1/auth`

## Register (Driver Self-Registration)

`POST /api/v1/auth/register`

**Request Body**:
```json
{
  "email": "driver@example.com",
  "username": "driver1",
  "password": "securepass123"
}
```

Creates a user with `role = 'driver'` and returns access credentials.

**Success Response** (201 Created):
```json
{
  "user": {
    "id": "USR-n3wdr1v3r00",
    "email": "driver@example.com",
    "username": "driver1",
    "role": "driver",
    "is_test": false,
    "created_at": "2026-05-26T10:00:00.123456Z",
    "updated_at": "2026-05-26T10:00:00.123456Z"
  },
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Error Responses**:
- `409 Conflict`: Email or username already exists
- `422 Unprocessable Entity`: Validation error (password < 8 chars, invalid email)

---

## Login

`POST /api/v1/auth/login`

**Request Body**:
```json
{
  "email": "driver@example.com",
  "password": "securepass123"
}
```

**Success Response** (200 OK):
```json
{
  "user": {
    "id": "USR-n3wdr1v3r00",
    "email": "driver@example.com",
    "username": "driver1",
    "role": "driver",
    "is_test": false,
    "created_at": "2026-05-26T10:00:00.123456Z",
    "updated_at": "2026-05-26T10:00:00.123456Z"
  },
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Error Responses**:
- `401 Unauthorized`: Invalid credentials

---

## Token Details

- **Algorithm**: HS256
- **Claims**:
  ```json
  {
    "sub": "USR-n3wdr1v3r00",
    "role": "driver",
    "iat": 1748246400,
    "exp": 1748332800
  }
  ```
- **Expiry**: 24 hours from issuance
- **Usage**: Include in requests as `Authorization: Bearer <token>`

---

## Protected Endpoints

All endpoints except `POST /api/v1/auth/register`, `POST /api/v1/auth/login`,
and `GET /api/v1/health` require a valid Bearer token.

**Missing token**: `401 Unauthorized`
```json
{
  "type": "unauthorized",
  "title": "Authentication required",
  "status": 401,
  "detail": "A valid Bearer token is required to access this resource."
}
```

**Expired/invalid token**: `401 Unauthorized`
```json
{
  "type": "unauthorized",
  "title": "Invalid or expired token",
  "status": 401,
  "detail": "The provided token is invalid or has expired."
}
```

## Partner-Scoped Access

When a partner-authenticated request accesses station endpoints, the `owner_id`
is automatically injected from the JWT `sub` claim. Partners cannot access
stations owned by other partners.

**Cross-tenant access**: `403 Forbidden`
```json
{
  "type": "forbidden",
  "title": "Access denied",
  "status": 403,
  "detail": "You do not have access to this resource."
}
```

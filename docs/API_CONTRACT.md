# BorneMap API Contract

Version  : v14
Status   : Draft
Updated  : 2026-06-26

---

## 1 · Global Rules

### 1.1 API Base

All endpoints MUST be versioned:

```
/api/v1/
```

Non-versioned routes are forbidden in public API surface.

---

### 1.2 Response Envelope

**Success**

```json
{
  "data": {},
  "meta": null,
  "error": null
}
```

**Error**

```json
{
  "data": null,
  "meta": null,
  "error": {
    "code": "STRING",
    "message": "STRING",
    "field": "STRING | null"
  }
}
```

---

### 1.3 Authorization

```
Authorization: Bearer <JWT>
```

- JWT required only for protected endpoints (see authorization matrix)
- MVP-1 driver endpoints may be public (UX-first constraint)

---

### 1.4 Pagination

**Request:**

```
?page=1&per_page=20
```

**Response:**

```json
{
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 100
  }
}
```

Defaults: `page=1`, `per_page=20`. Max `per_page=100`.

---

### 1.5 Error Codes

| Code | HTTP Status |
|---|---|
| `VALIDATION_ERROR` | 400 |
| `INVALID_CREDENTIALS` | 401 |
| `UNAUTHORIZED` | 401 |
| `FORBIDDEN` | 403 |
| `USER_NOT_FOUND` | 404 |
| `STATION_NOT_FOUND` | 404 |
| `INTERNAL_ERROR` | 500 |

---

## 2 · Auth Service (MVP-2+) — `/api/v1/auth/*`

⚠️ MVP-1: NOT ACTIVE

### 2.1 Register

```
POST /api/v1/auth/register
```

**Request:**

```json
{
  "email": "user@example.com",
  "password": "string"
}
```

**Response (201):**

```json
{
  "data": {
    "user_id": "uuid"
  }
}
```

### 2.2 Login

```
POST /api/v1/auth/login
```

**Request:**

```json
{
  "email": "user@example.com",
  "password": "string"
}
```

**Response (200):**

```json
{
  "data": {
    "access_token": "jwt",
    "refresh_token": "string",
    "expires_in": 86400
  }
}
```

### 2.3 Refresh

```
POST /api/v1/auth/refresh
```

**Request:**

```json
{
  "refresh_token": "string"
}
```

**Response (200):** Same as login.

### 2.4 Logout

```
POST /api/v1/auth/logout
```

**Headers:** `Authorization: Bearer <JWT>`

**Response (204):** No content.

### 2.5 OAuth Start

```
GET /api/v1/auth/oauth/{provider}/start
```

**Response (302):** Redirect to provider authorization URL.

### 2.6 OAuth Callback

```
GET /api/v1/auth/oauth/{provider}/callback?code=...
```

**Response (302):** Redirect to frontend with session token.

---

## 3 · Driver Service (MVP-1 Core) — `/api/v1/driver/*`

### 3.1 List Stations (Map Discovery)

```
GET /api/v1/driver/stations
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|---|---|---|---|---|
| `lat` | f64 | yes | — | Center latitude |
| `lng` | f64 | yes | — | Center longitude |
| `radius` | f64 | no | 5 | Search radius in km (max 5) |
| `page` | u32 | no | 1 | Page number |
| `per_page` | u32 | no | 20 | Items per page (max 100) |

**Response (200):**

```json
{
  "data": [
    {
      "id": "uuid",
      "name": "Station Tunis Centre",
      "latitude": 36.8,
      "longitude": 10.18,
      "address": "1 Rue de la Liberté, Tunis",
      "available_connectors": 3,
      "status": "ACTIVE"
    }
  ],
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 50
  }
}
```

### 3.2 Station Details

```
GET /api/v1/driver/stations/{id}
```

**Response (200):**

```json
{
  "data": {
    "id": "uuid",
    "name": "Station Tunis Centre",
    "latitude": 36.8,
    "longitude": 10.18,
    "address": "1 Rue de la Liberté, Tunis",
    "status": "ACTIVE",
    "connectors": [
      {
        "id": "uuid",
        "type": "CCS",
        "power_kw": 150.0,
        "status": "AVAILABLE",
        "price_per_kwh": 350
      }
    ],
    "reviews": [],
    "created_at": "2026-06-26T00:00:00Z"
  }
}
```

### 3.3 Favorites

```
POST /api/v1/driver/favorites
```

**Headers:** `Authorization: Bearer <JWT>`

**Request:**

```json
{
  "station_id": "uuid"
}
```

**Response (201):**

```json
{
  "data": {
    "id": "uuid",
    "station_id": "uuid",
    "created_at": "2026-06-26T00:00:00Z"
  }
}
```

```
DELETE /api/v1/driver/favorites/{station_id}
```

**Response (204):** No content.

```
GET /api/v1/driver/favorites
```

**Response (200):**

```json
{
  "data": [
    {
      "id": "uuid",
      "station_id": "uuid",
      "name": "Station Tunis Centre",
      "created_at": "2026-06-26T00:00:00Z"
    }
  ]
}
```

### 3.4 Charging Sessions

```
POST /api/v1/driver/sessions/start
```

**Headers:** `Authorization: Bearer <JWT>`

**Request:**

```json
{
  "connector_id": "uuid"
}
```

**Response (201):**

```json
{
  "data": {
    "session_id": "uuid",
    "started_at": "2026-06-26T00:00:00Z"
  }
}
```

```
POST /api/v1/driver/sessions/{id}/stop
```

**Response (200):**

```json
{
  "data": {
    "session_id": "uuid",
    "kwh_used": 45.5,
    "cost": 15925,
    "stopped_at": "2026-06-26T01:00:00Z"
  }
}
```

```
GET /api/v1/driver/sessions
```

**Query Parameters:** `?page=1&per_page=20`

**Response (200):**

```json
{
  "data": [
    {
      "session_id": "uuid",
      "station_name": "Station Tunis Centre",
      "connector_type": "CCS",
      "kwh_used": 45.5,
      "cost": 15925,
      "started_at": "2026-06-26T00:00:00Z",
      "stopped_at": "2026-06-26T01:00:00Z",
      "status": "COMPLETED"
    }
  ],
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 12
  }
}
```

---

## 4 · Admin Service (MVP-2+) — `/api/v1/admin/*`

⚠️ MVP-1: NOT ACTIVE

### 4.1 Station Management

```
POST /api/v1/admin/stations
```

**Headers:** `Authorization: Bearer <JWT>`

**Request:**

```json
{
  "name": "Station Sfax Centre",
  "address": "Avenue Habib Bourguiba, Sfax",
  "latitude": 34.74,
  "longitude": 10.76,
  "partner_id": "uuid | null"
}
```

**Response (201):**

```json
{
  "data": {
    "id": "uuid"
  }
}
```

```
PUT /api/v1/admin/stations/{id}
```

**Request:** Same as create (all fields optional for partial update).

**Response (200):**

```json
{
  "data": {
    "id": "uuid"
  }
}
```

```
DELETE /api/v1/admin/stations/{id}
```

**Response (204):** No content.

### 4.2 Connectors

```
POST /api/v1/admin/connectors
```

**Request:**

```json
{
  "station_id": "uuid",
  "type": "CCS",
  "power_kw": 150.0,
  "price_per_kwh": 350
}
```

**Response (201):**

```json
{
  "data": {
    "id": "uuid"
  }
}
```

```
PATCH /api/v1/admin/connectors/{id}
```

**Request:**

```json
{
  "status": "MAINTENANCE",
  "price_per_kwh": 400
}
```

**Response (200):**

```json
{
  "data": {
    "id": "uuid"
  }
}
```

### 4.3 Pricing (Post-MVP Only ❌ MVP-1 Disabled)

⚠️ DISABLED IN MVP-1 (no business model / no payment system)

Reserved for future releases:

```
/api/v1/admin/pricing
```

---

## 5 · Health & Observability

```
GET /health/live     → 200 OK
GET /health/ready    → 200 OK (or 503 if not ready)
GET /metrics         → Prometheus-format metrics
```

---

## 6 · Authorization Matrix

| Endpoint | Auth Required | Role |
|---|---|---|
| `POST /auth/register` | No | public |
| `POST /auth/login` | No | public |
| `POST /auth/refresh` | No | public |
| `POST /auth/logout` | Yes | any authenticated |
| `GET /auth/oauth/*` | No | public |
| `GET /driver/stations` | No | public (MVP-1 UX-first) |
| `GET /driver/stations/{id}` | No | public |
| `POST /driver/favorites` | Yes | REGISTERED_DRIVER |
| `DELETE /driver/favorites/{id}` | Yes | REGISTERED_DRIVER |
| `GET /driver/favorites` | Yes | REGISTERED_DRIVER |
| `POST /driver/sessions/start` | Yes | REGISTERED_DRIVER |
| `POST /driver/sessions/{id}/stop` | Yes | REGISTERED_DRIVER |
| `GET /driver/sessions` | Yes | REGISTERED_DRIVER |
| `POST /admin/stations` | Yes | ADMIN / PARTNER |
| `PUT /admin/stations/{id}` | Yes | ADMIN / PARTNER |
| `DELETE /admin/stations/{id}` | Yes | ADMIN |
| `POST /admin/connectors` | Yes | ADMIN / PARTNER |
| `PATCH /admin/connectors/{id}` | Yes | ADMIN / PARTNER |

---

## 7 · Validation Rules

| Field | Rule |
|---|---|
| `email` | RFC 5321 compliant, max 254 chars |
| `password` | min 8 chars, max 128 chars |
| `latitude` | [-90, 90] |
| `longitude` | [-180, 180] |
| `radius` | (0, 5] km |
| `station.name` | min 1 char, max 255 chars |
| `connector.type` | must be one of: `TYPE2`, `CCS`, `CHADEMO`, `TESLA` |
| `connector.power_kw` | > 0 |
| `review.rating` | [1, 5] |
| `pagination.page` | >= 1 |
| `pagination.per_page` | [1, 100] |

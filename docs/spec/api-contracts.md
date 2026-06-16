# API Contracts

**Base URL**: all services serve under their respective ports (Auth:3000, Driver:3001, Admin:3002, GIS:3003). In production (MVP-6), Traefik routes `api.bornemap.tn/v1/...` to the correct service.

**Version prefix**: all endpoints under `/api/v1/`.

**Auth header**: `Authorization: Bearer <JWT>` where required.

**Response envelope** (error):
```json
{
  "error": {
    "code": "STA_001",
    "message": "Station not found"
  }
}
```

---

## Auth Service (`:3000`)

**Owns**: `users` schema reads/writes, Keycloak admin API for partner ops.

### `GET /api/v1/health`

Liveness check. No auth.

**Response 200**:
```json
{ "status": "ok", "service": "auth-service", "version": "0.1.0" }
```

### `GET /api/v1/health/ready`

Readiness check — verifies DB connectivity. No auth.

**Response 200**: `{ "status": "ready" }`
**Response 503**: `{ "status": "not ready", "error": "DB connection failed" }`

### `POST /api/v1/auth/register`

Public driver self-registration. No auth.

**Request**:
```json
{
  "email": "user@example.com",
  "password": "securePassword123",
  "display_name": "Ahmed"
}
```

**Response 201**:
```json
{
  "user_id": "USR_xxxxx",
  "message": "Registration successful. Verify email to activate."
}
```

**Error codes**: `AUTH_001` (email already exists), `AUTH_002` (invalid password strength).

### `POST /api/v1/auth/partner/register`

Partner self-registration (requires admin approval). No auth.

**Request**:
```json
{
  "name": "ACME Charging",
  "email": "ops@acme-charging.tn",
  "phone": "+216 71 000 000",
  "type": "commercial",
  "password": "securePassword123"
}
```

**Response 201**:
```json
{
  "partner_id": "OPR_xxxxx",
  "message": "Registration submitted. Awaiting admin approval."
}
```

**Error codes**: `OPR_003` (email already exists), `AUTH_002` (weak password).

### `GET /api/v1/users/me`

Return profile for authenticated driver. Creates profile row on first call (lazy creation). Requires driver auth.

**Headers**: `Authorization: Bearer <JWT>`

**Response 200**:
```json
{
  "id": "USR_xxxxx",
  "keycloak_id": "uuid-from-keycloak",
  "email": "user@example.com",
  "display_name": "Ahmed",
  "created_at": "2026-06-15T10:00:00Z"
}
```

**Error codes**: `AUTH_003` (access token invalid/expired).

### `POST /api/v1/admin/partners/invite`

Admin invites a partner. Requires admin auth.

**Request**:
```json
{
  "name": "ACME Charging",
  "email": "ops@acme-charging.tn",
  "type": "commercial",
  "phone": "+216 71 000 000"
}
```

**Response 201**: `{ "partner_id": "OPR_xxxxx" }`

**Error codes**: `AUTH_004` (insufficient admin permissions), `OPR_003` (email already exists).

### `POST /api/v1/admin/partners/{id}/approve`

Approve a self-registered partner. Requires admin auth.

**Response 200**: `{ "partner_id": "OPR_xxxxx", "status": "active" }`

**Error codes**: `OPR_001` (partner not found), `OPR_005` (partner not in pending status).

### `POST /api/v1/admin/partners/{id}/reject`

Reject a self-registered partner. Requires admin auth.

**Response 200**: `{ "partner_id": "OPR_xxxxx", "status": "rejected" }`

---

**Login, social login, token refresh, and logout** are handled directly by Keycloak's OIDC endpoints — no Auth Service proxy. See `docs/spec/auth-flows.md` for the specific Keycloak endpoints and flows.

---

## Driver Service (`:3001`)

**Owns**: `inventory` schema writes for partner station/charger CRUD. Partner writes only (ownership verified). Admin overrides via Admin Service.

### `GET /api/v1/health`

Liveness check. No auth.

### `GET /api/v1/health/ready`

Readiness check. No auth.

### `POST /api/v1/stations`

Create a station. Requires partner auth (ownership verified on token claim).

**Request**:
```json
{
  "name": "My Station",
  "latitude": 36.8,
  "longitude": 10.18,
  "address": "123 Main St, Tunis",
  "city": "Tunis",
  "postal_code": "1001",
  "visibility": "commercial",
  "description": "Near the mall",
  "access_notes": "Gate code 1234",
  "opening_hours": "Mo-Fr 08:00-20:00",
  "has_24h_access": false
}
```

**Response 201**: `{ "id": "STA_xxxxx" }`

**Error codes**: `STA_002` (validation failed), `STA_003` (partner not found), `GEO_001` (invalid coordinates).

### `PATCH /api/v1/stations/{id}`

Partial update station. Requires partner auth (ownership verified).

**Response 200**: `{ "id": "STA_xxxxx" }`

**Error codes**: `STA_001` (not found), `STA_004` (not owner), `STA_002` (validation).

### `DELETE /api/v1/stations/{id}`

Soft-delete station. Requires partner auth (ownership verified).

**Response 204**: No content.

**Error codes**: `STA_001`, `STA_004`.

### `POST /api/v1/stations/{id}/chargers`

Add charger to station. Requires partner auth (ownership verified via station).

**Request**:
```json
{
  "charger_type": "dc",
  "connector": "ccs2",
  "power_kw": 150.0,
  "identifier_code": "CHG-A1"
}
```

**Response 201**: `{ "id": "CHG_xxxxx" }`

**Error codes**: `CHG_001` (validation), `STA_001`, `STA_004`.

### `PATCH /api/v1/stations/{station_id}/chargers/{charger_id}`

Partial update charger. Requires partner auth (ownership verified via station).

**Response 200**: `{ "id": "CHG_xxxxx" }`

### `DELETE /api/v1/stations/{station_id}/chargers/{charger_id}`

Soft-delete charger. Requires partner auth.

**Response 204**.

### `GET /api/v1/partner/stations`

List partner's own stations (for dashboard "my stations" view). Requires partner auth.

**Query params**: `status`, `city`, `page`, `per_page`.

**Response 200**:
```json
{
  "stations": [
    { "id": "STA_xxxxx", "name": "My Station", "city": "Tunis", "status": "active", "charger_count": 4 }
  ],
  "total": 1
}
```

### `GET /api/v1/partner/stations/{id}`

Get partner's station detail with chargers. Requires partner auth (ownership verified).

**Response 200**:
```json
{
  "id": "STA_xxxxx",
  "name": "My Station",
  "city": "Tunis",
  "status": "active",
  "chargers": [
    { "id": "CHG_xxxxx", "charger_type": "dc", "connector": "ccs2", "power_kw": 150.0, "status": "available" }
  ]
}
```

**Error codes**: `STA_001`, `STA_004`.

### `GET /api/v1/favorites`

List driver's favorite stations. Requires driver auth.

**Response 200**:
```json
{ "stations": [ ... ] }
```

### `POST /api/v1/favorites`

Add station to favorites. Requires driver auth.

**Request**:
```json
{
  "station_id": "STA_xxxxx"
}
```

**Response 201**: `{ "message": "Added" }`

**Error codes**: `FAV_001` (already favorited), `STA_001` (station not found).

### `DELETE /api/v1/favorites/{station_id}`

Remove from favorites. Requires driver auth.

**Response 204**.

---

## Admin Service (`:3002`)

**Owns**: partner CRUD, admin station overrides (status changes, cross-partner views), analytics.

### `GET /api/v1/health`

Liveness check. No auth.

### `GET /api/v1/health/ready`

Readiness check. No auth.

### `POST /api/v1/admin/partners`

Create a partner (admin invites). Requires admin auth.

**Request**:
```json
{
  "name": "ACME Charging",
  "type": "commercial",
  "email": "ops@acme-charging.tn",
  "phone": "+216 71 000 000"
}
```

**Response 201**: `{ "id": "OPR_xxxxx" }`

### `GET /api/v1/admin/partners`

List all partners. Requires admin auth.

**Query params**: `status` (active|suspended|closed|pending|rejected), `type` (commercial|private), `page`, `per_page`.

### `GET /api/v1/admin/partners/{id}`

Get partner details. Requires admin auth.

### `PATCH /api/v1/admin/partners/{id}`

Partial update partner. Requires admin auth.

### `DELETE /api/v1/admin/partners/{id}`

Soft-delete partner (cascades). Requires admin auth.

**Response 204**.

### `GET /api/v1/admin/partners/{id}/stations`

List all stations for a partner. Requires admin auth.

### `GET /api/v1/admin/stations`

List all stations across all partners. Requires admin auth.

**Query params**: `status`, `city`, `visibility`, `partner_id`, `page`, `per_page`.

### `GET /api/v1/admin/stations/{id}`

Get station detail with chargers. Requires admin auth.

**Response 200**:
```json
{
  "id": "STA_xxxxx",
  "partner_id": "OPR_xxxxx",
  "name": "My Station",
  "city": "Tunis",
  "status": "active",
  "chargers": [
    { "id": "CHG_xxxxx", "charger_type": "dc", "connector": "ccs2", "power_kw": 150.0, "status": "available" }
  ]
}
```

**Error codes**: `STA_001`.

### `PATCH /api/v1/admin/stations/{id}/status`

Change station status (admin override). Requires admin auth.

**Request**:
```json
{ "status": "active" }
```

**Error codes**: `STA_005` (invalid status transition).

### `GET /api/v1/admin/analytics/overview`

Dashboard home summary stats (MVP-5). Requires admin auth.

**Response 200**:
```json
{
  "total_stations": 120,
  "total_chargers": 480,
  "active_sessions": 42,
  "energy_delivered_kwh": 15200.5,
  "period": "today"
}
```

### `GET /api/v1/admin/analytics/searches`

Nearby query volume (MVP-5). Requires admin auth.

**Query params**: `from`, `to`, `granularity` (hour|day|week).

### `GET /api/v1/admin/analytics/stations`

Station counts and activity (MVP-5). Requires admin auth.

### `GET /api/v1/admin/analytics/sessions`

Charging session metrics (MVP-5). Requires admin auth.

**Query params**: `from`, `to`, `granularity` (hour|day|week).

### `GET /api/v1/admin/analytics/energy`

Energy dispensed over time (MVP-5). Requires admin auth.

**Query params**: `from`, `to`, `granularity`.

### `GET /api/v1/admin/analytics/revenue`

Revenue metrics (MVP-5, stub). Requires admin auth.

**Query params**: `from`, `to`, `granularity`.

---

## GIS Service (`:3003`)

**Role**: Read-optimized spatial API. All public map queries go here. Redis-cached.

### `GET /api/v1/health`

Liveness check. No auth.

### `GET /api/v1/health/ready`

Readiness check — verifies DB + Redis connectivity. No auth.

### `GET /api/v1/nearby`

Find active stations near a location. No auth (public).

**Query params**: `lat` (double, required), `lon` (double, required), `radius_m` (int, optional, default 5000), `max_results` (int, optional, default 50), `visibility` (string, optional, filter: `commercial`, `private_home`).

**Caching**: Redis-backed. Cache key `nearby:{lat:.2f}:{lon:.2f}:{radius_m}`, TTL 120s.

**Response 200**:
```json
{
  "stations": [
    {
      "id": "STA_xxxxx",
      "name": "My Station",
      "location": { "lat": 36.8, "lon": 10.18 },
      "address": "Tunis",
      "distance_m": 1200,
      "visibility": "commercial",
      "status": "active",
      "chargers": [
        { "charger_type": "dc", "connector": "ccs2", "power_kw": 150.0, "status": "available" }
      ]
    }
  ],
  "cached": true
}
```

**Error codes**: `GEO_001` (invalid coordinates), `GEO_002` (radius exceeds max), `GEO_003` (service degraded — cache unavailable, query still served from DB).

### `GET /api/v1/stations/{id}`

Get station details by ID. No auth (public). Includes all chargers.

**Caching**: `station:{id}`, TTL 300s.

**Response 200**: Same station shape as `stations[]` entry above.

**Error codes**: `STA_001` (station not found).

### `POST /api/v1/internal/cache/invalidate`

Internal endpoint — resets cached queries for given stations. Called by Driver/Admin Service after writes. Docker network only.

**Auth**: Internal shared secret (`CACHE_SECRET` env var).

**Request**:
```json
{
  "station_ids": ["STA_xxxxx"],
  "reason": "station_update"
}
```

**Response 200**:
```json
{
  "invalidated": true,
  "keys_affected": 5
}
```

**Error codes**: `GIS_001` (no station_ids provided), `GIS_002` (invalidation failed — Redis unreachable).

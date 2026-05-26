# API Contracts: Partner Dashboard — Multi-Tenant Views

## Base URL

All endpoints mounted under `/api/v1/`. Requests include `Authorization: Bearer <JWT>` header.

**Key difference from admin portal**: All list/detail endpoints are scoped to the authenticated partner's `owner_id`. The backend injects this from the JWT context automatically — no additional query params needed.

---

## Partner Profile

### GET /api/v1/partners/me

Get the authenticated partner's own profile. Scoped to the current user's partner profile.

**Response**: `200 OK`
```json
{
  "id": "PRT-abc123def456",
  "display_name": "Tunisie Charge",
  "classification": "Business",
  "tax_id": "1234567K",
  "contact_phone": "+21650123456",
  "logo_url": "https://cdn.example.com/logo.png",
  "created_at": "2026-05-25T10:00:00Z"
}
```

### PATCH /api/v1/partners/me

Update own profile. Only certain fields are writable by the partner.

**Request**:
```json
{
  "display_name": "Tunisie Charge Updated",
  "contact_phone": "+21650987654",
  "logo_url": "https://cdn.example.com/new-logo.png"
}
```

**Note**: `classification` and `tax_id` are rejected if included in the request body (read-only for partner).

**Response**: `200 OK` with updated partner object.

---

## Stations (Partner-Scoped)

### GET /api/v1/stations

List stations owned by the authenticated partner. Backend auto-filters by `owner_id`.

**Response**: `200 OK`
```json
{
  "data": [
    {
      "id": "STN-k4m2n9p1q5v8",
      "name": "Ariana Supercharge",
      "address": "15 Rue des Entrepreneurs",
      "city": "Ariana",
      "latitude": 36.8665,
      "longitude": 10.1647,
      "owner_id": "PRT-abc123def456",
      "owner_name": "Tunisie Charge",
      "is_operational": true,
      "is_test": false,
      "created_at": "2026-05-25T10:00:00Z"
    }
  ],
  "total": 3
}
```

### POST /api/v1/stations

Create a new station. `owner_id` is auto-assigned from the authenticated partner's profile. Any `owner_id` in the request body is ignored.

**Request**:
```json
{
  "name": "Ariana Supercharge",
  "address": "15 Rue des Entrepreneurs",
  "city": "Ariana",
  "latitude": 36.8665,
  "longitude": 10.1647,
  "is_operational": true
}
```

**Response**: `201 Created`

### PATCH /api/v1/stations/:id

Update station. Verifies `owner_id` matches the authenticated partner. Returns 403 if the station belongs to another partner.

**Response**: `200 OK`

### DELETE /api/v1/stations/:id

Soft-delete station. Sets `deleted_at`. Verifies ownership. Returns 403 if not owned by partner.

**Response**: `204 No Content`

---

## Chargers (Partner-Scoped)

### GET /api/v1/chargers

List chargers belonging to the authenticated partner's stations. Backend joins through `stations` table to filter by `owner_id`.

**Response**: `200 OK`
```json
{
  "data": [
    {
      "id": "CHG-m9n2p1q5v8k4",
      "station_id": "STN-k4m2n9p1q5v8",
      "station_name": "Ariana Supercharge",
      "connector_type_id": "CNT-abc123def456",
      "connector_type_name": "Type 2",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available"
    }
  ],
  "total": 5
}
```

### GET /api/v1/stations/:id/chargers

List chargers for a specific station. Verifies the station belongs to the authenticated partner before returning results.

**Response**: `200 OK` (same shape as above, filtered by station_id)

### POST /api/v1/chargers

Create charger. Verifies `station_id` belongs to the authenticated partner. Returns 403 if the station is not owned by the partner.

**Request**:
```json
{
  "station_id": "STN-k4m2n9p1q5v8",
  "connector_type_id": "CNT-abc123def456",
  "power_kw": 22.0,
  "current_type": "AC",
  "status": "available"
}
```

**Response**: `201 Created`

### PATCH /api/v1/chargers/:id

Update charger. Verifies ownership through station → partner chain.

**Response**: `200 OK`

### DELETE /api/v1/chargers/:id

Hard-delete charger. Verifies ownership through station → partner chain.

**Response**: `204 No Content`

---

## Auth

### POST /api/v1/auth/login

**Request**:
```json
{
  "email": "partner@example.com",
  "password": "securepassword"
}
```

**Response**: `200 OK`
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "USR-xyz789uvw012",
    "email": "partner@example.com",
    "role": "partner"
  }
}
```

### Ownership Verification Flow

```
Request → JWT Middleware extracts user_id + role
         ↓
   role = "partner"?
         ↓
   Look up partner_profile_id for user_id
         ↓
   Inject partner_profile_id as owner_id into query context
         ↓
   All list/detail/update/delete queries include WHERE owner_id = $N
         ↓
   Any operation on another partner's resource returns 403
```

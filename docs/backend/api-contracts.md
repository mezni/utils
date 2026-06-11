# API Contracts

**Base Rule:** All endpoints MUST be under `/api/v1/*`

---

## Driver Service (8080) — Discovery

### Get stations (map view)

```
GET /api/v1/stations

Query:
  bbox      (optional) — map bounds
  limit     (optional)
  offset    (optional)

Response:
{
  "stations": [
    {
      "id": "STA-abc123def456",
      "name": "Central EV Station",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "available_chargers": 4
    }
  ]
}
```

### Nearby stations (CORE UX)

```
GET /api/v1/stations/nearby

Query:
  lat        (required)
  lng        (required)
  radius_m   (default: 3000)

Response:
{
  "stations": [
    {
      "id": "STA-abc123def456",
      "distance_m": 420,
      "name": "Airport EV Hub"
    }
  ]
}
```

### Station details

```
GET /api/v1/stations/{id}

Response:
{
  "id": "STA-abc123def456",
  "name": "Central EV Station",
  "address": "Tunis Center",
  "chargers": [
    {
      "id": "CHR-abc123def456",
      "type": "CCS2",
      "power_kw": 50,
      "status": "available"
    }
  ]
}
```

---

## Admin Service (8081) — Management (MVP-2+)

### Create partner

```
POST /api/v1/partners
{
  "name": "EV Tunisia Ltd",
  "type": "business"
}
```

### Approve partner

```
PATCH /api/v1/partners/{id}/approve
```

### Create station

```
POST /api/v1/stations
{
  "partner_id": "PRT-abc123def456",
  "name": "Downtown Station",
  "latitude": 36.8,
  "longitude": 10.18,
  "address": "Tunis Center"
}
```

### Add charger

```
POST /api/v1/stations/{id}/chargers
{
  "connector_type": "CCS2",
  "power_kw": 50
}
```

### List partners

```
GET /api/v1/partners
```

---

## Clickstream Service (8082) — Events

### Send event

```
POST /api/v1/events
{
  "event_name": "station_view",
  "user_id": "USR-abc123def456",
  "session_id": "sess-abc123",
  "payload": {
    "station_id": "STA-abc123def456"
  }
}
```

### Batch events

```
POST /api/v1/events/batch
{
  "events": [
    {
      "event_name": "map_pan",
      "payload": {}
    },
    {
      "event_name": "station_click",
      "payload": {
        "station_id": "STA-abc123def456"
      }
    }
  ]
}
```

---

## Auth Gateway (MVP-3)

### Login

```
POST /api/v1/auth/login
{
  "email": "user@email.com",
  "password": "***"
}
```

### Social login

```
POST /api/v1/auth/social
{
  "provider": "google",
  "token": "oauth_token"
}
```

### Refresh token

```
POST /api/v1/auth/refresh
```

### Me (session)

```
GET /api/v1/auth/me
```

---

## Cross-Service Rules

- **JWT required:** `Authorization: Bearer <JWT>` on every request
- **Partner scoping:** `WHERE partner_id = JWT.partner_id`
- **Access model:**

| Endpoint | Access |
|---|---|
| stations | public |
| nearby | public |
| favorites | authenticated |
| admin APIs | partner/admin only |

## Forbidden Patterns

- `/v2/*` during MVP-1 → MVP-3
- Direct DB access from clients
- Mixed service responsibilities
- Cross-service endpoint chaining

## Performance Targets

- `/stations/nearby` → < 200ms
- PostGIS index required
- Pagination mandatory for large queries
- Clickstream → fire-and-forget, must not block UX

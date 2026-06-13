# API Contracts: Integration & Testing

## Event Logging Contract

**Endpoint**: `POST /api/v1/events` (single event)
**Service**: admin-service (:8081)
**Gateway route**: `http://localhost:8080/api/v1/events`

### Request

```json
{
  "event_type": "station_detail_view",
  "actor": {
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "ip_address": "192.168.1.1",
    "user_agent": "BornemapMobile/1.0"
  },
  "context": {
    "station_id": "STA-TEST-001"
  },
  "timestamp": "2026-06-12T12:00:00Z"
}
```

### Success Response (201)

```json
{
  "status": "accepted",
  "event_id": "EVT-abc123"
}
```

### Error Responses

| Status | Body |
|--------|------|
| 400 | `{ "error": "invalid_event_type", "message": "Event type must be one of: station_detail_view, search, nearby_search, navigate_to_station" }` |
| 400 | `{ "error": "missing_session_id", "message": "Session ID is required" }` |
| 400 | `{ "error": "invalid_coordinates", "message": "Latitude must be between -90 and 90" }` |
| 401 | `{ "error": "unauthorized", "message": "Authentication required" }` (graceful rejection — no crash) |

---

## Batch Event Contract

**Endpoint**: `POST /api/v1/events/batch`
**Service**: admin-service (:8081)
**Gateway route**: `http://localhost:8080/api/v1/events/batch`

### Request

```json
{
  "events": [
    {
      "event_type": "station_detail_view",
      "actor": { "session_id": "550e8400-e29b-41d4-a716-446655440000", "ip_address": "192.168.1.1" },
      "context": { "station_id": "STA-TEST-001" },
      "timestamp": "2026-06-12T12:00:00Z"
    }
  ]
}
```

### Success Response (201)

```json
{
  "status": "accepted",
  "accepted_count": 1,
  "rejected_count": 0
}
```

### Error: Batch Size Exceeded (400)

```json
{
  "error": "batch_size_exceeded",
  "message": "Maximum 100 events per batch request"
}
```

---

## Traefik Routing Contract

**Gateway**: `http://localhost:8080`

| Rule | Target | Expected Behavior |
|------|--------|-------------------|
| `PathPrefix /api/v1/stations` | `http://driver-service:8080` | Proxies to driver-service |
| `PathPrefix /api/v1/admin` | `http://admin-service:8081` | Proxies to admin-service |
| `PathPrefix /api/v1/events` | `http://admin-service:8081` | Proxies to admin-service |
| `Path /health` | `http://driver-service:8080` | Health check passthrough |
| Unknown route | — | Returns 404 |
| Upstream down | — | Returns 503 |

---

## Existing API Contracts (Reference)

The following API endpoints are already documented and implemented in Phase 2. Contract tests in this phase validate against these existing contracts:

- **Driver-service**: `GET /api/v1/stations`, `GET /api/v1/stations/{id}`, `GET /api/v1/stations/nearby`, `GET /api/v1/health`
- **Admin-service**: `POST /api/v1/stations`, `PUT /api/v1/stations/{id}`, `DELETE /api/v1/stations/{id}` (soft delete), `POST /api/v1/events`, `POST /api/v1/events/batch`

Full contract details: See `specs/002-backend-services/contracts/` and `specs/002-backend-services/data-model.md`.

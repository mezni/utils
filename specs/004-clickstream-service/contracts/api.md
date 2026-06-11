# API Contracts: Clickstream Service

**Base URL**: `http://localhost:8082/api/v1`

## Common

### JSON Response Envelope

All endpoints return responses wrapped in a consistent envelope:

```json
{
  "data": { ... },
  "error": null,
  "meta": {
    "request_id": "<uuid>"
  }
}
```

On error:

```json
{
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable description",
    "details": { ... }
  },
  "meta": {
    "request_id": "<uuid>"
  }
}
```

### Error Codes

| HTTP Status | Code | Description |
|-------------|------|-------------|
| 202 | — | Accepted (success) |
| 400 | `INVALID_JSON` | Request body is not valid JSON |
| 413 | `PAYLOAD_TOO_LARGE` | Event exceeds 64KB or batch exceeds 512KB |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Content-Type is not application/json |
| 422 | `INVALID_EVENT_NAME` | Unknown or empty event_name |
| 422 | `MISSING_SESSION_ID` | session_id is missing or empty |
| 422 | `INVALID_TIMESTAMP` | client_ts is missing or invalid |
| 422 | `INVALID_PAYLOAD` | payload is present but not a valid JSON object |
| 422 | `BATCH_SIZE_EXCEEDED` | Batch has more than 100 events or is empty |
| 422 | `BATCH_TOO_LARGE` | Batch exceeds 512KB total |
| 429 | `RATE_LIMITED` | Too many requests from this IP |
| 503 | `DB_DISCONNECTED` | Database is unreachable (health endpoint) |

---

## `POST /events`

Ingest a single event.

### Request

**Content-Type**: `application/json`

```json
{
  "event_name": "map_open",
  "user_id": "usr_abc123",
  "session_id": "sess_xyz789",
  "client_ts": "2026-06-11T12:00:00Z",
  "payload": {
    "map_center": { "lat": 48.8566, "lng": 2.3522 },
    "zoom_level": 12
  }
}
```

**Required fields**: `event_name`, `session_id`, `client_ts`
**Optional fields**: `user_id`, `payload`

### Response: 202 Accepted

```json
{
  "data": {
    "batch_id": "abc123xyz456"
  },
  "error": null,
  "meta": { "request_id": "req_001" }
}
```

The `batch_id` is a single-event batch identifier (nanoid).

### Response: 422 Validation Error

```json
{
  "data": null,
  "error": {
    "code": "INVALID_EVENT_NAME",
    "message": "Unknown event name: bad_event",
    "details": {
      "field": "event_name",
      "allowed": ["map_open","station_view","station_click","nearby_search","map_pan","map_zoom"]
    }
  },
  "meta": { "request_id": "req_001" }
}
```

### curl

```bash
curl -X POST http://localhost:8082/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{"event_name":"map_open","user_id":"usr_1","session_id":"sess_1","client_ts":"2026-06-11T12:00:00Z"}'
```

---

## `POST /events/batch`

Ingest multiple events in a single request.

### Request

**Content-Type**: `application/json`

```json
[
  {
    "event_name": "map_open",
    "user_id": "usr_1",
    "session_id": "sess_1",
    "client_ts": "2026-06-11T12:00:00Z"
  },
  {
    "event_name": "station_view",
    "user_id": "usr_2",
    "session_id": "sess_2",
    "client_ts": "2026-06-11T12:00:01Z",
    "payload": { "station_id": "st_42" }
  }
]
```

**Constraints**: 1–100 events, total payload ≤ 512KB.

### Response: 202 Accepted (full success)

```json
{
  "data": {
    "batch_id": "batch_xyz789",
    "accepted": 2,
    "failed": []
  },
  "error": null,
  "meta": { "request_id": "req_002" }
}
```

### Response: 202 Accepted (partial success)

```json
{
  "data": {
    "batch_id": "batch_xyz789",
    "accepted": 3,
    "failed": [
      { "index": 2, "error": "Unknown event_name: bad_event" }
    ]
  },
  "error": null,
  "meta": { "request_id": "req_002" }
}
```

### Response: 422 Validation Error (structural failure)

```json
{
  "data": null,
  "error": {
    "code": "BATCH_SIZE_EXCEEDED",
    "message": "Batch must contain 1–100 events"
  },
  "meta": { "request_id": "req_002" }
}
```

### curl

```bash
curl -X POST http://localhost:8082/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[{"event_name":"map_open","user_id":"usr_1","session_id":"sess_1","client_ts":"2026-06-11T12:00:00Z"}]'
```

---

## `GET /health`

Operational health check.

### Response: 200 OK

```json
{
  "data": {
    "status": "ok",
    "database": "connected",
    "uptime_seconds": 3600
  },
  "error": null,
  "meta": { "request_id": "req_003" }
}
```

### Response: 503 Service Unavailable

```json
{
  "data": {
    "status": "degraded",
    "database": "disconnected",
    "uptime_seconds": 3600
  },
  "error": null,
  "meta": { "request_id": "req_003" }
}
```

### curl

```bash
curl http://localhost:8082/api/v1/health
```

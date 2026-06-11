# Data Model: Clickstream Service

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Entity: RawEvent

Stored in `analytics_db.raw_events`. Represents a single user interaction event.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `BIGSERIAL` | auto | Auto-incrementing primary key |
| `batch_id` | `VARCHAR(21)` | yes | Nanoid identifying the batch this event was submitted in |
| `event_name` | `VARCHAR(50)` | yes | One of the MVP-1 taxonomy values |
| `user_id` | `VARCHAR(255)` | no | Nullable — anonymous users (pre-auth) |
| `session_id` | `VARCHAR(255)` | yes | Client session identifier |
| `payload` | `JSONB` | no | Free-form JSON object; no schema validation |
| `client_ts` | `TIMESTAMPTZ` | yes | Timestamp supplied by the client (ISO 8601) |
| `server_ts` | `TIMESTAMPTZ` | yes | Server-generated timestamp on receipt; defaults to `NOW()` |
| `ip_address` | `VARCHAR(45)` | no | Caller's IP address (IPv4 or IPv6) |

## Entity: Event (API Request Body)

The JSON payload sent to `POST /api/v1/events` and as elements of `POST /api/v1/events/batch`.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `event_name` | `string` | yes | Non-empty, must be in MVP-1 taxonomy |
| `user_id` | `string` | no | May be null/absent for anonymous users |
| `session_id` | `string` | yes | Non-empty |
| `client_ts` | `string` | yes | ISO 8601 timestamp |
| `payload` | `object` | no | Free-form JSON; validated as valid JSON only |

## Entity: BatchResponse

Returned from `POST /api/v1/events/batch` inside the `data` envelope field.

| Field | Type | Description |
|-------|------|-------------|
| `batch_id` | `string` | Server-generated nanoid for this batch |
| `accepted` | `integer` | Number of events successfully ingested |
| `failed` | `array` | List of `{ index, error }` for events that failed validation |

## Event Taxonomy (MVP-1)

| Event Name | Description |
|------------|-------------|
| `map_open` | User opened the map view |
| `station_view` | User viewed a charging station detail |
| `station_click` | User clicked/interacted with a station marker |
| `nearby_search` | User searched for nearby stations |
| `map_pan` | User panned the map |
| `map_zoom` | User zoomed the map |

## Validation Rules

| Field | Rule | Error Code |
|-------|------|------------|
| `event_name` | Must be present, non-empty, and one of the taxonomy values | `INVALID_EVENT_NAME` |
| `session_id` | Must be present and non-empty | `MISSING_SESSION_ID` |
| `client_ts` | Must be present and valid ISO 8601 | `INVALID_TIMESTAMP` |
| `payload` | If present, must be a valid JSON object | `INVALID_PAYLOAD` |
| Batch size | Array must contain 1–100 events | `BATCH_SIZE_EXCEEDED` |
| Event size | Serialized event must not exceed 64KB | `PAYLOAD_TOO_LARGE` |
| Batch size | Total serialized batch must not exceed 512KB | `BATCH_TOO_LARGE` |

## Indexes

```sql
CREATE INDEX idx_raw_events_event_name ON raw_events(event_name);
CREATE INDEX idx_raw_events_server_ts   ON raw_events(server_ts);
CREATE INDEX idx_raw_events_session_id  ON raw_events(session_id);
```

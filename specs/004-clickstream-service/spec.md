# Feature Specification: Clickstream Service — Event Ingestion

**Feature Branch**: `004-clickstream-service`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "sprint 1.3 (pivoted from admin-service to clickstream per docs/mvp)"

## Clarifications

### Session 2026-06-11

- Q: Should we lock in payload size limits from the "suggested TBD"? → A: Yes — 64KB per event, 512KB per batch.
- Q: How should timestamp/client_ts/server_ts relate in the event model? → A: Client sends `client_ts` (ISO 8601); server auto-generates `server_ts` on receipt. The ambiguous `timestamp` field name is removed.
- Q: Should `session_id` be explicitly validated as required? → A: Yes — reject with 422 if `session_id` is missing or empty, same as `event_name`.
- Q: Should the service capture the caller's IP address? → A: Yes — capture from the HTTP request and store in raw_events.ip_address.
- Q: Should rate limiting be added for MVP-1? → A: Yes — add per-IP token bucket rate limiting at the Actix-web middleware layer; excess requests receive 429.

## User Scenarios & Testing

### User Story 1 — Ingest Single Event (Priority: P1)

A mobile or web app sends individual user interaction events (map open, station view, search) to the clickstream service for recording. The service ingests the event and returns immediately without blocking the caller.

**Why this priority**: Event ingestion is the core of the clickstream service. Without it, no user interactions are tracked, and the entire analytics pipeline has no data.

**Independent Test**: Send a POST request with a valid event payload and verify a 202 Accepted response with the event recorded in the database.

**Acceptance Scenarios**:

1. **Given** an app user performs an action (e.g., opens the map), **When** the app sends a POST to `/api/v1/events` with the event payload, **Then** the service returns 202 Accepted within 100ms
2. **Given** an event with missing required fields (event_name), **When** posted, **Then** a 422 validation error is returned with field-level details
3. **Given** an event with an unknown event name not in the MVP-1 taxonomy, **When** posted, **Then** a 422 validation error is returned indicating invalid event name
4. **Given** the database is unreachable, **When** an event is posted, **Then** the service still returns 202 but logs the failure internally — the calling app must never be blocked by clickstream availability

---

### User Story 2 — Ingest Batch Events (Priority: P1)

An app sends multiple events at once (e.g., after being offline) to reduce network overhead. The service ingests all events atomically.

**Why this priority**: Batch ingestion is essential for offline-capable apps and for reducing network calls during high-frequency interactions like map pan/zoom.

**Independent Test**: Send a POST request with an array of events and verify all are recorded. Send a batch with one invalid event and verify the valid ones are still accepted.

**Acceptance Scenarios**:

1. **Given** an app has accumulated multiple events while offline, **When** the app sends a POST to `/api/v1/events/batch` with an array of events, **Then** all events are ingested and a 202 response is returned
2. **Given** a batch where one event has an invalid event name, **When** posted, **Then** the service accepts the valid events and returns a partial success response indicating which events failed
3. **Given** an empty batch array, **When** posted, **Then** a 422 validation error is returned

---

### User Story 3 — Health Check (Priority: P3)

Operational monitoring checks whether the clickstream service is running and can reach the analytics database.

**Why this priority**: Health checks are operational infrastructure, not user-facing. Needed for deployment orchestration.

**Independent Test**: Call the health endpoint and verify the service responds with status and database connectivity.

**Acceptance Scenarios**:

1. **Given** the clickstream service is running with a healthy database connection, **When** a monitoring system calls `GET /api/v1/health`, **Then** the service returns 200 with status "ok"
2. **Given** the analytics database is unreachable, **When** the health endpoint is called, **Then** the service returns 503 with database status "disconnected"

---

### Edge Cases

- What happens when the analytics_db is down during event ingestion? → Service accepts the event, queues/logs it, returns 202 (fire-and-forget principle). Event may be lost but caller is never blocked.
- What happens when an event payload is too large? → Reject with 413 Payload Too Large (limit 64KB per event, 512KB per batch).
- What happens when the service receives a non-JSON body? → Return 415 Unsupported Media Type.
- What happens when a batch contains 1000+ events? → Accept up to 100 events per batch; return 422 if exceeded.
- What happens when a client sends requests too quickly? → Per-IP token bucket rate limiting at the middleware layer; excess requests receive 429 Too Many Requests.

## Requirements

### Functional Requirements

- **FR-001**: System MUST accept POST `/api/v1/events` with a JSON body containing `event_name`, `user_id`, `session_id`, `client_ts` (ISO 8601), and optional `payload` object
- **FR-002**: System MUST accept POST `/api/v1/events/batch` with a JSON array of event objects
- **FR-003**: All event ingestion MUST return 202 Accepted and MUST NOT block the caller's UX flow
- **FR-004**: System MUST validate `event_name` against the MVP-1 taxonomy: `map_open`, `station_view`, `station_click`, `nearby_search`, `map_pan`, `map_zoom`
- **FR-005**: System MUST reject events with unknown event names with a 422 validation error
- **FR-006**: System MUST validate `event_name` and `session_id` are present and non-empty in every event
- **FR-007**: Batch ingestion MUST accept valid events even when some events in the batch fail validation (partial success)
- **FR-008**: System MUST respond with `GET /api/v1/health` returning service status and database connectivity
- **FR-009**: System MUST use the same JSON response envelope as other services (data, error, meta)
- **FR-010**: All endpoints MUST be under the `/api/v1/` prefix
- **FR-011**: System MUST use structured logging (tracing) for all ingested events with event_name, server_ts, and ingest result (one of: `accepted`, `rejected`, `db_error`)
- **FR-012**: System MUST capture the caller's IP address from the HTTP request and store it in raw_events.ip_address

### Key Entities

- **Event**: A user interaction record with event_name (string), user_id (string), session_id (string), client_ts (ISO 8601), and optional payload (JSONB). Stored in analytics_db.raw_events. The server additionally records server_ts on receipt.
- **Event Batch**: A collection of events sent together. Has a batch_id (nanoid) and list of events.
- **Event Taxonomy**: The set of valid event names defined per MVP. Currently MVP-1: map_open, station_view, station_click, nearby_search, map_pan, map_zoom.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Single event ingestion completes in under 100ms for 99% of requests (fire-and-forget latency)
- **SC-002**: Batch ingestion of 100 events completes in under 500ms
- **SC-003**: Clickstream service remains available and responsive even when analytics_db is unreachable — 100% of event requests receive a 202 response regardless of DB state
- **SC-004**: The service handles 500 concurrent event submissions without degradation below 200ms p95
- **SC-005**: Invalid events are rejected with clear validation error messages — 100% of invalid payloads receive appropriate error codes

## Assumptions

- The analytics_db is a separate PostgreSQL instance (port 5433) from platform_db — the clickstream service connects only to analytics_db
- The analytics_db schema has a `raw_events` table with columns: id, batch_id, event_name, user_id, session_id, payload (JSONB), client_ts (timestamp), server_ts (timestamp), ip_address
- No authentication is required for MVP-1 (consistent with driver-service approach)
- Event payload is a free-form JSON object with no schema validation beyond being valid JSON
- `user_id` may be null for anonymous users (pre-auth); `session_id` is always required
- Clickstream events must never undergo transformation or aggregation at ingest time — only raw storage
- The service stores its own schema/migrations for analytics_db

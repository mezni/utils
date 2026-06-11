# Research: Clickstream Service

**Phase**: Phase 0 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## 1. Fire-and-Forget Ingestion Pattern

**Decision**: `tokio::spawn` for DB writes; respond 202 immediately without awaiting the DB result.

**Rationale**: The spec mandates that callers must never be blocked by clickstream availability (SC-003). Using `tokio::spawn` to fire the INSERT statement in a background task achieves true fire-and-forget: the HTTP handler returns immediately, and the DB write continues asynchronously. If the DB is down, the spawned task logs the error via `tracing::error!` and the event is silently dropped (acceptable per spec: "Event may be lost but caller is never blocked").

**Alternatives considered**:
- **mpsc channel + background worker**: Adds a buffer queue and retry logic. Overkill for MVP-1 where event loss is acceptable. Adds complexity (channel sizing, backpressure, shutdown gracefulness).
- **tokio::task::spawn_blocking**: Not needed since sqlx is async-native.
- **Dedicated queue (RabbitMQ/Redis)**: Production-grade solution. Unnecessary for MVP-1; adds infrastructure dependencies.

## 2. Partial Batch Success Response

**Decision**: Return a JSON envelope that lists failed events by index within the batch array.

**Rationale**: When a batch contains both valid and invalid events, the spec requires valid ones to be accepted (FR-007). The client needs to know which events succeeded so it can retry only the failed ones. Index-based identification is unambiguous and consistent with common REST batch APIs (e.g., JSON:API, Elasticsearch _bulk API).

**Response shape**:
```json
{
  "data": {
    "accepted": 3,
    "failed": [
      { "index": 2, "error": "Unknown event_name: bad_event" }
    ]
  },
  "error": null,
  "meta": { "batch_id": "<nanoid>" }
}
```

**Alternatives considered**:
- **All-or-nothing atomic batch**: Rejects the entire batch if any event is invalid. Rejected per FR-007.
- **Return full event status array**: Includes success/fail for every event. More verbose but unnecessary for MVP-1 — clients only need to retry failed ones.

## 3. Rate Limiting

**Decision**: `actix-governor` crate with per-IP token bucket configuration.

**Rationale**: `actix-governor` is the most widely used Actix-web rate-limiting middleware. It implements the governor crate's token bucket algorithm, supports per-key (by IP) limits, and returns proper 429 responses. Configuration defaults: 100 requests per IP per burst window (configurable via env var).

**Alternatives considered**:
- **Custom in-memory rate limiter**: More control but duplicates battle-tested logic.
- **actix-middleware custom**: More boilerplate risk.

## 4. Database Migration Strategy

**Decision**: Embedded sqlx migrations (`sqlx::migrate!`) run on startup.

**Rationale**: sqlx's `migrate!` macro embeds `.sql` migration files at compile time and applies them in order via `migrator.run(&pool)`. This is the standard Rust pattern: zero external tools, migrations are version-controlled alongside the service, and they run automatically on startup. The `analytics_db` connection string is configured via `DATABASE_URL_ANALYTICS` env var.

**Migration file**: `migrations/001_create_raw_events.sql`
```sql
CREATE TABLE IF NOT EXISTS raw_events (
    id          BIGSERIAL PRIMARY KEY,
    batch_id    VARCHAR(21) NOT NULL,
    event_name  VARCHAR(50) NOT NULL,
    user_id     VARCHAR(255),
    session_id  VARCHAR(255) NOT NULL,
    payload     JSONB,
    client_ts   TIMESTAMPTZ NOT NULL,
    server_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip_address  VARCHAR(45)
);

CREATE INDEX idx_raw_events_event_name ON raw_events(event_name);
CREATE INDEX idx_raw_events_server_ts ON raw_events(server_ts);
CREATE INDEX idx_raw_events_session_id ON raw_events(session_id);
```

**Alternatives considered**:
- **sqlx-cli (separate migration tool)**: Works but adds a dev dependency not needed at runtime. Embedded migrations are simpler.
- **Raw SQL files + manual apply**: Error-prone; no version tracking.

## 5. Structured Logging Setup

**Decision**: `tracing` + `tracing-actix-web` + `tracing-subscriber` with JSON output.

**Rationale**: FR-011 requires structured logging for all ingested events. The `tracing` ecosystem is the standard for Rust async applications. `tracing-actix-web` provides a middleware that injects request span context automatically. The subscriber is configured with `JsonStorageLayer` for machine-parseable JSON logs.

**Log schema per ingested event**:
```json
{
  "event": "event_ingested",
  "event_name": "map_open",
  "batch_id": "abc123",
  "server_ts": "2026-06-11T12:00:00Z",
  "result": "accepted",
  "error": null
}
```

## 6. JSON Response Envelope

**Decision**: Same `{ data, error, meta }` envelope as driver-service.

**Rationale**: FR-009 mandates consistency with other services. The driver-service uses this envelope format. A shared helper crate (`ev-core` or a new `ev-api` crate) can provide the envelope type.

**Structure**:
```rust
struct ApiResponse<T> {
    data: Option<T>,
    error: Option<ApiError>,
    meta: Option<Meta>,
}

struct ApiError {
    code: String,
    message: String,
    details: Option<Value>,
}
```

## 7. Dependencies & Crate Versions

| Crate | Version | Purpose |
|-------|---------|---------|
| actix-web | 4 | HTTP framework |
| actix-governor | 0.6 | Rate limiting middleware |
| serde | 1 (with derive) | JSON serialization |
| serde_json | 1 | JSON value handling |
| sqlx | 0.8 (with postgres, runtime-tokio) | Async PostgreSQL |
| tokio | 1 | Async runtime |
| tracing | 0.1 | Structured logging |
| tracing-actix-web | 0.7 | Request span injection |
| tracing-subscriber | 0.3 (with json) | Log output |
| nanoid | 0.4 | Batch ID generation |

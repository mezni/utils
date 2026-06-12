# analytics_db Schema Documentation

**Database:** PostgreSQL 16  
**Owner:** Admin service (WRITE via events endpoint)  
**Access Model:** APPEND-ONLY (no UPDATE, no DELETE)  
**Purpose:** Immutable event log for analytics and intelligence

---

## Overview

`analytics_db` is a write-once, immutable event store. Events are ingested via:
- `POST /api/v1/events` — single event
- `POST /api/v1/events/batch` — up to 100 events

**Design Principles:**
- Immutable (no business logic overwrites historical data)
- Append-only (new rows only)
- High throughput (batched writes)
- Simple schema (flexible JSONB payload)

---

## Public Schema

### raw_events

Raw clickstream events from all sources (mobile, web, dashboard).

```sql
CREATE TABLE public.raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50),
    payload JSONB NOT NULL,
    occurred_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    client_ip INET
);

-- Index for event_type filtering
CREATE INDEX idx_raw_events_event_type ON public.raw_events(event_type);

-- Index for time-range queries (analytics aggregation)
CREATE INDEX idx_raw_events_occurred_at ON public.raw_events(occurred_at DESC);

-- Index for session analysis
CREATE INDEX idx_raw_events_session_id ON public.raw_events(session_id);

-- Index for user cohort analysis
CREATE INDEX idx_raw_events_user_id ON public.raw_events(user_id)
    WHERE user_id IS NOT NULL;

-- JSONB indexes for common payload fields (future optimization)
CREATE INDEX idx_raw_events_payload_station
    ON public.raw_events USING GIN(payload)
    WHERE event_type = 'station_viewed';
```

**Constraints:**
```sql
-- Prevent any UPDATE or DELETE
CREATE RULE no_update AS ON UPDATE TO public.raw_events
    DO INSTEAD NOTHING;

CREATE RULE no_delete AS ON DELETE TO public.raw_events
    DO INSTEAD NOTHING;
```

---

### Column Reference

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | BIGSERIAL | NO | Unique auto-incrementing ID (immutable identifier) |
| `event_type` | VARCHAR(50) | NO | Type of event (see enum below) |
| `session_id` | VARCHAR(50) | NO | Client session identifier (for session grouping) |
| `user_id` | VARCHAR(50) | YES | User ID if authenticated; NULL for anonymous |
| `payload` | JSONB | NO | Event-specific data (flexible structure) |
| `occurred_at` | TIMESTAMP | NO | Event timestamp (client-supplied, UTC) |
| `ingested_at` | TIMESTAMP | NO | Server timestamp when ingested (UTC, auto-generated) |
| `client_ip` | INET | YES | Client IP (for geo-analysis, optional) |

---

### Event Types

Events from the mobile/web driver app:

| Event Type | Payload Schema | Use Case |
|------------|----------------|----------|
| `station_viewed` | `{ station_id, source }` | User opened station detail |
| `station_searched` | `{ query, result_count }` | User initiated search query |
| `nearby_searched` | `{ lat, lng, radius_km, result_count }` | User triggered geo search |
| `charger_detail_viewed` | `{ station_id, charger_id, charger_type }` | User viewed charger info |
| `map_panned` | `{ lat, lng, zoom }` | User panned/dragged map |
| `map_zoomed` | `{ zoom_level, previous_zoom }` | User changed zoom |
| `session_started` | `{ app_version, os, locale }` | New session created |

---

### Payload Examples

```json
// station_viewed
{
  "station_id": "STA-abc123",
  "source": "map_marker"
}

// station_searched
{
  "query": "charging station tunis",
  "result_count": 12,
  "search_duration_ms": 342
}

// nearby_searched
{
  "lat": 36.8065,
  "lng": 10.1815,
  "radius_km": 5.0,
  "result_count": 8,
  "gps_accuracy_m": 25.5
}

// charger_detail_viewed
{
  "station_id": "STA-abc123",
  "charger_id": "CHR-def456",
  "charger_type": "CCS2",
  "charger_power_kw": 50
}

// map_panned
{
  "lat": 36.8100,
  "lng": 10.1850,
  "zoom": 13,
  "pan_distance_km": 0.8
}

// map_zoomed
{
  "zoom_level": 14,
  "previous_zoom": 12,
  "zoom_direction": "in"
}

// session_started
{
  "app_version": "1.0.0",
  "os": "iOS",
  "os_version": "17.4",
  "locale": "fr_TN",
  "session_id": "sess-abc123"
}
```

---

## Write Model

### Single Event Ingestion

```bash
curl -X POST http://localhost:8081/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "station_viewed",
    "session_id": "sess-abc123",
    "user_id": "USR-abc123",
    "payload": { "station_id": "STA-abc123", "source": "map_marker" },
    "occurred_at": "2026-06-10T14:30:00Z"
  }'
```

Response: `202 Accepted`

### Batch Event Ingestion

```bash
curl -X POST http://localhost:8081/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      { "event_type": "station_viewed", ... },
      { "event_type": "map_panned", ... }
    ]
  }'
```

Response: `202 Accepted` with `{ "accepted": N, "rejected": M }`

---

## Read Model (Analytics Queries)

### Session Timeline

Reconstruct user session chronologically:

```sql
SELECT occurred_at, event_type, payload
FROM public.raw_events
WHERE session_id = 'sess-abc123'
ORDER BY occurred_at ASC;
```

### Station Popularity

Count view events by station:

```sql
SELECT
    payload->>'station_id' as station_id,
    COUNT(*) as view_count,
    DATE(occurred_at) as day
FROM public.raw_events
WHERE event_type = 'station_viewed'
GROUP BY station_id, DATE(occurred_at)
ORDER BY view_count DESC;
```

### Search Funnel

From search to charger view:

```sql
WITH search_events AS (
    SELECT session_id, occurred_at as search_time
    FROM public.raw_events
    WHERE event_type IN ('station_searched', 'nearby_searched')
),
view_events AS (
    SELECT session_id, MIN(occurred_at) as view_time
    FROM public.raw_events
    WHERE event_type = 'station_viewed'
    GROUP BY session_id
)
SELECT
    COUNT(DISTINCT s.session_id) as search_sessions,
    COUNT(DISTINCT v.session_id) as view_sessions,
    ROUND(100.0 * COUNT(DISTINCT v.session_id) / COUNT(DISTINCT s.session_id), 1) as conversion_pct
FROM search_events s
LEFT JOIN view_events v ON s.session_id = v.session_id AND v.view_time > s.search_time;
```

### Average Session Duration

```sql
WITH session_times AS (
    SELECT
        session_id,
        MIN(occurred_at) as session_start,
        MAX(occurred_at) as session_end
    FROM public.raw_events
    GROUP BY session_id
)
SELECT
    AVG(EXTRACT(EPOCH FROM (session_end - session_start))) / 60 as avg_duration_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_end - session_start) as median_duration
FROM session_times;
```

### User Retention (Cohort Analysis)

First session date and return activity:

```sql
WITH first_session AS (
    SELECT
        user_id,
        DATE(MIN(occurred_at)) as first_session_date
    FROM public.raw_events
    WHERE user_id IS NOT NULL
    GROUP BY user_id
),
return_activity AS (
    SELECT
        fs.user_id,
        fs.first_session_date,
        COUNT(DISTINCT DATE(re.occurred_at)) as active_days,
        MAX(re.occurred_at) as last_seen
    FROM first_session fs
    JOIN public.raw_events re ON fs.user_id = re.user_id
    GROUP BY fs.user_id, fs.first_session_date
)
SELECT
    first_session_date,
    COUNT(*) as cohort_size,
    ROUND(100.0 * COUNT(CASE WHEN active_days >= 2 THEN 1 END) / COUNT(*), 1) as pct_return_users
FROM return_activity
GROUP BY first_session_date
ORDER BY first_session_date DESC;
```

---

## Data Governance

### Immutability Enforcement

1. **NO UPDATE** — use `CREATE RULE no_update` (see schema above)
2. **NO DELETE** — use `CREATE RULE no_delete` (see schema above)
3. **INSERT ONLY** — only mechanism to add data

Why? Historical data is precious for analytics. Overwrites obscure user behavior patterns.

### Data Retention

Currently: **Keep all events indefinitely** (MVP-1/2)

Future policy (MVP-4+):
- Retain raw events for 24 months
- Archive older events to cold storage (S3)
- Aggregate into summarized analytics tables

### Privacy & Compliance

- `user_id` is nullable (respect anonymous sessions)
- `client_ip` is optional (can be omitted for privacy)
- No PII in payloads (usernames, email, passwords)
- GDPR: User deletion adds `user_id` to a `deleted_users` table; joins exclude deleted users

```sql
-- Example: GDPR right-to-be-forgotten
INSERT INTO public.deleted_users(user_id, deleted_at)
VALUES ('USR-abc123', NOW());

-- Query respects deletion:
SELECT * FROM raw_events
WHERE NOT EXISTS (
    SELECT 1 FROM deleted_users
    WHERE deleted_users.user_id = raw_events.user_id
);
```

---

## Performance Considerations

### Write Throughput

Expected load:
- MVP-1: 100-500 events/sec
- MVP-2: 500-2000 events/sec
- MVP-3: 2000-5000 events/sec
- MVP-4+: Optimize as needed

Batch writes (`/api/v1/events/batch`) improve throughput by 5-10x over single-event writes.

### Index Strategy

1. `event_type` — fast filtering by event class
2. `occurred_at` — time-range queries for reporting
3. `session_id` — user session reconstruction
4. `user_id` — cohort analysis
5. `payload` (JSONB GIN) — future optimization for complex queries

### Query Optimization

For large date ranges (e.g., 1M+ rows), use:
- Time-range filters: `WHERE occurred_at >= ... AND occurred_at < ...`
- Partial indexes: `CREATE INDEX ... WHERE event_type = 'X'`
- Materialized views for common aggregations (future)

---

## Backup & Recovery

### Point-in-Time Recovery (PITR)

Enable WAL archiving for PITR:
```sql
-- In postgresql.conf
wal_level = replica
archive_mode = on
archive_command = 'cp %p /archive/%f'
```

### Export for Analysis

Export events to CSV for external analysis (Tableau, Looker):
```sql
COPY (
    SELECT id, event_type, session_id, user_id, payload, occurred_at
    FROM public.raw_events
    WHERE occurred_at >= '2026-06-01' AND occurred_at < '2026-07-01'
)
TO STDOUT
WITH (FORMAT CSV, HEADER);
```

---

## Scaling Path (Future)

As event volume grows:

1. **MVP-1/2:** Single table, basic indexes
2. **MVP-3:** Partitioning by month: `raw_events_2026_06`, `raw_events_2026_07`
3. **MVP-4:** Stream analytics (Kafka + Flink) for real-time aggregations
4. **MVP-5:** Data lake (S3 + Athena/BigQuery) for historical analysis

Each stage is opt-in and doesn't break the append-only contract.

---

## References

- [Immutable data patterns](https://www.postgresql.org/docs/current/sql-createrule.html)
- [JSONB indexing](https://www.postgresql.org/docs/current/datatype-json.html)
- [Table partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)

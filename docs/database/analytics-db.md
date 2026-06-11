# analytics_db — Event Stream

**Engine:** PostgreSQL 16
**Core Principle:** Append-only, immutable events

---

## Tables

### raw_events (PRIMARY)

```sql
CREATE TABLE raw_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  event_name TEXT NOT NULL,

  user_id TEXT NULL,
  session_id TEXT NULL,

  payload JSONB NOT NULL DEFAULT '{}'::jsonb,

  app_version TEXT,
  platform TEXT,                      -- ios | android | web

  latitude  NUMERIC(10,7),
  longitude NUMERIC(10,7),

  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_events_name    ON raw_events(event_name);
CREATE INDEX idx_events_time    ON raw_events(created_at);
CREATE INDEX idx_events_user    ON raw_events(user_id);
CREATE INDEX idx_events_session ON raw_events(session_id);
```

### event_types (taxonomy control)

```sql
CREATE TABLE event_types (
  event_name TEXT PRIMARY KEY,

  description TEXT,
  category TEXT,                      -- discovery | navigation | engagement | system | error

  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### event_aggregates (MVP-4+)

```sql
CREATE TABLE event_aggregates (
  event_name TEXT,
  date DATE,

  total_count BIGINT DEFAULT 0,
  unique_users BIGINT DEFAULT 0,
  unique_sessions BIGINT DEFAULT 0,

  PRIMARY KEY (event_name, date)
);
```

### station_analytics (derived)

```sql
CREATE TABLE station_analytics (
  station_id TEXT,
  date DATE,

  views BIGINT DEFAULT 0,
  clicks BIGINT DEFAULT 0,
  navigation_requests BIGINT DEFAULT 0,

  avg_session_time_seconds BIGINT,

  PRIMARY KEY (station_id, date)
);
```

### session_events (debug UX)

```sql
CREATE TABLE session_events (
  session_id TEXT,
  event_sequence INT,

  event_name TEXT,
  payload JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),

  PRIMARY KEY (session_id, event_sequence)
);
```

---

## Event Taxonomy

### Discovery

| Event | Meaning |
|---|---|
| map_open | user opened map |
| station_view | station details opened |
| station_click | marker clicked |
| nearby_search | radius search executed |

### Navigation

| Event | Meaning |
|---|---|
| route_request | user requests navigation |
| map_pan | map moved |
| map_zoom | zoom interaction |

### Engagement

| Event | Meaning |
|---|---|
| favorite_add | station saved |
| favorite_remove | station unsaved |

### System

| Event | Meaning |
|---|---|
| api_error | backend error |
| latency_report | performance metric |

---

## Data Rules

- **Append-only:** No UPDATE or DELETE on raw_events
- **Integrity:** user_id nullable (anonymous), session_id required for UX tracking
- **Payload standard:** `{ "station_id": "STA-xxx", "latency_ms": 120, "source": "map" }`
- **Privacy:** No PII, passwords, or auth tokens stored

---

## Performance Design

- Bulk inserts supported (batch endpoint)
- Minimal indexes on raw_events
- **Future:** Partitioning by RANGE (created_at), monthly partitions

---

## Analytics Evolution

| MVP | Capability |
|---|---|
| MVP-1 | raw event capture |
| MVP-2 | basic dashboard queries |
| MVP-3 | user segmentation |
| MVP-4 | event_aggregates + station_analytics |
| MVP-5+ | predictive analytics |

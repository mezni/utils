# Data Model: MVP-1 Stabilization Sprint

**Date**: 2026-06-13
**Feature**: MVP-1 Stabilization Sprint
**Reference**: [spec.md](./spec.md), [research.md](./research.md)

## Overview

This data model defines entities for performance monitoring, event tracking, and error logging to support the stabilization sprint's quality assurance and observability requirements.

---

## Entities

### 1. Performance Metric (monitoring)

**Purpose**: Track application performance metrics across devices and user actions

**Table**: `performance_metrics` (platform_db)

**Fields**:

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, default gen_random_uuid() | Unique metric identifier |
| metric_type | VARCHAR(50) | NOT NULL | Type of metric (response_time, frame_rate, memory_usage, cpu_usage, battery_drain) |
| value_ms | FLOAT | NOT NULL | Metric value in milliseconds (for response_time) |
| value_percent | FLOAT | NOT NULL | Metric value as percentage (for battery_drain) |
| user_action | VARCHAR(100) | NOT NULL | User action that triggered metric (e.g., 'station_list_view', 'map_pan', 'station_detail_view') |
| device_info | JSONB | NOT NULL | Device information (model, OS version, screen_size) |
| timestamp | TIMESTAMP WITH TIME ZONE | NOT NULL, default NOW() | When metric was recorded |
| environment | VARCHAR(20) | NOT NULL, default 'production' | Environment (production/staging/dev) |

**Indexes**:
- `idx_performance_timestamp` ON (timestamp DESC)
- `idx_performance_metric_type` ON (metric_type)
- `idx_performance_user_action` ON (user_action)

**Validations**:
- `metric_type` must be one of: response_time, frame_rate, memory_usage, cpu_usage, battery_drain
- `value_ms` >= 0 (for response_time and frame_rate)
- `value_percent` >= 0 and <= 100 (for battery_drain)
- `device_info` must be valid JSONB

**State Transitions**: No lifecycle (append-only tracking)

**Example**:
```sql
INSERT INTO performance_metrics (metric_type, value_ms, user_action, device_info, timestamp)
VALUES ('response_time', 185.3, 'station_list_view', '{"model": "iPhone 14", "os": "iOS 17", "screen_size": "6.1"}', '2026-06-13T10:30:00Z');
```

---

### 2. Error Log (analytics_db)

**Purpose**: Track application errors and recovery actions for debugging and monitoring

**Table**: `raw_events` (analytics_db)

**Fields**:

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, default gen_random_uuid() | Unique event identifier |
| event_type | VARCHAR(50) | NOT NULL | Type of event (network_error, server_error, timeout, permission_denial, cache_error) |
| user_action | VARCHAR(100) | NOT NULL | User action that failed (e.g., 'load_stations', 'view_station', 'search_nearby') |
| error_message | TEXT | NOT NULL | Error description for debugging |
| device_info | JSONB | NOT NULL | Device information (model, OS version, screen_size) |
| timestamp | TIMESTAMP WITH TIME ZONE | NOT NULL, default NOW() | When error occurred |
| recovery_action | VARCHAR(100) | NULL | Action user took (e.g., 'retry_button_pressed', 'close_button_pressed') |
| recovery_success | BOOLEAN | NOT NULL, default false | Whether recovery action succeeded |
| retry_count | INTEGER | NOT NULL, default 0 | Number of retry attempts made |
| metadata | JSONB | NULL | Additional context (error code, request_id, stack_trace) |

**Indexes**:
- `idx_events_timestamp` ON (timestamp DESC)
- `idx_events_type` ON (event_type)
- `idx_events_user_action` ON (user_action)

**Validations**:
- `event_type` must be one of: network_error, server_error, timeout, permission_denial, cache_error
- `recovery_success` is BOOLEAN (true/false)
- `retry_count` >= 0
- `metadata` must be valid JSONB

**Rules** (Append-Only):
- No UPDATE operations allowed on this table
- No DELETE operations allowed on this table
- All event logs are preserved indefinitely for auditing

**Example**:
```sql
INSERT INTO raw_events (event_type, user_action, error_message, device_info, timestamp, recovery_action, recovery_success, retry_count)
VALUES ('network_error', 'load_stations', 'Connection timeout after 30000ms', '{"model": "Pixel 7", "os": "Android 13", "screen_size": "6.3"}', '2026-06-13T10:35:00Z', 'retry_button_pressed', true, 2);
```

---

### 3. Event Bundle (in-memory)

**Purpose**: Aggregate events for batch ingestion to analytics_db

**Entity Structure**:

```typescript
interface EventBundle {
  events: Array<{
    id: string;
    event_type: string;
    user_action: string;
    error_message?: string;
    device_info: DeviceInfo;
    timestamp: string;
    recovery_action?: string;
    recovery_success?: boolean;
    retry_count: number;
    metadata?: Record<string, any>;
  }>;
  batch_id: string; // UUID
  created_at: string; // ISO 8601
  retry_count: number; // Number of retries attempted
  max_size: number; // 100 events max
  timeout_ms: number; // 500ms timeout
}
```

**Behavior**:
- Collects up to 100 events or until timeout (500ms)
- Sends batch to backend API endpoint: `POST /api/v1/events/batch`
- Retries on transient failures (network blips)
- Drops on permanent failures (analytics DB unreachable)
- Tracks retry attempts in `retry_count`

**Validation**:
- `max_size` must be >= 1
- `timeout_ms` must be >= 100ms
- All events in batch must have unique IDs

---

## Relationships

### Performance Metric ↔ User Action

- **Many-to-Many**: One metric can be recorded for one user action
- **Example**: Multiple response_time metrics can be recorded for 'station_list_view'
- **Foreign Key**: N/A (performance metrics are independent logs)

### Error Log ↔ User Action

- **One-to-Many**: One user action can result in multiple error logs (retries)
- **Example**: One 'load_stations' action can have multiple error logs if retries fail
- **Foreign Key**: N/A (error logs are independent audit trails)

### Event Bundle ↔ Error Log

- **One-to-Many**: One event bundle contains multiple error logs
- **Example**: Batch of 50 events includes 3 network errors
- **Foreign Key**: N/A (event bundle is transient in-memory structure)

---

## Lifecycle & State

### Performance Metrics
- **State**: Active (always being recorded)
- **Lifecycle**: Append-only, never deleted
- **Retention**: Indefinite (for performance trend analysis)
- **Monitoring**: Periodic aggregation (e.g., daily, weekly)

### Error Logs
- **State**: Active (always being recorded)
- **Lifecycle**: Append-only, never deleted
- **Retention**: Indefinite (for error trend analysis and debugging)
- **Monitoring**: Periodic alerting on error rate thresholds

### Event Bundles
- **State**: Transient (in-memory only)
- **Lifecycle**: Created → Batched → Sent → Purged
- **Retention**: Temporary (cleared after successful batch send)
- **Failure Handling**: Retries until success or max retries reached

---

## Query Examples

### 1. Response Time Analysis
```sql
-- Calculate p95 response time for a specific action
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY value_ms) AS p95_response_time
FROM performance_metrics
WHERE metric_type = 'response_time'
  AND user_action = 'station_list_view'
  AND timestamp >= NOW() - INTERVAL '1 day';
```

### 2. Error Rate by Action
```sql
-- Calculate error rate for a specific action
SELECT
  user_action,
  COUNT(*) AS total_events,
  SUM(CASE WHEN recovery_success = false THEN 1 ELSE 0 END) AS failed_events,
  SUM(CASE WHEN recovery_success = true THEN 1 ELSE 0 END) AS successful_events,
  ROUND(
    100.0 * SUM(CASE WHEN recovery_success = false THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS failure_rate
FROM raw_events
WHERE event_type = 'network_error'
  AND timestamp >= NOW() - INTERVAL '1 day'
GROUP BY user_action
ORDER BY failure_rate DESC;
```

### 3. Battery Drain by Device
```sql
-- Average battery drain by device model
SELECT
  device_info->>'model' AS device_model,
  AVG(value_percent) AS avg_battery_drain,
  COUNT(*) AS measurements
FROM performance_metrics
WHERE metric_type = 'battery_drain'
  AND timestamp >= NOW() - INTERVAL '1 day'
GROUP BY device_info->>'model'
ORDER BY avg_battery_drain DESC;
```

### 4. Event Batch Status
```typescript
// Frontend: Check if event batch was successfully sent
interface EventBatchResponse {
  batch_id: string;
  sent_count: number;
  failed_count: number;
  timestamp: string;
}

// Check last batch send
const { data } = useQuery(['lastEventBatch'], fetchLastEventBatch);
console.log(`Last batch: ${data?.sent_count} events sent, ${data?.failed_count} failed`);
```

---

## Validations & Constraints

### Performance Metrics
- `metric_type` enum: response_time, frame_rate, memory_usage, cpu_usage, battery_drain
- `value_ms`: Must be >= 0 (response_time, frame_rate)
- `value_percent`: Must be >= 0 and <= 100 (battery_drain)
- `device_info`: Must be valid JSONB with required fields (model, os, screen_size)
- `timestamp`: ISO 8601 UTC format

### Error Logs
- `event_type` enum: network_error, server_error, timeout, permission_denial, cache_error
- `recovery_action`: Must be one of [retry_button_pressed, close_button_pressed, reload_button_pressed]
- `recovery_success`: BOOLEAN (true/false)
- `retry_count`: Must be >= 0
- `device_info`: Must be valid JSONB with required fields (model, os, screen_size)
- `timestamp`: ISO 8601 UTC format
- `metadata`: Optional JSONB for error codes, request IDs, stack traces

### Event Bundles
- `max_size`: Must be 100 (configurable)
- `timeout_ms`: Must be 500 (configurable)
- `retry_count`: Must be >= 0
- Events in bundle must have unique IDs

---

## Monitoring & Alerting

### Performance Thresholds
- Response time p95 > 300ms: ⚠️ Warning
- Response time p95 > 500ms: 🔴 Critical
- Battery drain > 5% per hour: ⚠️ Warning
- Battery drain > 10% per hour: 🔴 Critical

### Error Thresholds
- Error rate > 10%: ⚠️ Warning
- Error rate > 25%: 🔴 Critical
- Event ingestion failure: 🔴 Critical (all retry attempts failed)

---

## Database Schema Impact

### platform_db (adds 1 table)

```sql
-- Add performance metrics table
CREATE TABLE IF NOT EXISTS performance_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_type VARCHAR(50) NOT NULL CHECK (metric_type IN ('response_time', 'frame_rate', 'memory_usage', 'cpu_usage', 'battery_drain')),
  value_ms FLOAT NOT NULL CHECK (value_ms >= 0),
  value_percent FLOAT NOT NULL CHECK (value_percent >= 0 AND value_percent <= 100),
  user_action VARCHAR(100) NOT NULL,
  device_info JSONB NOT NULL,
  timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  environment VARCHAR(20) NOT NULL DEFAULT 'production' CHECK (environment IN ('production', 'staging', 'dev')),
  CONSTRAINT performance_metrics_user_action_check CHECK (user_action IN (
    'station_list_view', 'station_detail_view', 'map_pan', 'search_nearby',
    'marker_press', 'filter_apply', 'settings_toggle', 'theme_change'
  ))
);

CREATE INDEX idx_performance_timestamp ON performance_metrics(timestamp DESC);
CREATE INDEX idx_performance_metric_type ON performance_metrics(metric_type);
CREATE INDEX idx_performance_user_action ON performance_metrics(user_action);
```

### analytics_db (no changes - append-only table exists)

Existing `raw_events` table is sufficient for error logging. No schema changes needed.

---

## Summary

This data model provides:
- ✅ **Performance monitoring**: Track response times, frame rates, battery drain
- ✅ **Error tracking**: Log errors with recovery actions and retry attempts
- ✅ **Device information**: Capture device model, OS version for cross-device testing
- ✅ **Append-only design**: Analytics database remains immutable (constitution compliance)
- ✅ **Monitoring ready**: Queryable for trend analysis and alerting

All entities are append-only (except Event Bundle which is transient), ensuring analytics database integrity per constitution principles.

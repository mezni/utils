# Data Model: Admin Analytics Read Layer

**Feature**: Admin Analytics Read Layer (Sprint 4)
**Date**: 2026-06-22
**Status**: Complete

## Overview

This document defines the data model for the admin analytics read layer, including materialized views, cache entries, and aggregation entities. All data structures are derived from telemetry events and enforce read-only access constraints.

---

## 1. Materialized Views

### 1.1 station_usage (Materialized View)

**Purpose**: Pre-aggregated station-level usage statistics

**Source**: `analytics_events` table (Sprint 3)

**Schema**:
```sql
CREATE MATERIALIZED VIEW station_usage AS
SELECT
    station_id,
    COUNT(*) AS station_views,
    SUM(favorite_count) AS total_favorites,
    COUNT(DISTINCT user_uuid) AS unique_users,
    COUNT(CASE WHEN event_type = 'FAVORITE' THEN 1 END) AS favorite_count,
    AVG(DATEDIFF(second, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY station_id ORDER BY event_timestamp))) AS avg_session_gap_seconds,
    MAX(event_timestamp) AS last_viewed_at,
    MIN(event_timestamp) AS first_viewed_at
FROM analytics_events
WHERE event_timestamp >= COALESCE(
    (SELECT MAX(last_refreshed_at) FROM materialized_view_meta WHERE view_name = 'station_usage'),
    '1970-01-01'
)
GROUP BY station_id;
```

**Fields**:

| Field | Type | Description | Derivation |
|-------|------|-------------|------------|
| `station_id` | TEXT | Station entity ID | Event.station_id |
| `station_views` | BIGINT | Total number of views | COUNT(*) from events |
| `total_favorites` | BIGINT | Sum of favorite counts per event | SUM(favorite_count) from events |
| `unique_users` | BIGINT | Number of distinct users | COUNT(DISTINCT user_uuid) |
| `favorite_count` | BIGINT | Number of favorite events | COUNT(CASE WHEN event_type = 'FAVORITE' THEN 1 END) |
| `avg_session_gap_seconds` | NUMERIC(10,2) | Average seconds between sessions per station | AVG(lead event_timestamp - event_timestamp) |
| `last_viewed_at` | TIMESTAMP | Most recent view timestamp | MAX(event_timestamp) |
| `first_viewed_at` | TIMESTAMP | Oldest view timestamp | MIN(event_timestamp) |

**Query Safety**: Parameterized queries only (no dynamic SQL)

**Partitioning**: None (station_id is primary key, queries use range queries)

**Indexes**:
- PRIMARY KEY (station_id)

**Refresh Strategy**: Incremental refresh on event ingestion + scheduled refresh every 5 minutes

---

### 1.2 user_activity (Materialized View)

**Purpose**: Pre-aggregated user-level activity patterns

**Source**: `analytics_events` table (Sprint 3)

**Schema**:
```sql
CREATE MATERIALIZED VIEW user_activity AS
SELECT
    user_uuid,
    COUNT(*) AS total_views,
    COUNT(DISTINCT station_id) AS stations_visited,
    COUNT(DISTINCT operator_id) AS operators_visited,
    COUNT(CASE WHEN event_type = 'FAVORITE' THEN 1 END) AS favorites_count,
    COUNT(CASE WHEN event_type = 'SEARCH' THEN 1 END) AS search_count,
    AVG(DATEDIFF(second, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY user_uuid ORDER BY event_timestamp))) AS avg_session_gap_seconds,
    MAX(event_timestamp) AS last_active_at,
    MIN(event_timestamp) AS first_active_at
FROM analytics_events
WHERE event_timestamp >= COALESCE(
    (SELECT MAX(last_refreshed_at) FROM materialized_view_meta WHERE view_name = 'user_activity'),
    '1970-01-01'
)
GROUP BY user_uuid;
```

**Fields**:

| Field | Type | Description | Derivation |
|-------|------|-------------|------------|
| `user_uuid` | UUID | User identity (Keycloak UUID) | Event.user_uuid |
| `total_views` | BIGINT | Total number of events by user | COUNT(*) from events |
| `stations_visited` | BIGINT | Number of distinct stations visited | COUNT(DISTINCT station_id) |
| `operators_visited` | BIGINT | Number of distinct operators visited | COUNT(DISTINCT operator_id) |
| `favorites_count` | BIGINT | Number of favorite events | COUNT(CASE WHEN event_type = 'FAVORITE' THEN 1 END) |
| `search_count` | BIGINT | Number of search events | COUNT(CASE WHEN event_type = 'SEARCH' THEN 1 END) |
| `avg_session_gap_seconds` | NUMERIC(10,2) | Average seconds between sessions per user | AVG(lead event_timestamp - event_timestamp) |
| `last_active_at` | TIMESTAMP | Most recent activity timestamp | MAX(event_timestamp) |
| `first_active_at` | TIMESTAMP | Oldest activity timestamp | MIN(event_timestamp) |

**Query Safety**: Parameterized queries only (no dynamic SQL)

**Partitioning**: None (user_uuid is primary key, queries use range queries)

**Indexes**:
- PRIMARY KEY (user_uuid)

**Refresh Strategy**: Incremental refresh on event ingestion + scheduled refresh every 5 minutes

---

### 1.3 search_trends (Materialized View)

**Purpose**: Pre-aggregated search query patterns and trends

**Source**: `analytics_events` table (Sprint 3)

**Schema**:
```sql
CREATE MATERIALIZED VIEW search_trends AS
SELECT
    query_text,
    COUNT(*) AS search_count,
    COUNT(DISTINCT user_uuid) AS unique_searchers,
    COUNT(DISTINCT station_id) AS stations_searched,
    AVG(DATEDIFF(hour, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY query_text ORDER BY event_timestamp))) AS query_frequency_hours,
    MAX(event_timestamp) AS last_search_at,
    MIN(event_timestamp) AS first_search_at,
    COUNT(CASE WHEN event_type = 'SEARCH' THEN 1 END) AS search_events
FROM analytics_events
WHERE event_timestamp >= COALESCE(
    (SELECT MAX(last_refreshed_at) FROM materialized_view_meta WHERE view_name = 'search_trends'),
    '1970-01-01'
)
GROUP BY query_text
HAVING COUNT(*) >= 5;  -- Only aggregate queries with minimum 5 occurrences
```

**Fields**:

| Field | Type | Description | Derivation |
|-------|------|-------------|------------|
| `query_text` | TEXT | Search query text | Event.payload.query_text |
| `search_count` | BIGINT | Total number of search events | COUNT(*) from events |
| `unique_searchers` | BIGINT | Number of distinct users who searched | COUNT(DISTINCT user_uuid) |
| `stations_searched` | BIGINT | Number of distinct stations in results | COUNT(DISTINCT station_id) |
| `query_frequency_hours` | NUMERIC(10,2) | Average hours between queries | AVG(lead event_timestamp - event_timestamp) |
| `last_search_at` | TIMESTAMP | Most recent search timestamp | MAX(event_timestamp) |
| `first_search_at` | TIMESTAMP | Oldest search timestamp | MIN(event_timestamp) |
| `search_events` | BIGINT | Number of search events (duplicate of search_count) | COUNT(CASE WHEN event_type = 'SEARCH' THEN 1 END) |

**Query Safety**: Parameterized queries only (no dynamic SQL)

**Partitioning**: None (query_text is primary key, queries use LIKE patterns)

**Indexes**:
- PRIMARY KEY (query_text)
- GIN index on query_text for LIKE pattern matching (optional optimization)

**Refresh Strategy**: Incremental refresh on event ingestion + scheduled refresh every 5 minutes

---

## 2. Cache Entities

### 2.1 Cache Entry

**Purpose**: Represents a cached analytics query result

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub key: String,
    pub value: serde_json::Value,
    pub ttl_seconds: u64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    pub fn remaining_ttl(&self) -> Duration {
        self.expires_at - Utc::now()
    }
}
```

**Storage**: Redis

**TTL Policy**:
- `station_{{station_id}}`: 5 minutes (300 seconds)
- `summary`: 10 minutes (600 seconds)
- `user_{{user_uuid}}`: 5 minutes (300 seconds)
- `search_{{query_text}}`: 10 minutes (600 seconds)

**Invalidation**: Synchronous callback from driver-service on event ingestion

---

### 2.2 Cache Invalidation Request

**Purpose**: Represents a cache invalidation request from driver-service

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInvalidationRequest {
    pub station_id: String,
    pub user_uuid: Option<String>,
    pub event_type: EventType,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    VIEW,
    FAVORITE,
    SEARCH,
    LOGIN,
    LOGOUT,
}
```

**Transport**: HTTP POST to admin-service cache invalidation endpoint

**Retry Policy**: 3 retries with exponential backoff, circuit breaker pattern

---

## 3. Analytics Query Models

### 3.1 AnalyticsResponse

**Purpose**: Generic response structure for analytics queries

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsResponse<T> {
    pub data: T,
    pub metadata: AnalyticsMetadata,
    pub cache_status: CacheStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsMetadata {
    pub request_id: String,
    pub query_duration_ms: u64,
    pub timestamp: String,
    pub cached: bool,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatus {
    pub status: String,  // "hit" | "miss" | "error"
    pub latency_ms: u64,
    pub ttl_remaining_seconds: Option<u64>,
}
```

**Usage**: Generic wrapper for all analytics endpoints

---

### 3.2 StationAnalytics (Station Intelligence)

**Purpose**: Station-specific analytics response

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationAnalytics {
    pub station_id: String,
    pub station_views: u64,
    pub favorites: u64,
    pub search_hits: u64,
    pub avg_session_time_seconds: f64,
    pub unique_users: u64,
    pub last_viewed_at: Option<String>,
    pub first_viewed_at: Option<String>,
    pub partner_id: Option<String>,
}

impl From<StationUsageRow> for StationAnalytics {
    fn from(row: StationUsageRow) -> Self {
        Self {
            station_id: row.station_id,
            station_views: row.station_views,
            favorites: row.favorite_count,
            search_hits: 0,  // Calculated separately
            avg_session_time_seconds: row.avg_session_gap_seconds,
            unique_users: row.unique_users,
            last_viewed_at: row.last_viewed_at.map(|dt| dt.to_rfc3339()),
            first_viewed_at: row.first_viewed_at.map(|dt| dt.to_rfc3339()),
            partner_id: None,  // Partner filtering applied in query
        }
    }
}
```

**Derivation**: Materialized view `station_usage` + calculation of search_hits from events

---

### 3.3 SummaryAnalytics (KPI Aggregation)

**Purpose**: Platform-level KPI aggregation

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryAnalytics {
    pub station_views: u64,
    pub search_volume: u64,
    pub favorite_count: u64,
    pub active_users: u64,
    pub total_stations: u64,
    pub total_users: u64,
    pub total_searches: u64,
    pub trends: Vec<String>,  // Top search queries
}

impl SummaryAnalytics {
    pub fn new(station_views: u64, search_volume: u64, favorite_count: u64, active_users: u64) -> Self {
        Self {
            station_views,
            search_volume,
            favorite_count,
            active_users,
            total_stations: 0,
            total_users: 0,
            total_searches: 0,
            trends: Vec::new(),
        }
    }
}
```

**Derivation**: Aggregated from materialized views (station_usage, user_activity, search_trends)

---

## 4. Query Request Models

### 4.1 AnalyticsQuery

**Purpose**: Parameters for analytics queries with filtering and pagination

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsQuery {
    pub station_id: Option<String>,
    pub user_uuid: Option<String>,
    pub partner_id: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub page: usize,
    pub per_page: usize,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,  // "asc" or "desc"
}

impl AnalyticsQuery {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.page < 1 {
            return Err(ValidationError::InvalidPageNumber);
        }

        if self.per_page < 1 || self.per_page > 100 {
            return Err(ValidationError::InvalidPageSize);
        }

        if self.start_date.is_some() && self.end_date.is_some() {
            if self.start_date > self.end_date {
                return Err(ValidationError::InvalidDateRange);
            }
        }

        Ok(())
    }
}
```

**Validation**:
- Page number: >= 1
- Page size: 1-100
- Date range: start_date <= end_date

---

## 5. Materialized View Metadata

### 5.1 MaterializedViewMeta

**Purpose**: Track last refresh time for materialized views

**Schema**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewMeta {
    pub view_name: String,
    pub last_refreshed_at: DateTime<Utc>,
    pub rows_count: u64,
}
```

**Storage**: Table in `analytics_db`

**Query**:
```sql
CREATE TABLE materialized_view_meta (
    view_name TEXT PRIMARY KEY,
    last_refreshed_at TIMESTAMP NOT NULL,
    rows_count BIGINT NOT NULL
);
```

**Usage**: Track view refresh time for incremental updates

---

## 6. Database Schema

### 6.1 analytics_db Schema (Existing)

**Schemas**:
- `analytics_events` (driver-service writes)
- `materialized_view_meta` (admin-service read-only)

**Tables**:

| Table | Owner | Purpose | Access |
|-------|-------|---------|--------|
| `analytics_events` | driver-service | Raw telemetry events | READ/WRITE (driver-service only) |
| `station_usage` | admin-service | Materialized view | READ ONLY (admin-service only) |
| `user_activity` | admin-service | Materialized view | READ ONLY (admin-service only) |
| `search_trends` | admin-service | Materialized view | READ ONLY (admin-service only) |
| `materialized_view_meta` | admin-service | Metadata table | READ ONLY (admin-service only) |

**Permissions**:
- `bornemap_analytics_reader` role: SELECT on all views + metadata
- `bornemap_analytics_writer` role: INSERT on events (driver-service only)

---

## 7. Relationships

### 7.1 Event → StationUsage
```
analytics_events.station_id → station_usage.station_id (1:1 relation via view)
```

### 7.2 Event → UserActivity
```
analytics_events.user_uuid → user_activity.user_uuid (1:1 relation via view)
```

### 7.3 Event → SearchTrends
```
analytics_events.payload.query_text → search_trends.query_text (1:1 relation via view)
```

### 7.4 StationUsage → UserActivity
```
No direct relationship (separate views)
```

### 7.5 AnalyticsResponse → CacheEntry
```
AnalyticsResponse.cache_status → CacheEntry.is_expired (validation)
```

---

## 8. Validation Rules

### 8.1 Data Integrity
- All materialized view fields derived from `analytics_events` table
- `user_uuid` must be valid UUID format (Keycloak UUID)
- `station_id` must follow PREFIX-nanoid(12) format (STX-xxxxx)
- `event_type` must be one of predefined enum values
- `schema_version` must be supported (>= 1)

### 8.2 Access Control
- admin-service only reads from materialized views
- No write operations to materialized views
- No write operations to metadata table
- Partner isolation enforced at query level

### 8.3 Query Safety
- All queries use parameterized queries only
- No dynamic SQL (reject CONCAT, +, || operators)
- No user input concatenated into SQL strings
- CI gate validates query safety on every commit

### 8.4 Caching
- Cache keys follow naming convention: `{{entity_type}}_{{entity_id}}`
- TTL enforced at cache layer
- Synchronous invalidation on every event ingestion
- Circuit breaker prevents cascading failures

---

## 9. Query Patterns

### 9.1 Station Analytics Query
```sql
SELECT
    station_id,
    station_views,
    favorite_count,
    avg_session_gap_seconds,
    last_viewed_at,
    first_viewed_at
FROM station_usage
WHERE station_id = $1
  AND station_id LIKE $2  -- Partner isolation filter
```

### 9.2 Summary Analytics Query
```sql
SELECT
    SUM(station_views) as station_views,
    COUNT(DISTINCT station_id) as total_stations,
    SUM(favorite_count) as favorite_count,
    COUNT(DISTINCT user_uuid) as active_users
FROM station_usage
WHERE station_id LIKE $1  -- Partner isolation filter
```

### 9.3 Search Trends Query
```sql
SELECT query_text, search_count
FROM search_trends
WHERE query_text LIKE $1
ORDER BY search_count DESC
LIMIT $2
```

---

## 10. Migration Plan

### 10.1 Create Materialized Views
```sql
-- Run in analytics_db
CREATE MATERIALIZED VIEW station_usage AS [definition above];
CREATE MATERIALIZED VIEW user_activity AS [definition above];
CREATE MATERIALIZED VIEW search_trends AS [definition above];
CREATE TABLE materialized_view_meta (...) [definition above];

-- Refresh views initially
REFRESH MATERIALIZED VIEW station_usage;
REFRESH MATERIALIZED VIEW user_activity;
REFRESH MATERIALIZED VIEW search_trends;

-- Create indexes
CREATE INDEX idx_station_usage_station_id ON station_usage(station_id);
CREATE INDEX idx_user_activity_user_uuid ON user_activity(user_uuid);
CREATE INDEX idx_search_trends_query_text ON search_trends(query_text);
```

### 10.2 Grant Permissions
```sql
CREATE ROLE bornemap_analytics_reader WITH NOINHERIT;
GRANT CONNECT ON DATABASE analytics_db TO bornemap_analytics_reader;
GRANT USAGE ON SCHEMA public TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bornemap_analytics_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bornemap_analytics_reader;

REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM bornemap_analytics_reader;
```

### 10.3 Verify Read-Only Enforcement
```bash
# CI gate should pass: no write operations in admin-service
cargo check
cargo sqlx prepare --check
```

---

## 11. Performance Considerations

### 11.1 Query Performance
- Materialized views eliminate expensive aggregations at query time
- Incremental refresh minimizes impact on write performance
- Indexed materialized views ensure fast lookups

### 11.2 Refresh Performance
- Incremental refresh (O(1) affected rows) vs full refresh (O(n) rows)
- Scheduled refresh during low-traffic periods
- Parallel refresh of independent views

### 11.3 Cache Performance
- Cache hit rate target: 80%+
- Low latency: <500ms for cached queries
- Efficient invalidation: <5 seconds from event ingestion

---

## 12. Compliance with Requirements

| Requirement | Data Model Support |
|-------------|-------------------|
| FR-ANALYTICS-001: Analytics read API | AnalyticsResponse, AnalyticsQuery |
| FR-ANALYTICS-002: Read-only enforcement | Database role + CI gate |
| FR-ANALYTICS-003: Materialized views | station_usage, user_activity, search_trends |
| FR-ANALYTICS-004: KPI aggregation | SummaryAnalytics, KPI models |
| FR-ANALYTICS-005: Station intelligence | StationAnalytics |
| FR-ANALYTICS-006: Caching | CacheEntry, CacheInvalidationRequest |
| FR-ANALYTICS-007: Cache invalidation | CacheInvalidationRequest + callback |
| FR-ANALYTICS-008: Domain types contracts | DTOs in domain-types crate |

---

**Status**: Data model complete, ready for contract generation.
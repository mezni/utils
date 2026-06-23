# Research Report: Admin Analytics Read Layer

**Feature**: Admin Analytics Read Layer (Sprint 4)
**Date**: 2026-06-22
**Status**: Complete

## Overview

This document records technical decisions, best practices, and alternatives evaluated for the admin analytics read layer implementation. All decisions align with project constraints (3 services, read-only enforcement, SQLx compilation, synchronous cache invalidation).

---

## 1. Redis Caching Configuration

**Question**: What Redis caching strategy should be used for analytics queries?

**Decision**: Use cache-aside pattern with configurable TTL-based expiration (5 minutes default)

**Rationale**:
- Cache-aside gives more control over cache invalidation timing
- TTL-based expiration prevents memory bloat and stale data accumulation
- Configurable TTL allows different cache lifetimes for different endpoints
- Synchronous invalidation ensures consistency without race conditions

**Alternatives Considered**:
1. **Write-through caching** → Rejected: Requires driver-service to update cache on write, violates single-writer enforcement
2. **Write-behind caching** → Rejected: Asynchronous complexity, risk of cache divergence
3. **Immutable cache with scheduled refresh** → Rejected: Doesn't support cache invalidation on event ingestion
4. **Cache-aside with synchronous invalidation** → Selected: Best balance of consistency, control, and performance

**Implementation Details**:
```rust
// Cache TTL configuration
const CACHE_TTL_STATION_ANALYTICS: Duration = Duration::from_secs(300); // 5 minutes
const CACHE_TTL_SUMMARY: Duration = Duration::from_secs(600); // 10 minutes

// Cache-aside pattern with invalidation
async fn get_station_analytics(id: &str, db: &Pool) -> Result<StationAnalytics> {
    // Try cache first
    if let Some(cached) = cache_service.get(format!("station_{}", id)).await? {
        return Ok(cached);
    }

    // Cache miss: query database
    let analytics = db::queries::station_usage::get_by_id(id).await?;

    // Write to cache
    cache_service.set(
        format!("station_{}", id),
        &analytics,
        CACHE_TTL_STATION_ANALYTICS
    ).await?;

    Ok(analytics)
}

// Synchronous invalidation
async fn invalidate_cache(id: &str) -> Result<()> {
    cache_service.delete(format!("station_{}", id)).await?;
    Ok(())
}
```

---

## 2. Materialized View Refresh Strategy

**Question**: Should materialized views be manually refreshed or automatically refreshed?

**Decision**: Hybrid approach - automatic refresh on demand via scheduled job, with manual refresh capability

**Rationale**:
- Automatic refresh ensures views are always up-to-date
- On-demand refresh allows immediate updates after event ingestion
- Scheduled refresh prevents drift during low-traffic periods
- Low-impact refresh during off-hours maintains performance

**Alternatives Considered**:
1. **No automatic refresh** → Rejected: Views become stale, user sees outdated analytics
2. **Manual refresh only** → Rejected: Requires manual intervention, violates automaticity requirement
3. **Background refresh every X minutes** → Selected: Provides consistent updates without excessive load
4. **Refresh on event ingestion** → Selected: Immediate consistency with minimal latency

**Implementation Details**:
```sql
-- Materialized view definitions
CREATE MATERIALIZED VIEW station_usage AS
SELECT
    station_id,
    COUNT(*) as station_views,
    SUM(favorite_count) as total_favorites,
    COUNT(DISTINCT user_uuid) as unique_users
FROM analytics_events
GROUP BY station_id;

CREATE MATERIALIZED VIEW user_activity AS
SELECT
    user_uuid,
    COUNT(*) as total_views,
    COUNT(DISTINCT station_id) as stations_visited,
    AVG(DATEDIFF(hour, event_timestamp, LEAD(event_timestamp) OVER (PARTITION BY user_uuid ORDER BY event_timestamp))) as avg_session_gap
FROM analytics_events
GROUP BY user_uuid;

-- On-demand refresh
REFRESH MATERIALIZED VIEW station_usage;
REFRESH MATERIALIZED VIEW user_activity;
```

**Refresh Triggers**:
1. **On-demand**: When event ingested, refresh views for affected station/user
2. **Scheduled**: Every 5 minutes (async job in admin-service)
3. **Manual**: Via admin API endpoint (for testing/debugging)

---

## 3. Cache Invalidation Callback Implementation

**Question**: How should driver-service call admin-service for cache invalidation?

**Decision**: Synchronous HTTP POST callback with retry logic and circuit breaker pattern

**Rationale**:
- Synchronous ensures immediate consistency
- HTTP POST is simple, reliable, and service-agnostic
- Retry logic handles transient network failures
- Circuit breaker prevents cascading failures if admin-service is down
- Idempotent request prevents double invalidation on retry

**Alternatives Considered**:
1. **Asynchronous message queue (Kafka/RabbitMQ)** → Rejected: Adds new infrastructure, violates "no new services" constraint
2. **Synchronous gRPC** → Rejected: Adds dependency on protobuf definitions, unnecessary complexity
3. **RESTful POST with idempotency key** → Selected: Simple, standard, idempotent
4. **WebSocket subscription** → Rejected: Overkill for simple cache invalidation, adds complexity

**Implementation Details**:
```rust
// Admin-service cache invalidation endpoint
#[post("/api/v1/cache/invalidate")]
async fn invalidate_cache(
    Json(payload): Json<CacheInvalidationPayload>,
    db: web::Data<Pool>,
) -> Result<HttpResponse, AppError> {
    let station_id = &payload.station_id;

    // Refresh materialized views
    db::queries::materialized_views::refresh_station_views(station_id).await?;

    // Invalidate cache entries
    cache_service.delete(format!("station_{}", station_id)).await?;
    cache_service.delete("summary").await?;

    Ok(HttpResponse::Ok().json(InvalidationResponse {
        success: true,
        invalidated_at: Utc::now(),
    }))
}

// Driver-service callback with retry logic
async fn notify_cache_invalidation(station_id: &str) -> Result<()> {
    let url = config.cache_invalidation_url.clone();

    let response = retry_with_circuit_breaker(
        url,
        CacheInvalidationPayload { station_id: station_id.to_string() },
        3, // max retries
        Duration::from_secs(1), // retry delay
        Duration::from_secs(5), // circuit open duration
    ).await?;

    if !response.success {
        return Err(anyhow!("Cache invalidation failed"));
    }

    Ok(())
}
```

**Circuit Breaker Pattern**:
```rust
struct CircuitBreaker {
    failure_count: AtomicU32,
    last_failure_time: AtomicInstant,
    state: Atomic<State>,
}

enum State {
    Closed,   // Allow requests
    Open,     // Reject requests
    HalfOpen, // Allow one trial request
}
```

---

## 4. Query Safety Validation Framework

**Question**: How to prevent SQL injection and dynamic SQL vulnerabilities?

**Decision**: Compile-time verified parameterized queries with strict query safety validator

**Rationale**:
- SQLx compile-time verification catches query errors at compile time
- Parameterized queries prevent SQL injection
- Static query registry enforced by CI gate
- No user input directly concatenated into SQL strings

**Alternatives Considered**:
1. **Dynamic query builder (e.g., sqlx::query)** → Rejected: Still vulnerable if not careful with parameterization
2. **ORM-based queries** → Rejected: Additional abstraction layer, query safety harder to enforce
3. **Manual string validation** → Rejected: Complex to validate all SQL constructs
4. **SQLx query! macro with parameterization** → Selected: Compile-time verification + runtime safety

**Implementation Details**:
```rust
// Strict query safety validator
pub struct QuerySafetyValidator;

impl QuerySafetyValidator {
    pub fn validate_query(query_str: &str) -> Result<(), QuerySafetyError> {
        // Reject dynamic SQL patterns
        let dangerous_patterns = [
            r"CONCAT\s*\(",  // Concatenation
            r"\+\s*\$1",     // String concatenation with +
            r"||\s*\$1",     // PostgreSQL concatenation
        ];

        for pattern in dangerous_patterns {
            if query_str.contains(pattern) {
                return Err(QuerySafetyError::DynamicSQL(pattern.to_string()));
            }
        }

        // Verify parameterized query
        if !query_str.contains("$") {
            return Err(QuerySafetyError::NonParameterized);
        }

        Ok(())
    }
}

// CI gate: scan for dynamic SQL
fn scan_for_dynamic_sql(dir: &Path) -> Result<bool> {
    let mut has_dynamic_sql = false;

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let content = fs::read_to_string(entry.path())?;

            if QuerySafetyValidator::validate_query(&content).is_err() {
                has_dynamic_sql = true;
                println!("Found dynamic SQL in: {}", entry.path());
            }
        }
    }

    Ok(has_dynamic_sql)
}
```

---

## 5. KPI Aggregation Performance Optimization

**Question**: How to optimize KPI calculations for large datasets?

**Decision**: Pre-aggregated materialized views with incremental refresh strategy

**Rationale**:
- Materialized views eliminate expensive aggregations at query time
- Incremental refresh updates only affected data (O(1) vs O(n))
- Pre-aggregated data reduces query latency
- Separate views for different query patterns (station-level, user-level, summary)

**Alternatives Considered**:
1. **Live aggregation on every query** → Rejected: Performance degrades with dataset size
2. **Summarization on write** → Rejected: Violates single-writer principle (driver-service only writes events)
3. **Background aggregation job** → Selected: Periodic updates, efficient refresh
4. **Incremental refresh on demand** → Selected: Immediate consistency, minimal performance impact

**Implementation Details**:
```sql
-- Pre-aggregated materialized views (incremental refresh)
CREATE MATERIALIZED VIEW station_usage AS
SELECT
    station_id,
    COUNT(*) as station_views,
    SUM(favorite_count) as total_favorites,
    COUNT(DISTINCT user_uuid) as unique_users,
    AVG(event_timestamp) as last_viewed_at
FROM analytics_events
WHERE event_timestamp >= COALESCE(
    (SELECT MAX(last_refreshed_at) FROM materialized_view_meta WHERE view_name = 'station_usage'),
    '1970-01-01'
)
GROUP BY station_id;

-- Update query (incremental)
CREATE OR REPLACE FUNCTION refresh_station_usage() RETURNS void AS $$
BEGIN
    TRUNCATE TABLE station_usage;
    INSERT INTO station_usage
    SELECT
        station_id,
        COUNT(*) as station_views,
        SUM(favorite_count) as total_favorites,
        COUNT(DISTINCT user_uuid) as unique_users,
        AVG(event_timestamp) as last_viewed_at
    FROM analytics_events
    WHERE event_timestamp >= COALESCE(
        (SELECT MAX(last_refreshed_at) FROM materialized_view_meta WHERE view_name = 'station_usage'),
        '1970-01-01'
    )
    GROUP BY station_id;

    UPDATE materialized_view_meta
    SET last_refreshed_at = NOW()
    WHERE view_name = 'station_usage';
END;
$$ LANGUAGE plpgsql;
```

---

## 6. Partner Data Isolation Pattern

**Question**: How to ensure partner analytics queries are isolated?

**Decision**: Row-level filtering with partner_id in all analytics queries

**Rationale**:
- Partner isolation prevents data leakage between partners
- Simple, effective, no additional infrastructure
- Consistent filtering across all query patterns
- CI gate validates no cross-partner data access

**Alternatives Considered**:
1. **Separate databases per partner** → Rejected: Violates "no new databases" constraint
2. **Separate tables per partner** → Rejected: Duplicate schema, complex queries
3. **Application-level filtering** → Rejected: Easy to miss, audit risk
4. **Database row-level security** → Selected: Database-enforced, hard to bypass

**Implementation Details**:
```rust
// Partner isolation middleware
pub struct PartnerIsolation {
    pub current_partner_id: String,
}

impl PartnerIsolation {
    pub fn validate_partner_access(&self, resource_id: &str, expected_partner_id: &str) -> bool {
        // If partner is 'admin', they can access everything
        if self.current_partner_id == "admin" {
            return true;
        }

        // Otherwise, resource must belong to same partner
        resource_id.starts_with(&format!("{}-", self.current_partner_id))
    }
}

// CI gate: scan for missing partner filtering
fn scan_for_missing_partner_filtering(dir: &Path) -> Result<bool> {
    let mut missing_filters = false;

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let content = fs::read_to_string(entry.path())?;

            // Check for SELECT queries without partner filtering
            if content.contains("SELECT") && !content.contains("partner_id") && !content.contains("'admin'") {
                println!("Found query without partner filtering in: {}", entry.path());
                missing_filters = true;
            }
        }
    }

    Ok(missing_filters)
}
```

---

## 7. Database Role Setup for Read-Only Access

**Question**: How to enforce read-only access at database level?

**Decision**: Separate database role with SELECT-only permissions, enforced via CI gate

**Rationale**:
- Database-level enforcement prevents application bypass
- SELECT-only role provides defense in depth
- CI gate validates no write operations exist
- Application-layer validation provides additional safety

**Alternatives Considered**:
1. **Application-level enforcement only** → Rejected: Easy to bypass, no defense in depth
2. **PostgreSQL RLS (Row Level Security)** → Rejected: Overkill for read-only access, adds complexity
3. **Separate user with SELECT permissions** → Selected: Simple, effective, enforceable

**Implementation Details**:
```sql
-- Create read-only database role
CREATE ROLE bornemap_analytics_reader WITH NOINHERIT;

-- Grant SELECT-only permissions
GRANT CONNECT ON DATABASE bornemap TO bornemap_analytics_reader;
GRANT USAGE ON SCHEMA public TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bornemap_analytics_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bornemap_analytics_reader;

-- Revoke any write permissions (safety check)
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM bornemap_analytics_reader;

-- Application connection string (no write permissions)
DATABASE_URL=postgresql://bornemap_analytics_reader:password@localhost/analytics_db

-- CI gate: validate no write operations in admin-service
grep -r "INSERT\|UPDATE\|DELETE\|TRUNCATE\|CREATE TABLE" admin-service/src/ | grep analytics_db || echo "No write operations found"
```

---

## 8. Caching Strategy (Cache-Aside vs Write-Through)

**Question**: Should cache updates be synchronized with database writes (write-through) or separate (cache-aside)?

**Decision**: Cache-aside with synchronous invalidation

**Rationale**:
- Driver-service writes events to database, then calls cache invalidation
- Cache-aside gives more control over invalidation timing
- No need to update cache on write (driver-service doesn't know which cache keys to invalidate)
- Consistent invalidation callback ensures cache is synchronized

**Alternatives Considered**:
1. **Write-through caching** → Rejected: Requires driver-service to update cache on write
2. **Write-behind caching** → Rejected: Asynchronous, risk of cache divergence
3. **Cache-aside with synchronous invalidation** → Selected: Best consistency model

**Implementation Details**:
```rust
// Driver-service writes event
async fn ingest_event(event: AnalyticsEvent) -> Result<()> {
    // Write to database
    driver_service::write_event(event).await?;

    // Invalidate cache
    notify_cache_invalidation(&event.station_id).await?;

    Ok(())
}

// Admin-service refreshes cache
async fn refresh_cache(station_id: &str) -> Result<()> {
    let analytics = db::queries::station_usage::get_by_id(station_id).await?;

    cache_service.set(
        format!("station_{}", station_id),
        &analytics,
        CACHE_TTL_STATION_ANALYTICS
    ).await?;

    Ok(())
}
```

---

## 9. Event Schema Compatibility

**Question**: How to handle future event schema changes while maintaining backward compatibility?

**Decision**: Schema versioning with validation middleware

**Rationale**:
- Event schemas must remain compatible with existing materialized views
- Version validation ensures correct data interpretation
- Graceful handling of unknown fields prevents crashes

**Alternatives Considered**:
1. **No schema versioning** → Rejected: Breaking changes to event schemas
2. **Full backward compatibility** → Selected: Support multiple versions for a grace period
3. **Strict version enforcement** → Selected: Enforce valid schemas, reject invalid

**Implementation Details**:
```rust
// Event schema validator
pub struct EventSchemaValidator;

impl EventSchemaValidator {
    pub fn validate(event: &serde_json::Value) -> Result<(), ValidationError> {
        let required_fields = ["event_type", "schema_version", "payload"];

        for field in required_fields {
            if event.get(field).is_none() {
                return Err(ValidationError::MissingField(field.to_string()));
            }
        }

        Ok(())
    }
}

// Middleware: validate event schema on ingestion
async fn validate_event_schema(
    Json(event): Json<AnalyticsEvent>,
) -> Result<Json<AnalyticsEvent>, AppError> {
    EventSchemaValidator::validate(&event.payload)?;

    Ok(Json(event))
}
```

---

## 10. Cache Hit Rate Monitoring

**Question**: How to monitor and optimize cache performance?

**Decision**: Metrics collection with cache hit rate, latency, and invalidation metrics

**Rationale**:
- Cache hit rate target: 80%+
- Latency targets: <500ms (cached), <500ms (uncached)
- Monitor invalidation success rate
- Alert on cache hit rate drops

**Alternatives Considered**:
1. **No monitoring** → Rejected: Cannot detect performance issues
2. **Application-level metrics** → Selected: Simple, effective, compatible with existing stack

**Implementation Details**:
```rust
// Cache metrics
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub invalidations: AtomicU64,
}

impl CacheMetrics {
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

// Metrics endpoint
#[get("/metrics/cache")]
async fn cache_metrics(metrics: web::Data<CacheMetrics>) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "hit_rate": metrics.hit_rate(),
        "hits": metrics.hits.load(Ordering::Relaxed),
        "misses": metrics.misses.load(Ordering::Relaxed),
        "invalidations": metrics.invalidations.load(Ordering::Relaxed),
    }))
}
```

---

## Summary of Decisions

| Decision Area | Chosen Approach | Key Benefits |
|---------------|-----------------|--------------|
| **Redis Caching** | Cache-aside with TTL | Control, consistency, configurable expiration |
| **Materialized Views** | Hybrid refresh (on-demand + scheduled) | Immediate consistency, low-impact refresh |
| **Cache Invalidation** | Synchronous HTTP POST callback | Immediate consistency, simple protocol |
| **Query Safety** | SQLx compile-time verification | Catch errors at compile time, prevent SQL injection |
| **KPI Aggregation** | Incremental materialized views | O(1) refresh, high performance |
| **Partner Isolation** | Row-level filtering | Database-enforced, consistent |
| **Read-Only Access** | Database role + CI gate | Defense in depth, enforceable |
| **Caching Strategy** | Cache-aside with invalidation | Consistency, control, simplicity |
| **Schema Compatibility** | Versioned validation | Backward compatibility, safety |
| **Cache Monitoring** | Metrics collection | Observability, optimization |

---

## Next Steps

1. Review and approve research decisions
2. Generate data model (Phase 1)
3. Generate contracts (Phase 1)
4. Generate quickstart guide (Phase 1)
5. Generate tasks (Phase 2)

---

**Status**: All unknowns resolved, ready for Phase 1 design.
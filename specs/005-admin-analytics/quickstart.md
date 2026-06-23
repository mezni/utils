# Quickstart Guide: Admin Analytics Read Layer

**Feature**: Admin Analytics Read Layer (Sprint 4)
**Date**: 2026-06-22
**Purpose**: Guide for testing and verifying the admin analytics read layer implementation

---

## Overview

This quickstart guide provides practical steps for testing the admin analytics read layer, verifying cache invalidation, and validating read-only enforcement. It assumes Sprint 3 (Telemetry Ingestion Core) is complete and events are being ingested.

---

## Prerequisites

### Services Running
1. **Keycloak** (port 8080) - Authentication provider
2. **PostgreSQL** (port 5432) - analytics_db database
3. **Redis** (port 6379) - Cache storage
4. **driver-service** (port 3001) - Telemetry ingestion
5. **admin-service** (port 3002) - Analytics read layer

### Dependencies Installed
- Rust 1.75+ with actix-web 4.4
- PostgreSQL 16+
- Redis 7.0+
- Keycloak 24+

---

## Setup

### 1. Start Keycloak

```bash
cd keycloak
./start-keycloak.sh

# Create realm and client
# Navigate to http://localhost:8080
# Create admin user (admin/admin123)
# Create client "bornemap" with OIDC protocol
```

### 2. Start PostgreSQL with Materialized Views

```bash
# Ensure analytics_db exists
createdb analytics_db

# Run migrations
psql -d analytics_db -f migrations/analytics_db/000001_create_materialized_views.sql

# Grant read-only permissions
psql -d analytics_db << EOF
CREATE ROLE bornemap_analytics_reader WITH NOINHERIT;
GRANT CONNECT ON DATABASE analytics_db TO bornemap_analytics_reader;
GRANT USAGE ON SCHEMA public TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bornemap_analytics_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bornemap_analytics_reader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM bornemap_analytics_reader;
EOF
```

### 3. Start Redis

```bash
redis-server
```

### 4. Start Services

```bash
# Start driver-service (telemetry ingestion)
cd driver-service
cargo run

# Start admin-service (analytics read layer)
cd admin-service
cargo run

# Verify services are running
curl http://localhost:3001/health
curl http://localhost:3002/health
```

---

## Testing Scenarios

### Scenario 1: Verify Read-Only Enforcement

**Goal**: Ensure admin-service cannot write to analytics_db

**Steps**:
1. Create Keycloak admin user
2. Get JWT token
3. Attempt POST request to analytics endpoint
4. Verify 403 Forbidden is returned

**Command**:
```bash
# Create admin user in Keycloak
# Get JWT token
TOKEN=$(curl -X POST 'http://localhost:8080/realms/bornemap/protocol/openid-connect/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=password&username=admin&password=admin123&client_id=bornemap')

# Attempt write operation (should fail)
curl -X POST 'http://localhost:3002/api/v1/analytics/stations/test' \
  -H 'Authorization: Bearer '$TOKEN \
  -H 'Content-Type: application/json' \
  -d '{"station_id": "STA-test"}'

# Expected response: 403 Forbidden
# {
#   "error": "forbidden",
#   "message": "Read-only enforcement: cannot modify analytics data"
# }
```

**Expected Result**:
- Response code: 403 Forbidden
- Response body contains error: "Read-only enforcement: cannot modify analytics data"

**Validation**:
```bash
# Verify no write operations exist in admin-service
grep -r "INSERT\|UPDATE\|DELETE\|TRUNCATE" admin-service/src/ | grep analytics_db
# Should return nothing (no write operations found)
```

---

### Scenario 2: Retrieve Station Analytics

**Goal**: Retrieve station-specific analytics data

**Steps**:
1. Ingest test telemetry events (if not already done)
2. Query station analytics endpoint
3. Verify response includes expected KPIs

**Command**:
```bash
# Ingest test events via driver-service
curl -X POST 'http://localhost:3001/api/v1/telemetry/events' \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "VIEW",
    "schema_version": 1,
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "idempotency_key": "test-event-001",
    "payload": {
      "station_id": "STA-test123",
      "favorite_count": 0,
      "event_timestamp": "2026-06-22T15:30:00Z"
    },
    "timestamp": "2026-06-22T15:30:00Z"
  }'

# Retrieve station analytics
curl -X GET 'http://localhost:3002/api/v1/analytics/stations/STA-test123' \
  -H 'Authorization: Bearer '$TOKEN

# Expected response:
# {
#   "data": {
#     "station_id": "STA-test123",
#     "station_views": 1,
#     "favorites": 0,
#     "search_hits": 0,
#     "avg_session_time_seconds": 0.0,
#     "unique_users": 1,
#     "last_viewed_at": "2026-06-22T15:30:00Z",
#     "first_viewed_at": "2026-06-22T15:30:00Z",
#     "partner_id": null
#   },
#   "metadata": {
#     "request_id": "req_xxx",
#     "query_duration_ms": 23,
#     "timestamp": "2026-06-22T15:30:00Z",
#     "cached": true,
#     "cache_hit_rate": 0.75
#   },
#   "cache_status": {
#     "status": "hit",
#     "latency_ms": 8,
#     "ttl_remaining_seconds": 292
#   }
# }
```

**Expected Result**:
- Response code: 200 OK
- Data contains: station_id, station_views, favorites, search_hits, avg_session_time_seconds, unique_users, last_viewed_at, first_viewed_at, partner_id
- Cache status: "hit" (first query), "miss" (subsequent queries)

**Validation**:
```bash
# Verify station_views equals number of events ingested
# Verify favorites equals sum of favorite_count in events
# Verify unique_users equals distinct user_uuid count
```

---

### Scenario 3: Verify Cache Invalidation

**Goal**: Ensure cache is invalidated when events are ingested

**Steps**:
1. Query station analytics (should be cached)
2. Ingest new event for same station
3. Query station analytics again (should be uncached or refreshed)
4. Verify cache status indicates cache miss

**Command**:
```bash
# Query station analytics (first time - cache miss)
curl -X GET 'http://localhost:3002/api/v1/analytics/stations/STA-test123' \
  -H 'Authorization: Bearer '$TOKEN
# Expected: cache_status.status = "miss"

# Ingest new event
curl -X POST 'http://localhost:3001/api/v1/telemetry/events' \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "FAVORITE",
    "schema_version": 1,
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "idempotency_key": "test-event-002",
    "payload": {
      "station_id": "STA-test123",
      "favorite_count": 1,
      "event_timestamp": "2026-06-22T15:30:05Z"
    },
    "timestamp": "2026-06-22T15:30:05Z"
  }'

# Query station analytics again (should be cached after invalidation)
curl -X GET 'http://localhost:3002/api/v1/analytics/stations/STA-test123' \
  -H 'Authorization: Bearer '$TOKEN
# Expected: cache_status.status = "hit"

# Verify favorites count increased
# Verify station_views count increased
```

**Expected Result**:
- After first query: cache_status.status = "miss"
- After event ingestion: cache invalidation occurs
- After second query: cache_status.status = "hit"

**Validation**:
```bash
# Verify cache invalidation endpoint received request
curl -X POST 'http://localhost:3002/api/v1/cache/invalidate' \
  -H 'Content-Type: application/json' \
  -d '{
    "station_id": "STA-test123",
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "FAVORITE",
    "timestamp": "2026-06-22T15:30:05Z"
  }'

# Expected response:
# {
#   "success": true,
#   "invalidated_at": "2026-06-22T15:30:06Z",
#   "cached_keys": ["station_STA-test123", "user_550e8400-e29b-41d4-a716-446655440000", "summary"],
#   "views_refreshed": 1,
#   "duration_ms": 45
# }
```

---

### Scenario 4: Retrieve Platform Summary

**Goal**: Retrieve platform-wide KPI aggregation

**Steps**:
1. Ingest multiple events (VIEW, FAVORITE, SEARCH)
2. Query summary endpoint
3. Verify all KPIs are present and accurate

**Command**:
```bash
# Ingest test events
for i in {1..100}; do
  curl -X POST 'http://localhost:3001/api/v1/telemetry/events' \
    -H 'Content-Type: application/json' \
    -d '{
      "event_type": "VIEW",
      "schema_version": 1,
      "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
      "idempotency_key": "test-view-event-0'$i'",
      "payload": {
        "station_id": "STA-test123",
        "favorite_count": 0,
        "event_timestamp": "2026-06-22T15:30:0'$i'Z"
      },
      "timestamp": "2026-06-22T15:30:0'$i'Z"
    }' & done

curl -X POST 'http://localhost:3001/api/v1/telemetry/events' \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "FAVORITE",
    "schema_version": 1,
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "idempotency_key": "test-favorite-event-001",
    "payload": {
      "station_id": "STA-test123",
      "favorite_count": 5,
      "event_timestamp": "2026-06-22T15:30:10Z"
    },
    "timestamp": "2026-06-22T15:30:10Z"
  }'

curl -X POST 'http://localhost:3001/api/v1/telemetry/events' \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "SEARCH",
    "schema_version": 1,
    "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
    "idempotency_key": "test-search-event-001",
    "payload": {
      "query_text": "fast charging near me",
      "station_id": "STA-test123",
      "event_timestamp": "2026-06-22T15:30:12Z"
    },
    "timestamp": "2026-06-22T15:30:12Z"
  }'

# Retrieve platform summary
curl -X GET 'http://localhost:3002/api/v1/analytics/summary' \
  -H 'Authorization: Bearer '$TOKEN

# Expected response:
# {
#   "data": {
#     "station_views": 102,
#     "search_volume": 1,
#     "favorite_count": 5,
#     "active_users": 1,
#     "total_stations": 1,
#     "total_users": 1,
#     "total_searches": 1,
#     "trends": ["fast charging near me"]
#   },
#   "metadata": {
#     "request_id": "req_xxx",
#     "query_duration_ms": 45,
#     "timestamp": "2026-06-22T15:30:00Z",
#     "cached": true,
#     "cache_hit_rate": 0.75
#   },
#   "cache_status": {
#     "status": "hit",
#     "latency_ms": 8,
#     "ttl_remaining_seconds": 292
#   }
# }
```

**Expected Result**:
- Response code: 200 OK
- Data contains: station_views, search_volume, favorite_count, active_users, total_stations, total_users, total_searches, trends
- station_views equals total VIEW events
- favorites equals total FAVORITE events
- search_volume equals total SEARCH events

---

### Scenario 5: Verify Materialized Views

**Goal**: Ensure materialized views are updated correctly

**Steps**:
1. Query materialized view directly
2. Verify results match expected values

**Command**:
```bash
# Query station_usage materialized view directly
psql -d analytics_db -c "SELECT * FROM station_usage WHERE station_id = 'STA-test123';"

# Expected output:
#  station_id | station_views | total_favorites | unique_users | favorite_count | avg_session_gap_seconds | last_viewed_at | first_viewed_at
# ------------+---------------+-----------------+--------------+----------------+-------------------------+----------------+-----------------
#  STA-test123 |          102 |              5 |            1 |              5 |              0.00       | 2026-06-22 15:30 | 2026-06-22 15:30
```

**Expected Result**:
- station_views: 102 (number of VIEW events)
- total_favorites: 5 (sum of favorite_count)
- unique_users: 1 (distinct user_uuid)
- favorite_count: 5 (number of FAVORITE events)

---

### Scenario 6: Test Cache Health Check

**Goal**: Retrieve cache health metrics

**Steps**:
1. Query cache health endpoint
2. Verify metrics are returned correctly

**Command**:
```bash
# Query cache health
curl -X GET 'http://localhost:3002/api/v1/analytics/cache/health' \
  -H 'Authorization: Bearer '$TOKEN

# Expected response:
# {
#   "data": {
#     "hit_rate": 0.75,
#     "hits": 75000,
#     "misses": 25000,
#     "invalidations": 4500,
#     "total_requests": 100000,
#     "cache_size_mb": 128.5,
#     "avg_latency_ms": {
#       "hit": 8.5,
#       "miss": 42.3
#     }
#   },
#   "metadata": {
#     "request_id": "req_xxx",
#     "query_duration_ms": 12,
#     "timestamp": "2026-06-22T15:30:00Z",
#     "cached": true,
#     "cache_hit_rate": 0.95
#   },
#   "cache_status": {
#     "status": "hit",
#     "latency_ms": 12,
#     "ttl_remaining_seconds": 299
#   }
# }
```

**Expected Result**:
- Response code: 200 OK
- Data contains: hit_rate, hits, misses, invalidations, total_requests, cache_size_mb, avg_latency_ms
- hit_rate > 0.75 (target: 80%+)

---

## Performance Testing

### Query Latency Test

**Goal**: Verify query latency meets targets (< 500ms cached, < 500ms uncached)

**Command**:
```bash
# Measure query latency for cached queries
for i in {1..10}; do
  start=$(date +%s%N)
  curl -X GET 'http://localhost:3002/api/v1/analytics/stations/STA-test123' \
    -H 'Authorization: Bearer '$TOKEN > /dev/null
  end=$(date +%s%N)
  duration=$(( ($end - $start) / 1000000 ))
  echo "Query $i: $duration ms"
done
```

**Expected Result**:
- Average cached query latency: < 500ms
- Average uncached query latency: < 500ms

### Cache Hit Rate Test

**Goal**: Verify cache hit rate meets target (80%+)

**Command**:
```bash
# Clear cache
redis-cli FLUSHALL

# Query same station 20 times (first 5 will be cache misses, rest cache hits)
for i in {1..20}; do
  curl -X GET 'http://localhost:3002/api/v1/analytics/stations/STA-test123' \
    -H 'Authorization: Bearer '$TOKEN > /dev/null
done

# Query cache health
curl -X GET 'http://localhost:3002/api/v1/analytics/cache/health' \
  -H 'Authorization: Bearer '$TOKEN | jq '.data.hit_rate'
```

**Expected Result**:
- Cache hit rate: > 0.80 (80%+)

---

## CI Gate Validation

### 1. Read-Only Enforcement Gate

**Command**:
```bash
# Scan for write operations in admin-service
./scripts/check-read-only-enforcement.sh

# Expected output:
# ✓ No write operations found in admin-service
# ✓ No dynamic SQL in analytics queries
# ✓ All endpoints are GET methods only
```

### 2. Query Safety Gate

**Command**:
```bash
# Scan for dynamic SQL
./scripts/check-query-safety.sh

# Expected output:
# ✓ No CONCAT operators found
# ✓ No string concatenation with +
# ✓ No PostgreSQL || operators
# ✓ All queries use parameterized queries
```

### 3. KPI Integrity Gate

**Command**:
```bash
# Scan for external data sources in KPI calculations
./scripts/check-kpi-integrity.sh

# Expected output:
# ✓ All KPIs derived from analytics_events table
# ✓ No hardcoded values found
# ✓ No external API calls in KPI calculations
```

---

## Troubleshooting

### Issue: Cache Not Invalidating

**Symptoms**: Analytics data not updating after event ingestion

**Steps**:
1. Verify driver-service is calling cache invalidation endpoint
2. Check admin-service logs for invalidation requests
3. Verify Redis cache is being cleared
4. Check materialized view refresh status

**Debug Commands**:
```bash
# Check admin-service logs for invalidation requests
grep -i "invalidation" admin-service/logs/

# Check Redis cache status
redis-cli KEYS "station_*"

# Check materialized view last refresh
psql -d analytics_db -c "SELECT * FROM materialized_view_meta;"
```

### Issue: Read-Only Enforcement Bypass

**Symptoms**: admin-service successfully writes to analytics_db

**Steps**:
1. Verify database role permissions
2. Check application code for write operations
3. Verify CI gate is catching violations

**Debug Commands**:
```bash
# Check database role permissions
psql -d analytics_db -c "\du bornemap_analytics_reader"

# Scan for write operations
grep -r "INSERT\|UPDATE\|DELETE" admin-service/src/ | grep analytics_db

# Run CI gate check
./scripts/check-read-only-enforcement.sh
```

### Issue: Query Latency Too High

**Symptoms**: Analytics queries taking > 500ms

**Steps**:
1. Verify materialized views are properly indexed
2. Check cache hit rate
3. Verify query performance in database
4. Check for slow queries in PostgreSQL logs

**Debug Commands**:
```bash
# Check PostgreSQL query performance
psql -d analytics_db -c "EXPLAIN ANALYZE SELECT * FROM station_usage WHERE station_id = 'STA-test123';"

# Check PostgreSQL slow query log
tail -f /var/log/postgresql/postgresql-*.log | grep "duration:"
```

---

## Next Steps

1. Complete integration tests
2. Run E2E tests with Playwright
3. Deploy to staging environment
4. Monitor performance metrics
5. Verify cache hit rate and latency targets

---

**Status**: Quickstart guide complete. Follow scenarios in order for end-to-end testing.
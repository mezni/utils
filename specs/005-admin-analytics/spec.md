# Feature Specification: Analytics Read Layer (Admin Visibility)

**Feature Branch**: `005-admin-analytics`

**Created**: 2026-06-22

**Status**: Draft

**Input**: Build a controlled analytics consumption system where admin-service can read analytics data, driver-service remains the ONLY writer, and aggregated insights are introduced without violating data ownership rules.

## Clarifications

### Session 2026-06-22

- Q: How should cache invalidation be triggered when telemetry events are ingested by driver-service? → A: Synchronous callback from driver-service to admin-service immediately after event ingestion

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Admin Team Views Analytics Dashboard

**Goal**: Admin team can access comprehensive analytics dashboards showing KPIs, station usage, user activity, and search trends without compromising single-writer enforcement.

**Why this priority**: Essential for monitoring system health, making business decisions, and understanding platform usage patterns.

**Independent Test**: Access admin-service GET /api/v1/analytics endpoint, verify response contains expected metrics (station_views, search_volume, active_users) and that no write operations are possible.

**Acceptance Scenarios**:

1. **Given** admin team accesses dashboard via admin-service GET /api/v1/analytics, **When** valid authentication and authorization are provided, **Then** response includes station_views, search_volume, active_users, and other KPIs derived from telemetry events

2. **Given** admin team queries for station analytics, **When** GET /api/v1/analytics/stations/:id is called, **Then** response includes station_views, favorites, search_hits, avg_session_time, and other station-specific metrics

3. **Given** admin team accesses aggregated data, **When** GET /api/v1/analytics/views is called, **Then** response includes materialized views (station usage, user activity, search trends) with correct aggregations

4. **Given** cached analytics data exists, **When** GET /api/v1/analytics/views is called, **Then** response uses cache with cache hit status indicator

5. **Given** admin service receives event ingestion notification, **When** driver-service immediately calls cache invalidation endpoint after event persistence, **Then** cache is invalidated and subsequent queries reflect updated data

6. **Given** admin team attempts write operation on analytics data, **When** write operation is attempted, **Then** request is rejected with access denied (read-only enforcement)

### User Story 2 - System Validates Read-Only Enforcement

**Goal**: System enforces strict read-only enforcement for analytics data, ensuring driver-service remains the ONLY writer and admin-service has no mutation authority.

**Why this priority**: Critical for maintaining data integrity and preventing accidental or malicious write attempts.

**Independent Test**: Attempt to write to analytics database via admin-service, verify write is rejected with 403 Forbidden and CI gate validates enforcement.

**Acceptance Scenarios**:

1. **Given** any service other than driver-service attempts write to analytics_db, **When** CI read-only analytics gate runs, **Then** gate fails and prevents write operations

2. **Given** admin-service receives data ingestion request, **When** request targets analytics_db tables, **Then** database layer rejects write with 403 Forbidden

3. **Given** materialized views are created or updated, **When** validation occurs, **Then** enforcement gate verifies only driver-service can modify views

4. **Given** cache system is updated, **When** cache invalidation is triggered, **Then** system validates write originates from driver-service event ingestion flow

### User Story 3 - System Safely Aggregates Analytics Data

**Goal**: System aggregates analytics data into materialized views and KPIs derived from telemetry events, ensuring query safety and data integrity.

**Why this priority**: Enables efficient analytics queries without impacting write performance.

**Independent Test**: Run analytics query, verify results are derived from materialized views and not from raw events, validate query safety.

**Acceptance Scenarios**:

1. **Given** analytics query is executed, **When** query targets materialized views, **Then** results are derived from pre-aggregated data (station usage, user activity, search trends)

2. **Given** query safety validation runs, **When** dynamic SQL is detected, **Then** query safety gate fails and blocks execution

3. **Given** KPI calculations occur, **When** KPI integrity gate runs, **Then** system verifies all KPIs are derived from telemetry events and not external data sources

4. **Given** admin team queries for aggregated metrics, **When** GET /api/v1/analytics/summary is called, **Then** response includes station_views, search_volume, favorite_count, active_users with accurate aggregations

### User Story 4 - Admin Team Views Partner Analytics

**Goal**: Admin team can view partner-specific analytics (partner-level aggregation views) to understand platform usage across different partner organizations.

**Why this priority**: Provides insights into partner performance and helps with partner management and optimization.

**Independent Test**: Query partner-specific analytics, verify results include partner-level aggregations and no data leakage between partners.

**Acceptance Scenarios**:

1. **Given** admin team accesses partner analytics, **When** GET /api/v1/analytics/partners/:id is called, **Then** response includes partner-specific metrics and partner-level aggregation views

2. **Given** partner analytics data exists, **When** query executes, **Then** results are isolated per partner with no cross-partner data leakage

## Requirements *(mandatory)*

### Functional Requirements

- **FR-ANALYTICS-001**: admin-service must provide analytics read API for GET events, stats, and station/:id queries with proper authentication and authorization

- **FR-ANALYTICS-002**: admin-service must enforce READ ONLY enforcement for all analytics operations, rejecting any write attempts

- **FR-ANALYTICS-003**: admin-service must create materialized analytics views (station usage, user activity, search trends) derived from telemetry events

- **FR-ANALYTICS-004**: admin-service must implement KPI aggregation engine for station_views, search_volume, favorite_count, active_users metrics

- **FR-ANALYTICS-005**: admin-service must provide station intelligence API returning views, favorites, search_hits, avg_session_time for specific stations

- **FR-ANALYTICS-006**: admin-service must implement caching system for aggregated analytics queries with configurable expiration

- **FR-ANALYTICS-007**: system must provide cache invalidation triggered by synchronous callback from driver-service to admin-service immediately after each event ingestion, ensuring cache consistency

- **FR-ANALYTICS-008**: admin-service must define analytics domain-types contracts with response DTOs for type-safe API responses

### Key Entities *(include if feature involves data)*

- **AnalyticsEvent**: Core telemetry event from analytics database (already defined in Sprint 3)

- **KPI**: Aggregated metrics derived from events (station_views, search_volume, favorite_count, active_users)

- **StationUsage**: Materialized view of station-level usage statistics

- **UserActivity**: Materialized view of user-level activity patterns

- **SearchTrends**: Materialized view of search query patterns and trends

- **StationIntelligence**: Response DTO for station-specific analytics (views, favorites, search_hits, avg_session_time)

- **AnalyticsQuery**: Query request parameters with filtering and pagination

- **AnalyticsResponse**: Response DTO with KPIs and metadata

- **CacheEntry**: Cache entry with expiration time

- **CacheInvalidation**: Event indicating cache needs to be invalidated

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Admin team can retrieve station analytics (station_views, favorites, search_hits, avg_session_time) in under 500ms

- **SC-002**: Read-only enforcement prevents all write operations to analytics database from admin-service with 100% failure rate for unauthorized writes

- **SC-003**: All analytics queries derive from materialized views (station usage, user activity, search trends) ensuring query safety

- **SC-004**: Cached analytics queries respond within 100ms for cache hits and < 500ms for cache misses

- **SC-005**: Cache invalidation triggered by driver-service event ingestion successfully updates cache within 5 seconds

- **SC-006**: System enforces KPI integrity with 100% of KPIs derived from telemetry events and not external sources

- **SC-007**: Partner analytics queries are isolated per partner with no data leakage (0% cross-partner data access)

- **SC-008**: All CI gates for read-only analytics enforcement pass with 0% false positives

## Assumptions

- Telemetry ingestion pipeline from Sprint 3 is live and producing events in analytics database

- Driver-service maintains single-writer enforcement for analytics database (already implemented)

- Materialized views are sufficient for query performance without impacting write latency

- Caching system is available and configured for analytics queries

- Admin team has proper authentication and authorization for analytics access (from Sprint 1 keycloak integration)

- Cache invalidation occurs synchronously (within 5 seconds) to ensure data consistency
- Communication between driver-service and admin-service occurs synchronously for cache invalidation

- KPI calculations are straightforward aggregations (count, average, sum) without complex transformations

- Partner-level analytics are optional and not required for initial implementation

- Frontend dashboards will be built separately (this feature focuses on backend read layer only)

## Out of Scope (Explicitly Excluded)

1. Frontend dashboard UI implementation
2. Partner analytics dashboard UI (optional, separate feature)
3. Real-time analytics streaming (events are ingested and queries are synchronous)
4. Advanced data visualization (charts, graphs - will be handled by frontend)
5. Event sampling for large volumes (analytics_db is sized appropriately)
6. Multi-region analytics queries (will use single region for MVP)
7. Event replay mechanism (not needed for read-only analytics layer)
8. Dashboard UI (separate frontend feature)

## Dependencies

- **Internal Dependencies**:
  - Sprint 3: Telemetry Ingestion Core must be complete (driver-service ingestion pipeline live)
  - Sprint 1: Keycloak authentication and authorization system must be complete
  - Sprint 2: GIS engine must be complete (for station geolocation data)
  - Domain-types crate: Event schemas must be available for analytics contracts

- **External Dependencies**:
  - Database system with materialized views support (query optimization layer)
  - Caching system for aggregated analytics queries
  - Database with reliable read-only role enforcement
  - Communication infrastructure for cache invalidation callbacks

## Risks and Mitigations

### Risk R-ANALYTICS-1: Read-Only Enforcement Bypass
**Risk**: Admin-service could bypass read-only enforcement through direct SQL queries or API bypass.
**Impact**: Critical - write access to analytics_db would violate single-writer principle.
**Mitigation**:
  - Enforce database-level read-only role (bornemap_analytics_reader)
  - CI gate validates all write attempts are blocked
  - Audit logging for all analytics queries
  - Application-layer validation for all API endpoints

### Risk R-ANALYTICS-2: Materialized View Performance
**Risk**: Materialized views could be slow to refresh or impact write performance.
**Impact**: Medium - delayed or inaccurate analytics, write latency increases.
**Mitigation**:
  - Schedule refreshes during low-traffic periods
  - Ensure refresh mechanism is efficient and optimized
  - Monitor refresh times and adjust accordingly
  - Implement fallback strategies for failed refreshes

### Risk R-ANALYTICS-3: Cache Consistency Issues
**Risk**: Cache invalidation could miss events or not trigger in time, leading to stale data.
**Impact**: High - users see outdated analytics, incorrect business decisions.
**Mitigation**:
  - Implement cache invalidation on every event ingestion
  - Use reliable communication mechanism for cache invalidation
  - Set appropriate cache TTL to balance freshness and performance
  - Monitor cache hit/miss rates and alert on issues

### Risk R-ANALYTICS-4: Query Safety Violations
**Risk**: Dynamic SQL construction could expose SQL injection vulnerabilities.
**Impact**: Critical - security breach, data corruption.
**Mitigation**:
  - CI query safety gate rejects any dynamic SQL in analytics queries
  - Use parameterized queries only
  - Validate all user inputs for queries
  - Regular security audits

### Risk R-ANALYTICS-5: Partner Data Isolation
**Risk**: Partner analytics queries could leak data between partners.
**Impact**: Medium - privacy violation, business risk.
**Mitigation**:
  - Filter all queries by partner_id
  - Verify no cross-partner data access in CI gates
  - Implement row-level security in database (if needed)
  - Regular audits for data isolation

## Test Strategy

### Unit Tests
- KPI aggregation logic (count, average, sum calculations)
- Materialized view query building
- Cache operations (get, set, invalidate)
- Read-only enforcement at application level
- Query safety validation

### Integration Tests
- Admin-service analytics read API endpoints
- Cache invalidation flow with callback from driver-service
- Materialized view refresh triggers
- CI gate validations
- Read-only enforcement with database layer

### E2E Tests
- End-to-end analytics query flow from frontend to database
- Cache hit/miss scenarios
- Cache invalidation after event ingestion
- Read-only enforcement with unauthorized write attempts

### Performance Tests
- Query latency (cached vs uncached)
- Cache hit rate targets (80%+)
- Materialized view refresh time (< 1 minute)
- Query throughput (1000+ queries/second)

# Tasks: Admin Analytics Read Layer (Sprint 4)

**Input**: Design documents from `/specs/005-admin-analytics/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Read-only enforcement, query safety, KPI integrity, cache invalidation, performance benchmarks, E2E tests

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- `admin-service/` - Admin service source code
- `migrations/analytics_db/` - Database migrations
- `tests/integration/` - Integration tests
- `tests/e2e/` - End-to-end tests

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, environment setup, and foundational configuration

- [X] T001 [P] Create admin-service module structure in admin-service/src/ with api/, services/, models/, db/, middleware/, validators/ directories
- [X] T002 [P] Add project dependencies to admin-service/Cargo.toml (sqlx, redis, serde, serde_json, chrono, thiserror)
- [X] T003 [P] Configure database connection pool in admin-service/src/db/analytics_db.rs with proper timeout and max connections
- [X] T004 [P] Configure Redis connection in admin-service/src/services/cache_service.rs with connection pooling and health checks
- [X] T005 [P] Create environment variables file .env.example in admin-service with all required configuration (DB_URL, REDIS_URL, CACHE_TTL, etc.)
- [X] T006 [P] Update Makefile in admin-service to include build, test, and sqlx-prepare targets
- [X] T007 [P] Create SQLx database file in admin-service/.sqlx/ directory with prepare command output

**Checkpoint**: Environment setup complete - all infrastructure configured and ready for development

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Implement CI gates and shared utilities that all user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T008 [P] Create read-only enforcement validator in admin-service/src/validators/read_only.rs that scans for write operations targeting analytics_db
- [X] T009 [P] Create query safety validator in admin-service/src/validators/query_safety.rs that rejects dynamic SQL (CONCAT, +, || operators)
- [X] T010 [P] Create KPI integrity validator in admin-service/src/validators/kpi_integrity.rs that verifies all KPIs derived from events only
- [X] T011 Create CI read-only gate script in .specify/ci-gates/020-read-only-enforcement.sh that scans admin-service for analytics_db write operations
- [X] T012 Create CI query safety gate script in .specify/ci-gates/021-query-safety.sh that scans for dynamic SQL in analytics queries
- [X] T013 Create CI KPI integrity gate script in .specify/ci-gates/022-kpi-integrity.sh that verifies KPI calculations
- [X] T014 Create database role creation migration in migrations/analytics_db/000001_create_materialized_views.sql with bornemap_analytics_reader role and permissions
- [X] T015 [P] Create cache service implementation in admin-service/src/services/cache_service.rs with get/set/delete operations
- [X] T016 [P] Create circuit breaker implementation in admin-service/src/services/circuit_breaker.rs with retry logic and state management
- [X] T017 [P] Create materialized view metadata table in migrations/analytics_db/000001_create_materialized_views.sql for tracking last refresh times
- [X] T018 [P] Create KPI aggregation engine service in admin-service/src/services/analytics_query_service.rs with station_views, search_volume, favorite_count, active_users calculations
- [X] T019 Implement KPI aggregation tests in admin-service/src/services/analytics_query_service.rs with test cases for each KPI calculation

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Admin Team Views Analytics Dashboard

**Goal**: Admin team can access comprehensive analytics dashboards showing KPIs, station usage, user activity, and search trends

**Independent Test**: Access admin-service GET /api/v1/analytics endpoint, verify response contains expected metrics and that no write operations are possible

### Implementation for User Story 1

- [ ] T020 [P] [US1] Define analytics response DTOs in domain-types/src/analytics/analytics_response.rs (AnalyticsResponse, AnalyticsMetadata, CacheStatus)
- [ ] T021 [P] [US1] Define station analytics DTOs in domain-types/src/analytics/station_analytics.rs (StationAnalytics fields and conversion from database rows)
- [ ] T022 [P] [US1] Define summary analytics DTOs in domain-types/src/analytics/summary_analytics.rs (SummaryAnalytics fields and KPI aggregation)
- [ ] T023 [P] [US1] Define analytics query DTOs in domain-types/src/analytics/analytics_query.rs (AnalyticsQuery with validation methods)
- [X] T024 [P] [US1] Create analytics API routes in admin-service/src/api/mod.rs that expose GET /api/v1/analytics endpoints
- [X] T025 [P] [US1] Implement GET /api/v1/analytics/summary endpoint in admin-service/src/api/analytics.rs that calls KPI aggregation engine and returns SummaryAnalytics
- [X] T026 [P] [US1] Implement GET /api/v1/analytics/stations/:id endpoint in admin-service/src/api/analytics.rs that queries station_usage materialized view
- [X] T027 [P] [US1] Implement GET /api/v1/analytics/users/:uuid endpoint in admin-service/src/api/analytics.rs that queries user_activity materialized view
- [X] T028 [P] [US1] Implement GET /api/v1/analytics/search-trends endpoint in admin-service/src/api/analytics.rs that queries search_trends materialized view
- [X] T029 [P] [US1] Create materialized view queries in admin-service/src/db/queries/materialized_views.rs with parameterized SELECT queries
- [X] T032 [P] [US1] Implement cache service integration in analytics API endpoints to check cache before querying database
- [X] T033 [P] [US1] Implement cache hit/miss status in responses using CacheStatus DTO with TTL remaining
 - [ ] T034 [P] [US1] Implement Keycloak authentication middleware in admin-service/src/middleware/auth.rs with JWT validation
 - [ ] T035 [P] [US1] Implement Keycloak role-based authorization in admin-service/src/middleware/auth.rs to enforce admin/manager roles for analytics endpoints
 - [X] T036 [P] [US1] Implement partner isolation filter in admin-service/src/api/analytics.rs to filter results by partner_id when manager role is active
 - [X] T037 Create integration tests for analytics API endpoints in admin-service/tests/integration/analytics_api_tests.rs with mock Keycloak authentication
 - [X] T038 Create performance benchmarks for analytics API endpoints in admin-service/tests/performance/analytics_benchmarks.rs with load testing

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - System Validates Read-Only Enforcement

**Goal**: System enforces strict read-only enforcement for analytics data, ensuring driver-service remains the ONLY writer and admin-service has no mutation authority

**Independent Test**: Attempt to write to analytics database via admin-service, verify write is rejected with 403 Forbidden and CI gate validates enforcement

### Implementation for User Story 2

- [ ] T039 [P] [US2] Implement read-only enforcement middleware in admin-service/src/middleware/read_only.rs that validates all endpoints are GET methods
- [ ] T040 [P] [US2] Implement database-level read-only validation in admin-service/src/db/analytics_db.rs that uses bornemap_analytics_reader role
- [ ] T041 [P] [US2] Implement query safety validation in admin-service/src/validators/query_safety.rs that rejects dynamic SQL in analytics queries
- [ ] T042 [P] [US2] Create SQLx prepare command target in admin-service/Cargo.toml for compile-time query verification
- [ ] T043 [P] [US2] Create audit logging middleware in admin-service/src/middleware/audit_log.rs to log all analytics queries
- [ ] T044 [P] [US2] Create endpoint validation in admin-service/src/api/analytics.rs that checks for POST/PUT/DELETE requests and returns 403 Forbidden
- [ ] T045 [P] [US2] Create database migration validation in migrations/analytics_db/000001_create_materialized_views.sql that verifies read-only permissions
- [ ] T046 [P] [US2] Implement application-layer validation in admin-service/src/api/analytics.rs that validates all KPI calculations
- [ ] T047 Create integration tests for read-only enforcement in admin-service/tests/integration/read_only_tests.rs attempting write operations and verifying 403 responses
- [ ] T048 Create CI gate validation tests in tests/ci-gates/020-read-only-enforcement.sh that automatically scans for write operations

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - System Safely Aggregates Analytics Data

**Goal**: System aggregates analytics data into materialized views and KPIs derived from telemetry events, ensuring query safety and data integrity

**Independent Test**: Run analytics query, verify results are derived from materialized views and not from raw events, validate query safety

### Implementation for User Story 3

- [ ] T049 [P] [US3] Create station_usage materialized view definition in migrations/analytics_db/000001_create_materialized_views.sql with station_views, favorites, unique_users
- [ ] T050 [P] [US3] Create user_activity materialized view definition in migrations/analytics_db/000001_create_materialized_views.sql with total_views, stations_visited, favorites_count
- [ ] T051 [P] [US3] Create search_trends materialized view definition in migrations/analytics_db/000001_create_materialized_views.sql with query_text, search_count, unique_searchers
- [ ] T052 [P] [US3] Create index for station_usage.station_id in migrations/analytics_db/000001_create_materialized_views.sql for fast lookups
- [ ] T053 [P] [US3] Create index for user_activity.user_uuid in migrations/analytics_db/000001_create_materialized_views.sql for fast lookups
- [ ] T054 [P] [US3] Create index for search_trends.query_text in migrations/analytics_db/000001_create_materialized_views.sql for LIKE pattern matching
- [ ] T055 [P] [US3] Implement materialized view refresh function in migrations/analytics_db/000001_create_materialized_views.sql for incremental updates
- [ ] T056 [P] [US3] Create materialized view queries module in admin-service/src/db/queries/materialized_views.rs with refresh methods
- [ ] T057 [P] [US3] Implement KPI integrity validation in admin-service/src/validators/kpi_integrity.rs that verifies all KPIs derived from telemetry events only
- [ ] T058 [P] [US3] Create SQLx macros in admin-service/src/db/queries/station_usage.rs for compile-time verified queries
- [ ] T059 [P] [US3] Create SQLx macros in admin-service/src/db/queries/user_activity.rs for compile-time verified queries
- [ ] T060 [P] [US3] Create SQLx macros in admin-service/src/db/queries/search_trends.rs for compile-time verified queries
- [ ] T061 [P] [US3] Implement query safety validation in admin-service/src/validators/query_safety.rs that rejects dynamic SQL patterns
- [ ] T062 [P] [US3] Create query safety unit tests in admin-service/src/validators/query_safety.rs testing all SQL validation patterns
- [ ] T063 Create integration tests for materialized views in admin-service/tests/integration/materialized_views_tests.rs verifying views are updated correctly
- [ ] T064 Create integration tests for KPI integrity in admin-service/tests/integration/kpi_integrity_tests.rs verifying KPIs are derived from events only

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - Admin Team Views Partner Analytics

**Goal**: Admin team can view partner-specific analytics (partner-level aggregation views) to understand platform usage across different partner organizations

**Independent Test**: Query partner-specific analytics, verify results include partner-level aggregations and no data leakage between partners

### Implementation for User Story 4

- [ ] T065 [P] [US4] Implement partner isolation middleware in admin-service/src/middleware/partner_isolation.rs that filters by partner_id
- [ ] T066 [P] [US4] Implement partner-level aggregation queries in admin-service/src/db/queries/partner_analytics.rs grouping by partner_id
- [ ] T067 [P] [US4] Implement GET /api/v1/analytics/partners/:id endpoint in admin-service/src/api/analytics.rs returning partner-specific metrics
- [ ] T068 [P] [US4] Create partner analytics DTOs in domain-types/src/analytics/partner_analytics.rs with partner-specific KPIs
- [ ] T069 [P] [US4] Implement partner validation in admin-service/src/validators/partner_isolation.rs checking PREFIX-nanoid(12) format
- [ ] T070 [P] [US4] Create partner data isolation tests in admin-service/tests/integration/partner_isolation_tests.rs verifying no cross-partner data access
- [ ] T071 Create partner analytics integration tests in admin-service/tests/integration/partner_analytics_tests.rs with mock partner data

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Phase 7: User Story 5 - Cache Invalidation System

**Goal**: Cache invalidation is triggered by synchronous callback from driver-service to admin-service immediately after event ingestion

**Independent Test**: Trigger cache invalidation via driver-service event ingestion and verify cache is invalidated and subsequent queries reflect updated data

### Implementation for User Story 5

- [ ] T072 [P] [US5] Define cache invalidation DTOs in domain-types/src/analytics/cache_invalidation.rs (CacheInvalidationRequest, CacheInvalidationResponse)
- [ ] T073 [P] [US5] Create cache invalidation endpoint in admin-service/src/api/analytics.rs at POST /api/v1/cache/invalidate
- [ ] T074 [P] [US5] Implement cache invalidation service in admin-service/src/services/cache_invalidation.rs that refreshes materialized views
- [ ] T075 [P] [US5] Implement cache entry deletion in admin-service/src/services/cache_invalidation.rs using cache_service.delete
- [ ] T076 [P] [US5] Implement station refresh logic in admin-service/src/services/cache_invalidation.rs for station_usage and summary views
- [ ] T077 [P] [US5] Implement search trends refresh logic in admin-service/src/services/cache_invalidation.rs for search_trends view
- [ ] T078 [P] [US5] Implement user activity refresh logic in admin-service/src/services/cache_invalidation.rs for user_activity view
- [ ] T079 [P] [US5] Implement circuit breaker integration in admin-service/src/services/cache_invalidation.rs for retry logic
- [ ] T080 [P] [US5] Implement dead letter queue in admin-service/src/services/cache_invalidation.rs for failed invalidation requests
- [ ] T081 [P] [US5] Create cache invalidation endpoint tests in admin-service/tests/integration/cache_invalidation_tests.rs with mock requests
- [ ] T082 [P] [US5] Create circuit breaker tests in admin-service/tests/integration/circuit_breaker_tests.rs with failure scenarios

**Checkpoint**: At this point, User Story 5 should be fully functional and testable independently

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, monitoring, and documentation

- [ ] T083 [P] Implement metrics collection in admin-service/src/api/analytics.rs for analytics_requests_total, analytics_request_duration_seconds, cache_hits_total, cache_misses_total
- [ ] T084 [P] Create metrics endpoint in admin-service/src/api/analytics.rs at GET /api/v1/analytics/metrics for cache health monitoring
- [ ] T085 [P] Implement error handling middleware in admin-service/src/middleware/error_handler.rs for consistent error responses
- [ ] T086 [P] Create documentation in admin-service/README.md with API endpoints, authentication, and caching strategy
- [ ] T087 [P] Create cache health check endpoint in admin-service/src/api/analytics.rs at GET /api/v1/analytics/cache/health
- [ ] T088 [P] Implement log aggregation in admin-service/src/main.rs with structured logging for analytics queries and cache operations
- [ ] T089 [P] Create environment validation in admin-service/src/main.rs checking required environment variables
- [ ] T090 Create E2E tests in admin-service/tests/e2e/analytics_dashboard_e2e_test.rs with Playwright testing full user journey
- [ ] T091 Create end-to-end cache invalidation tests in admin-service/tests/e2e/cache_invalidation_e2e_test.rs with event ingestion flow
- [ ] T092 Run performance benchmarks in admin-service/tests/performance/analytics_benchmarks.rs with 1000 concurrent queries
- [ ] T093 Verify cache hit rate in performance benchmarks targeting 80%+ with <500ms latency for cached queries
- [ ] T094 Verify cache invalidation performance targeting <5 seconds from event ingestion to cache update
- [ ] T095 Create CI pipeline integration tests in .specify/ci-gates/020-read-only-enforcement.sh, 021-query-safety.sh, 022-kpi-integrity.sh
- [ ] T096 Update quickstart guide in specs/005-admin-analytics/quickstart.md with testing scenarios and troubleshooting

**Checkpoint**: All features complete, polished, and ready for deployment

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (Admin Dashboard)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (Read-Only Enforcement)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (Analytics Aggregation)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 4 (Partner Analytics)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 5 (Cache Invalidation)**: Can start after Foundational (Phase 2) - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story
- Stories are fully independent of each other

### Parallel Opportunities

- Phase 1: 6 parallelizable tasks (T001-T006)
- Phase 2: 13 parallelizable tasks (T008-T016, T019)
- Phase 3: 17 parallelizable tasks (T020-T036, T037-T038)
- Phase 4: 9 parallelizable tasks (T039-T048)
- Phase 5: 15 parallelizable tasks (T049-T064)
- Phase 6: 6 parallelizable tasks (T065-T071)
- Phase 7: 10 parallelizable tasks (T072-T082)
- Phase 8: 13 parallelizable tasks (T083-T096)
- **Total Parallelizable Tasks**: 109 out of 96 ({parallel_percent}%)

**Independent Test Criteria**:
- US1: GET /api/v1/analytics endpoint returns SummaryAnalytics with KPIs and cache status
- US2: POST /api/v1/analytics endpoints return 403 Forbidden and CI gate validates enforcement
- US3: Materialized views exist and queries return data derived from views only
- US4: GET /api/v1/analytics/partners/:id returns partner-specific metrics without cross-partner data
- US5: POST /api/v1/cache/invalidate refreshes cache and subsequent queries return fresh data

---

## Sprint 4 Task Summary

**Total Tasks**: 96

**Task Count per User Story**:
- User Story 1: 19 tasks (T020-T038)
- User Story 2: 10 tasks (T039-T048)
- User Story 3: 16 tasks (T049-T064)
- User Story 4: 7 tasks (T065-T071)
- User Story 5: 11 tasks (T072-T082)
- Setup: 7 tasks (T001-T007)
- Foundational: 12 tasks (T008-T019)
- Polish: 14 tasks (T083-T096)

**Parallel Opportunities**:
- Phase 1: 6 parallelizable tasks
- Phase 2: 13 parallelizable tasks
- Phase 3: 17 parallelizable tasks
- Phase 4: 9 parallelizable tasks
- Phase 5: 15 parallelizable tasks
- Phase 6: 6 parallelizable tasks
- Phase 7: 10 parallelizable tasks
- Phase 8: 13 parallelizable tasks
- **Total Parallelizable Tasks**: 109 out of 96 ({parallel_percent}%)
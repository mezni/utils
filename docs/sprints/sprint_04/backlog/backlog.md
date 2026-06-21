# Sprint 4 — Analytics Read Layer (Admin Visibility)

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 4 (telemetry pipeline live)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S5-001 | Implement admin-service analytics read API (GET events, stats, station/:id) | team | NOT_STARTED |
| S5-002 | Create analytics read isolation layer (READ ONLY enforced) | team | NOT_STARTED |
| S5-003 | Create materialized analytics views (mv_station_usage, mv_user_activity, mv_search_trends) | team | NOT_STARTED |
| S5-004 | Implement KPI aggregation engine (station_views, search_volume, favorite_count, active_users) | team | NOT_STARTED |
| S5-005 | Implement station intelligence API (views, favorites, search_hits, avg_session_time) | team | NOT_STARTED |
| S5-006 | Set up Redis cache for aggregated analytics queries (TTL-based) | team | NOT_STARTED |
| S5-007 | Implement cache invalidation triggered by driver-service event ingestion | team | NOT_STARTED |
| S5-008 | Create analytics domain-types contracts (response DTOs) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S5-009 | Create partner analytics dashboard UI | team | NOT_STARTED |
| S5-010 | Create CI read-only analytics gate | team | NOT_STARTED |
| S5-011 | Create CI query safety gate | team | NOT_STARTED |
| S5-012 | Create CI KPI integrity gate | team | NOT_STARTED |
| S5-013 | Create CI view ownership gate | team | NOT_STARTED |
| S5-014 | Create CI cache consistency gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S5-015 | Add usage heatmaps to dashboard | team | NOT_STARTED |
| S5-016 | Add time-based activity charts | team | NOT_STARTED |
| S5-017 | Add partner-level aggregation views | team | NOT_STARTED |

## CI Additions (Sprint 5)

| ID | Gate | Rule |
|----|------|------|
| CI-5.1 | Read-Only Analytics Gate | FAIL if admin-service attempts write to analytics_db |
| CI-5.2 | Query Safety Gate | FAIL if dynamic SQL detected in analytics queries |
| CI-5.3 | KPI Integrity Gate | FAIL if KPI derived from non-driver-service data |
| CI-5.4 | View Ownership Gate | FAIL if materialized view modified outside driver-service |
| CI-5.5 | Cache Consistency Gate | FAIL if Redis cache updated outside driver-service event flow |

## Exit Criteria

Sprint 5 is COMPLETE ONLY IF:
- [ ] admin-service can read analytics safely
- [ ] No write paths exist outside driver-service
- [ ] All KPIs derived from validated events
- [ ] Cached analytics operational
- [ ] Read-only enforcement passes
- [ ] Query safety validated
- [ ] KPI integrity enforced

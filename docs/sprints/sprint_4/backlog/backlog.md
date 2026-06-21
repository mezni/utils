# Sprint 6 — Driver Experience Layer (UX + Product Polish)

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 5 (analytics read layer)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S6-001 | Implement favorites system (POST/GET/DELETE /api/v1/driver/favorites) | team | NOT_STARTED |
| S6-002 | Store favorites in users.preferences JSONB | team | NOT_STARTED |
| S6-003 | Implement user preferences system (preferred charger type, map filters, last region) | team | NOT_STARTED |
| S6-004 | Implement offline cache layer (AsyncStorage for mobile, IndexedDB for web) | team | NOT_STARTED |
| S6-005 | Optimize map UX (clustering, smooth transitions, station preview cards) | team | NOT_STARTED |
| S6-006 | Implement station search endpoint (GET /api/v1/driver/search?q=) | team | NOT_STARTED |
| S6-007 | Implement fuzzy matching for station search | team | NOT_STARTED |
| S6-008 | Add skeleton loaders (<150ms rule) | team | NOT_STARTED |
| S6-009 | Add optimistic UI updates (favorites, search) | team | NOT_STARTED |
| S6-010 | Implement session continuity (remember last position, restore filters) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S6-011 | Implement performance optimization (reduce render latency, improve cache hit rate) | team | NOT_STARTED |
| S6-012 | Create CI preferences isolation gate | team | NOT_STARTED |
| S6-013 | Create CI offline storage gate | team | NOT_STARTED |
| S6-014 | Create CI search safety gate | team | NOT_STARTED |
| S6-015 | Create CI UI boundary gate | team | NOT_STARTED |
| S6-016 | Create CI performance regression gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S6-017 | Add distance-to-user indicator on station cards | team | NOT_STARTED |
| S6-018 | Add consistent card components to ui-kit | team | NOT_STARTED |
| S6-019 | Reduce re-render cycles on map | team | NOT_STARTED |

## CI Additions (Sprint 6)

| ID | Gate | Rule |
|----|------|------|
| CI-6.1 | Preferences Isolation Gate | FAIL if preferences stored outside users.preferences JSONB |
| CI-6.2 | Offline Storage Gate | FAIL if backend dependency required for offline functionality |
| CI-6.3 | Search Safety Gate | FAIL if non-SQLx search implementation or external search service used |
| CI-6.4 | UI Boundary Gate | FAIL if frontend contains business logic or ui-kit violated by direct overrides |
| CI-6.5 | Performance Regression Gate | FAIL if API response time increases beyond baseline or map rendering exceeds latency budget |

## Exit Criteria

Sprint 6 is COMPLETE ONLY IF:
- [ ] Map is fast, responsive, stable
- [ ] Favorites system fully operational
- [ ] Preferences stored only in existing schema (no schema expansion)
- [ ] Caching optimized and stable
- [ ] No regression in spatial queries
- [ ] UI boundary rules pass
- [ ] Search safety enforced
- [ ] Offline isolation validated

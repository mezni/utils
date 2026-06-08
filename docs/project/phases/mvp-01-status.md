# MVP-1 Status

Project phase status and done criteria for Core Product Loop.

## Overview

**Goal**: A partner creates stations and chargers through the dashboard. A driver finds nearby stations on a map. The full loop works end to end with real data.

**Status**: Not started

**Started**: TBD

**Target completion**: TBD

## MVP-1 Done Criteria

- [ ] All 16 endpoints return correct data against real database
- [ ] Nearby endpoint returns stations ordered by distance
- [ ] Smoke tests all pass
- [ ] Dashboard — partner, station, charger CRUD fully working
- [ ] Dashboard — filter dropdowns populated from real API
- [ ] Dashboard — all form validations working
- [ ] Driver Web — map shows real markers with correct colors
- [ ] Driver Web — station detail shows real charger data
- [ ] Driver Web — works on Chrome, Firefox, Safari
- [ ] Driver Mobile — map shows real markers on iOS simulator
- [ ] Driver Mobile — map shows real markers on Android emulator
- [ ] Driver Mobile — location denied handled gracefully
- [ ] Full loop tested: create in dashboard → visible in driver apps
- [ ] All apps handle API unreachable gracefully
- [ ] No N+1 queries in any endpoint
- [ ] Onboarding guide tested from scratch on clean machine
- [ ] API documentation complete and accurate
- [ ] Zero Class A bugs open

## Sprint Status

### Sprint 1.1 — Backend and Database

**Status**: Not started

**Target**: 2 weeks

**Owner**: TBD

**Tasks**:
- [ ] Project setup (FastAPI, SQLAlchemy, Alembic, Postgres)
- [ ] Create schemas and tables
- [ ] Implement 16 CRUD endpoints
- [ ] Write Alembic migrations
- [ ] Seed database
- [ ] Write smoke tests
- [ ] Verify all endpoints working

**Exit criteria**:
- `GET /api/health` returns `{"status":"ok","service":"bornemap-service","db":"ok"}`
- Nearby endpoint returns stations ordered by distance
- All 16 endpoints return correct HTTP status codes
- All smoke tests pass

---

### Sprint 1.2 — Dashboard App

**Status**: Not started

**Target**: 2 weeks

**Owner**: TBD

**Tasks**:
- [ ] Project setup (Vite, React, TypeScript, Tailwind)
- [ ] Design token base config
- [ ] AppShell and navigation
- [ ] Overview, Partners, Stations, Chargers screens
- [ ] Form validation and error handling
- [ ] Cross-browser testing

**Exit criteria**:
- Partner CRUD fully working
- All forms validate before submit
- Filter dropdowns populated from real API
- No visual regressions on Chrome, Firefox, Safari

---

### Sprint 1.3 — Driver Web App

**Status**: Not started

**Target**: 2 weeks

**Owner**: TBD

**Tasks**:
- [ ] Project setup (Vite, React, Leaflet)
- [ ] Full-bleed map with OpenStreetMap tiles
- [ ] Fetch and render station markers
- [ ] Station Detail screen
- [ ] Floating UI components
- [ ] Cross-browser testing

**Exit criteria**:
- Map loads with all 15 seeded stations
- Marker colors reflect availability correctly
- Station Detail shows real charger data
- Works on Chrome, Firefox, Safari

---

### Sprint 1.4 — Driver Mobile App

**Status**: Not started

**Target**: 2 weeks

**Owner**: TBD

**Tasks**:
- [ ] Project setup (Expo SDK 54, react-native-maps)
- [ ] Full-bleed map with location permissions
- [ ] Fetch and render station markers
- [ ] Station Detail screen
- [ ] iOS simulator testing
- [ ] Android emulator testing

**Exit criteria**:
- Map loads with stations on iOS simulator
- Map loads with stations on Android emulator
- Location denied handled gracefully
- No crashes on network error

---

### Sprint 1.5 — Integration and Hardening

**Status**: Not started

**Target**: 1 week

**Owner**: TBD

**Tasks**:
- [ ] Full loop verification (Dashboard → Driver apps)
- [ ] Backend fix sweep (tests, performance, edge cases)
- [ ] Frontend fix sweep (validation, error handling, visual QA)
- [ ] Documentation (onboarding, API docs, status file)

**Exit criteria**:
- Full loop verified end-to-end
- All smoke tests pass
- No N+1 queries
- API documented
- Zero Class A bugs

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Database schema changes mid-sprint | Medium | High | Lock schema in Sprint 1.1, use migrations for changes |
| Map library learning curve | Low | Medium | Spike on Leaflet + react-leaflet early |
| Location permission UX on mobile | Low | Medium | Handle denied gracefully, no retry prompt |
| Performance regression on full loop | Medium | Medium | Run performance checks at end of each sprint |

---

## Notes

- All three apps must work on real data, not mocks.
- `/api` prefix is non-negotiable (constitution rule).
- Token-only styling (no hardcoded colors) from day one.
- Design token sync: when colors.ts changes, native.ts must change in same commit.

---

## Archive

*Completed sprints and resolved issues moved here at MVP-1 close.*

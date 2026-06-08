# Sprint Quick Reference

Fast lookup for what each sprint delivers and its exit criteria.

## MVP-1 Sprints

### 1.1 — Backend and Database (2 weeks)
**Owner**: TBD | **Status**: Not started

**Delivers**: FastAPI service + PostgreSQL with 16 CRUD endpoints

**Exit Criteria**:
```
✓ GET /api/health returns status OK with db connectivity
✓ All 16 CRUD endpoints working against seed data
✓ Nearby endpoint returns stations sorted by distance
✓ All smoke tests pass
```

**Key deliverables**:
- FastAPI project at `source/services/bornemap-service/`
- PostgreSQL `ev_platform` database with `inventory` and `gis` schemas
- Three tables: partner, station, charger (all UUID PKs)
- 16 endpoints: Health, Partners (5), Stations (7), Chargers (5)
- Alembic migrations (0001 schemas, 0002 tables, 0003 indexes)
- Seed data: 3 partners, 15 stations, 24 chargers

**Key file**: `docs/api/bornemap-service.md`

---

### 1.2 — Dashboard App (2 weeks)
**Owner**: TBD | **Status**: Not started

**Delivers**: React + Vite dashboard with full CRUD UI

**Exit Criteria**:
```
✓ Partner CRUD fully working (create, read, update, delete)
✓ Station CRUD with partner filter dropdown
✓ Charger CRUD with station filter dropdown
✓ All forms validate before submit
✓ No visual regressions on Chrome, Firefox, Safari
✓ API unreachable handled gracefully on all screens
```

**Key deliverables**:
- Vite + React project at `source/apps/dashboard/`
- Design token base config in `source/packages/ui/`
- Four screens: Overview, Partners, Stations, Chargers
- AppShell with fixed sidebar, top bar, form modals
- StatusBadge component (available/in_use/maintenance)

**Key file**: `docs/implementation-plan.md` Sprint 1.2

---

### 1.3 — Driver Web App (2 weeks)
**Owner**: TBD | **Status**: Not started

**Delivers**: React + Leaflet map with real station markers

**Exit Criteria**:
```
✓ Full-bleed map loads with all 15 seeded stations
✓ Marker colors: green if available, red if unavailable
✓ Marker popups show station info + available count
✓ Station Detail screen shows real charger data
✓ Clicking marker navigates to detail, back button returns to map
✓ Works on Chrome, Firefox, Safari
```

**Key deliverables**:
- Vite + React project at `source/apps/driver-web/`
- Leaflet + react-leaflet with OpenStreetMap tiles
- Station markers with color logic (brand.glow vs status.maintenance)
- Floating UI: top bar, zoom controls
- Station Detail screen with charger list

**Key file**: `docs/implementation-plan.md` Sprint 1.3

---

### 1.4 — Driver Mobile App (2 weeks)
**Owner**: TBD | **Status**: Not started

**Delivers**: React Native + Expo map on iOS and Android

**Exit Criteria**:
```
✓ Map loads on iOS simulator with real station markers
✓ Map loads on Android emulator with real station markers
✓ Location permission denied handled gracefully (no crash, use Tunisia center)
✓ Marker colors reflect availability
✓ Station Detail shows real charger data
✓ No crashes on network errors (shows error text instead)
```

**Key deliverables**:
- Expo SDK 54 project at `source/apps/driver-mobile/`
- react-native-maps with location permission request
- Station markers with color logic (pinColor green/red)
- Graceful location permission denial (use Tunisia center)
- Station Detail screen with charger list

**Exact versions locked**:
```
React Native:    0.76.5
React:           18.3.1
Expo Router:     ~4.0.0
expo-location:   ~18.0.0
react-native-maps: 1.18.0
```

**Key file**: `docs/implementation-plan.md` Sprint 1.4

---

### 1.5 — Integration and Hardening (1 week)
**Owner**: TBD | **Status**: Not started

**Delivers**: Verified end-to-end loop + documentation

**Exit Criteria**:
```
✓ Full loop verified: Dashboard create → Driver Web map → Driver Mobile map
✓ Changing charger status in Dashboard → marker colors change
✓ All smoke tests pass
✓ No N+1 queries in any endpoint
✓ All endpoints respond under 200ms
✓ Onboarding guide tested from scratch on clean machine
✓ API documentation complete
✓ Zero Class A bugs
```

**Key tasks**:
- Full loop testing (Dashboard → both driver apps)
- Backend fix sweep (tests, performance, edge cases)
- Frontend fix sweep (validation, error handling, visual QA)
- Write `docs/guides/onboarding.md`
- Write final API documentation
- Update `docs/project/phases/mvp-01-status.md`

**Key file**: `docs/implementation-plan.md` Sprint 1.5

---

## MVP-1 Done Criteria

**All 18 items must be checked before MVP-1 closes**:

```
✓ All 16 endpoints return correct data
✓ Nearby endpoint returns ordered by distance
✓ Smoke tests all pass
✓ Dashboard CRUD fully working
✓ Dashboard filter dropdowns from API
✓ Dashboard form validation working
✓ Driver Web map shows real markers with correct colors
✓ Driver Web station detail shows real charger data
✓ Driver Web works on Chrome, Firefox, Safari
✓ Driver Mobile map shows real markers on iOS simulator
✓ Driver Mobile map shows real markers on Android emulator
✓ Driver Mobile location denied handled gracefully
✓ Full loop tested: create in dashboard → visible in driver apps
✓ All apps handle API unreachable gracefully
✓ No N+1 queries in any endpoint
✓ Onboarding guide tested from scratch on clean machine
✓ API documentation complete and accurate
✓ Zero Class A bugs open
```

---

## Key Numbers

| What | Count |
|------|-------|
| Total Endpoints | 16 |
| Frontend Apps | 3 |
| Database Tables | 3 |
| Migrations | 3 |
| Seed Partners | 3 |
| Seed Stations | 15 |
| Seed Chargers | 24 |
| Sprints in MVP-1 | 5 |
| Total Weeks | 9 |
| Done Criteria | 18 |

---

## Document References

- **Full sprint details**: `docs/implementation-plan.md`
- **Phase status and tracking**: `docs/project/phases/mvp-01-status.md`
- **API contracts**: `docs/api/bornemap-service.md`
- **Database schema**: `docs/schema/inventory-schema.md`
- **Small decisions**: `docs/project/decisions.md`
- **Bug classification**: `docs/project/bugs.md`

---

**Updated**: Sprint 1.1 (starting phase)

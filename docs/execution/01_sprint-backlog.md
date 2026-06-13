# Sprint Backlog

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 PURPOSE

**List of tasks for current MVP only.**

This is the single source of truth for what needs to be built.

---

## 📋 TASKS (MVP-1)

### Backend (driver-service)

- [ ] Implement `/api/v1/stations` endpoint
  - [ ] Create station handler
  - [ ] Implement query logic
  - [ ] Add error handling
  - [ ] Write unit tests
  - **Est:** 4h

- [ ] Implement `/api/v1/stations/nearby` endpoint
  - [ ] Create nearby handler
  - [ ] Implement PostGIS distance query
  - [ ] Add radius validation
  - [ ] Add distance sorting
  - [ ] Write unit tests
  - **Est:** 6h

- [ ] Implement `/api/v1/stations/{id}` endpoint
  - [ ] Create detail handler
  - [ ] Implement charger query
  - [ ] Add error handling
  - [ ] Write unit tests
  - **Est:** 3h

- [ ] Create station data model
  - [ ] Define station struct
  - [ ] Define charger struct
  - [ ] Add required fields
  - [ ] Add validation
  - **Est:** 2h

- [ ] Set up database connection
  - [ ] Configure PostgreSQL connection
  - [ ] Add PostGIS extension
  - [ ] Set up connection pooling
  - [ ] Create test database
  - **Est:** 2h

---

### Frontend (mobile-driver)

- [ ] Implement MapContainer.native.ts
  - [ ] Create component structure
  - [ ] Integrate react-native-maps
  - [ ] Add user location tracking
  - [ ] Implement marker rendering
  - [ ] Write unit tests
  - **Est:** 8h

- [ ] Implement StationMarker.native.tsx
  - [ ] Create marker component
  - [ ] Add status indicators
  - [ ] Implement tap handling
  - [ ] Add haptic feedback
  - [ ] Write unit tests
  - **Est:** 4h

- [ ] Implement station detail bottom sheet
  - [ ] Create sheet component
  - [ ] Add station information
  - [ ] Add charger list
  - [ ] Implement close action
  - [ ] Write unit tests
  - **Est:** 6h

- [ ] Implement useStations hook
  - [ ] Create React Query hook
  - [ ] Add caching logic
  - [ ] Handle loading states
  - [ ] Write unit tests
  - **Est:** 4h

- [ ] Implement useNearbyStations hook
  - [ ] Create React Query hook
  - [ ] Add debounce logic
  - [ ] Handle map movement
  - [ ] Write unit tests
  - **Est:** 4h

---

### Frontend (web-driver)

- [ ] Implement MapContainer.web.ts
  - [ ] Create component structure
  - [ ] Integrate Leaflet
  - [ ] Add user location tracking
  - [ ] Implement marker rendering
  - [ ] Write unit tests
  - **Est:** 8h

- [ ] Implement StationMarker.web.tsx
  - [ ] Create marker component
  - [ ] Add status indicators
  - [ ] Implement click handling
  - [ ] Write unit tests
  - **Est:** 4h

- [ ] Implement station detail side panel
  - [ ] Create panel component
  - [ ] Add station information
  - [ ] Add charger list
  - [ ] Implement close action
  - [ ] Write unit tests
  - **Est:** 6h

- [ ] Implement React Query hooks
  - [ ] Same as mobile
  - [ ] Write unit tests
  - **Est:** 4h

---

### Shared Packages

- [ ] Create @bm/api-client package
  - [ ] Setup package structure
  - [ ] Implement getStations()
  - [ ] Implement getNearbyStations()
  - [ ] Implement getStationById()
  - [ ] Write unit tests
  - **Est:** 6h

- [ ] Create @bm/types package
  - [ ] Define Station interface
  - [ ] Define Charger interface
  - [ ] Define API response types
  - [ ] Write TypeScript tests
  - **Est:** 4h

- [ ] Create @bm/utils package
  - [ ] Implement distance calculations
  - [ ] Implement validation functions
  - [ ] Write unit tests
  - **Est:** 4h

- [ ] Create @bm/design-tokens package
  - [ ] Define color system
  - [ ] Define typography scale
  - [ ] Define spacing scale
  - [ ] Define radius system
  - [ ] Write unit tests
  - **Est:** 4h

---

### Integration & Testing

- [ ] Create integration tests for API
  - [ ] Test station endpoints
  - [ ] Test nearby search
  - [ ] Test error handling
  - [ ] Set up test database
  - **Est:** 6h

- [ ] Create E2E tests for map flows
  - [ ] Test app load
  - [ ] Test map movement
  - [ ] Test station selection
  - [ ] Test error scenarios
  - **Est:** 8h

- [ ] Create performance tests
  - [ ] Test map rendering speed
  - [ ] Test marker performance
  - [ ] Test API response times
  - [ ] Test memory usage
  - **Est:** 6h

---

## 📊 TASK SUMMARY

**Total Tasks:** 27

**By Component:**
- Backend: 5 tasks (18%)
- Mobile Frontend: 5 tasks (18%)
- Web Frontend: 4 tasks (15%)
- Shared Packages: 4 tasks (15%)
- Integration/Testing: 3 tasks (11%)

**By Type:**
- Implementation: 22 tasks (81%)
- Testing: 5 tasks (19%)

---

## 🎯 PROGRESS TRACKING

**Sprint Progress:**
- Total Tasks: 27
- Completed: 0
- In Progress: 0
- Pending: 27
- Progress: 0%

**Timeline:**
- Sprint Start: 2026-06-01
- Sprint End: 2026-06-20
- Days Remaining: 7

---

## 🚀 PRIORITY ORDER

### High Priority (Week 1)
1. API endpoints (backend)
2. MapContainer (both platforms)
3. API client setup

### Medium Priority (Week 2)
4. Station markers
5. Station detail view
6. React Query hooks

### Low Priority (Week 2-3)
7. Integration tests
8. E2E tests
9. Performance tests

---

## 🎯 SUCCESS CRITERIA

**Before MVP-1 Complete:**
- [ ] All API endpoints working
- [ ] Map loads on both platforms
- [ ] Stations render correctly
- [ ] Nearby search functional
- [ ] Station details viewable
- [ ] All tests passing
- [ ] No architecture violations

---

*This sprint backlog contains only MVP-1 tasks. No tasks outside current MVP are allowed.*
# Testing Strategy

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 TESTING OBJECTIVE

Ensure that the map discovery system works end-to-end:

- backend correctness (PostGIS queries)
- API contract stability
- frontend rendering correctness
- map interaction reliability
- UX responsiveness

---

## 🧠 TESTING LEVELS

### 1. Unit Tests
**Focus:** Utilities and query logic
**Scope:** Pure functions, logic, helper methods
**Goal:** Ensure correctness of individual components

### 2. Integration Tests
**Focus:** API ↔ Database communication
**Scope:** End-to-end service calls, data validation
**Goal:** Ensure data flow works correctly

### 3. E2E Tests
**Focus:** Complete user flows
**Scope:** Map interaction scenarios, end-to-end workflows
**Goal:** Ensure user experience works as expected

### 4. UX Validation
**Focus:** Interaction behavior
**Scope:** Loading states, empty states, error handling
**Goal:** Ensure professional user experience

### 5. Regression Guard
**Focus:** LLM safety checks
**Scope:** Architecture integrity, MVP scope adherence
**Goal:** Prevent unintended changes

---

## 🧱 BACKEND TESTING (driver-service)

### 2.1 Station API Tests

**Endpoint:** `GET /api/v1/stations`

**Assertions:**
- [ ] returns array
- [ ] station fields exist:
  - [ ] id
  - [ ] name
  - [ ] latitude
  - [ ] longitude
  - [ ] status

**Test Cases:**
- Case 1 — Empty database
  - [ ] returns empty array
- Case 2 — Valid data
  - [ ] returns all stations
  - [ ] maintains order

---

### 2.2 Nearby Search Tests (CRITICAL)

**Endpoint:** `GET /api/v1/stations/nearby?lat=..&lng=..&radius=5000`

**Test Cases:**

**Case 1 — Valid location**
- [ ] returns stations within radius
- [ ] sorted by distance ASC
- [ ] distance calculations accurate
- [ ] only active stations returned

**Case 2 — No stations**
- [ ] returns empty array
- [ ] no error thrown
- [ ] proper HTTP 200 response

**Case 3 — Invalid coordinates**
- [ ] returns error object
- [ ] error code is appropriate
- [ ] HTTP 400 response

**Case 4 — Radius validation**
- [ ] max radius enforced (10km)
- [ ] min radius respected (100m)
- [ ] radius validation in service layer

---

### 2.3 Station Detail Tests

**Endpoint:** `GET /api/v1/stations/{id}`

**Assertions:**
- [ ] correct station returned
- [ ] includes chargers array
- [ ] charger fields present
- [ ] 404 if not found

**Test Cases:**
- Case 1 — Valid station
  - [ ] returns station details
  - [ ] includes all required fields
  - [ ] chargers array populated
- Case 2 — Invalid ID
  - [ ] returns 404
  - [ ] error message clear

---

## 🔌 API CONTRACT TESTING

### 3.1 Contract Rules

**Response shape must not change:**
- [ ] no missing fields allowed
- [ ] no wrapper objects (data: forbidden)
- [ ] consistent field types
- [ ] proper JSON structure

### 3.2 Schema Validation

**Station Schema:**
```typescript
interface Station {
  id: string
  name: string
  latitude: number
  longitude: number
  status: string
  power_kw?: number
  connector_types?: string[]
}
```

**Validation Tests:**
- [ ] Response matches TypeScript interface
- [ ] Required fields present
- [ ] Optional fields behave correctly
- [ ] Types are correct

### 3.3 Regression Rule

**If API shape changes → test FAIL immediately**

---

## 📱 FRONTEND UNIT TESTS

### 4.1 MapContainer Tests

**Validate:**
- [ ] renders without crashing
- [ ] receives markers correctly
- [ ] calls onMarkerClick
- [ ] updates region correctly
- [ ] handles platform differences

**Test Cases:**
- Case 1 — Empty markers
  - [ ] renders empty state
  - [ ] no error thrown
- Case 2 — Multiple markers
  - [ ] all markers displayed
  - [ ] markers positioned correctly

---

### 4.2 Marker Rendering Tests

**Validations:**
- [ ] correct number of markers
- [ ] selected marker highlights
- [ ] no duplicate renders
- [ ] markers memoized correctly

**Test Cases:**
- Case 1 — Single marker
  - [ ] single marker displayed
  - [ ] marker clickable
- Case 2 — Multiple markers
  - [ ] all markers shown
  - [ ] no overlap issues
  - [ ] selection works

---

### 4.3 State Tests

**Zustand Store:**
- [ ] selectedStationId updates correctly
- [ ] mapCenter updates correctly
- [ ] radius updates correctly
- [ ] state persistence works

**Test Cases:**
- Case 1 — Station selection
  - [ ] selection sets state
  - [ ] state triggers UI updates
- Case 2 — Map movement
  - [ ] position updates state
  - [ ] triggers nearby search

---

## 🔌 API CLIENT TESTS (@bm/api-client)

### 5.1 Functions Tested

- [ ] `getStations()`
- [ ] `getNearbyStations()`
- [ ] `getStationById()`
- [ ] `trackEvent()`

---

### 5.2 Rules

- [ ] correct endpoint used
- [ ] correct params passed
- [ ] correct response typed
- [ ] proper error handling

**Test Cases:**
- Case 1 — Successful request
  - [ ] returns typed response
  - [ ] calls correct endpoint
  - [ ] handles success
- Case 2 — Network error
  - [ ] throws error appropriately
  - [ ] error is handled in UI
- Case 3 — Timeout
  - [ ] handles timeout
  - [ ] provides fallback

---

### 5.3 Failure Cases

- [ ] network error handled
- [ ] timeout fallback
- [ ] invalid response rejected
- [ ] proper error messages

---

## 🧭 E2E TESTING (CRITICAL FLOW)

### 6.1 Scenario 1 — App Load

**Steps:**
1. Open app
2. Get location
3. Fetch nearby stations
4. Render map

**Expected:**
- [ ] Map visible
- [ ] Markers rendered
- [ ] No crash
- [ ] Stations displayed

**Validation:**
- [ ] Loading state shown
- [ ] Error state if fails
- [ ] Data populated correctly

---

### 6.2 Scenario 2 — Map Movement

**Steps:**
1. Drag map
2. Wait debounce
3. API called again
4. Markers updated

**Expected:**
- [ ] Smooth update
- [ ] No full reload
- [ ] No flicker
- [ ] Performance maintained

**Validation:**
- [ ] Debounce working
- [ ] API called only once
- [ ] Markers replaced correctly
- [ ] No duplicate markers

---

### 6.3 Scenario 3 — Station Selection

**Steps:**
1. Tap marker
2. Open detail view
3. Fetch station detail
4. Display details

**Expected:**
- [ ] Detail opens instantly
- [ ] Correct station shown
- [ ] UI not blocked
- [ ] Smooth transition

**Validation:**
- [ ] Haptic feedback (mobile)
- [ ] Animation plays
- [ ] No loading freeze
- [ ] Correct data displayed

---

## 🎨 UX TESTING (PRO MAX RULE)

### 7.1 Loading States

**Must verify:**

**Skeleton Map:**
- [ ] Skeleton visible on load
- [ ] No spinner usage
- [ ] Smooth appearance
- [ ] Disappears when ready

**Server State:**
- [ ] Loading indicator for API calls
- [ ] Not blocking UI
- [ ] Quick feedback
- [ ] Progress indication

---

### 7.2 Empty States

**Test:**

**No stations nearby:**
- [ ] User-friendly message
- [ ] Retry option
- [ ] No error text
- [ ] Clear action

**GPS disabled:**
- [ ] Location permission error
- [ ] Clear message
- [ ] Re-request option
- [ ] No frustration

**API failure:**
- [ ] Retry button
- [ ] Retry functionality
- [ ] Error message
- [ ] Clear next steps

---

### 7.3 Interaction Latency

**Measure:**

**Marker tap response:**
- [ ] < 100ms perceived
- [ ] Immediate feedback
- [ ] No lag

**Map pan:**
- [ ] No UI freeze
- [ ] Smooth panning
- [ ] No stuttering
- [ ] 60fps maintained

**Data updates:**
- [ ] Instant updates
- [ ] No loading delays
- [ ] Smooth transitions

---

## 📊 ANALYTICS TESTS

### 8.1 Events Validation

**Event Table:**

| Event | Trigger | Validation |
|-------|---------|------------|
| MapViewed | App open | [ ] Fires on launch |
| StationOpened | Marker tap | [ ] Fires on selection |
| NearbySearchExecuted | Map move | [ ] Fires on update |

**Rules:**
- [ ] events must not block UI
- [ ] events must not fail app flow
- [ ] events fire reliably
- [ ] No duplicate events

**Test Cases:**
- Case 1 — MapViewed
  - [ ] Fires on app launch
  - [ ] No delay
  - [ ] Correct payload
- Case 2 — StationOpened
  - [ ] Fires on marker tap
  - [ ] Includes station ID
  - [ ] No blocking
- Case 3 — NearbySearchExecuted
  - [ ] Fires on map movement
  - [ ] Fires only after debounce
  - [ ] Payload includes coordinates

---

## 🧪 REGRESSION GUARD (LLM SAFETY)

### 9.1 FAIL Conditions

**If any of these occur → TEST FAIL:**

- [ ] New endpoint introduced
- [ ] fetch() used in frontend
- [ ] Map logic outside MapContainer
- [ ] API shape changed
- [ ] New service added in MVP-1
- [ ] Backend DB access from frontend
- [ ] Architecture changed
- [ ] MVP scope expanded
- [ ] TypeScript type errors introduced

---

### 9.2 PASS Conditions

**Pass only if:**

- [ ] Architecture unchanged
- [ ] MVP scope intact
- [ ] API contract stable
- [ ] All existing tests pass
- [ ] No new warnings
- [ ] No TypeScript errors
- [ ] No ESLint errors
- [ ] Performance targets met

---

## 🧠 TEST EXECUTION ORDER

### 1. Backend Unit Tests
- [ ] Service logic tests
- [ ] Query tests
- [ ] Validation tests
- [ ] Error handling tests

### 2. API Contract Tests
- [ ] Response shape validation
- [ ] Schema validation
- [ ] Endpoint tests
- [ ] Error format tests

### 3. API Client Tests
- [ ] Function calls
- [ ] Parameter passing
- [ ] Response typing
- [ ] Error handling

### 4. Frontend Unit Tests
- [ ] Component tests
- [ ] Hook tests
- [ ] Store tests
- [ ] Utility tests

### 5. Map Integration Tests
- [ ] MapContainer tests
- [ ] Marker rendering
- [ ] Interaction tests
- [ ] Platform tests

### 6. E2E Flows
- [ ] App load flow
- [ ] Map movement flow
- [ ] Station selection flow
- [ ] Error recovery flow

### 7. UX Validation
- [ ] Loading states
- [ ] Empty states
- [ ] Error states
- [ ] Interaction latency

### 8. Regression Guard Check
- [ ] Architecture integrity
- [ ] MVP scope verification
- [ ] API contract stability
- [ ] No violations found

---

## 🚫 TESTING ANTI-PATTERNS

**Forbidden:**
- [ ] testing UI without API mocking
- [ ] skipping nearby search tests
- [ ] ignoring MapContainer abstraction
- [ ] testing backend without PostGIS validation
- [ ] testing without TypeScript coverage
- [ ] skipping error state tests
- [ ] ignoring performance tests
- [ ] testing without E2E coverage

---

## 🎯 MVP-1 TEST COVERAGE

### Required Coverage

**Backend:**
- [ ] Service logic ≥ 80%
- [ ] API endpoints ≥ 90%
- [ ] Query optimization tests ≥ 90%
- [ ] Error handling ≥ 95%

**Frontend:**
- [ ] Components ≥ 70%
- [ ] Hooks ≥ 80%
- [ ] State management ≥ 90%
- [ ] Utility functions ≥ 95%

**E2E:**
- [ ] Core flows ≥ 90%
- [ ] Critical paths covered
- [ ] Error scenarios covered

**Overall:**
- [ ] Combined coverage ≥ 80%

---

## 🧠 CORE PRINCIPLE

**If a feature is not tested, it does not exist.**

---

*This testing specification ensures comprehensive coverage of all MVP-1 functionality with strict quality gates.*
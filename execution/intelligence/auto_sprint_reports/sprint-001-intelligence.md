# Sprint 001 — Auto Sprint Intelligence Report
**Generated:** SPEC phase analysis

---

## 1. Missing Edge Cases

### 1.1 Empty / Zero Results
- **Feature 5 (nearby function):** Must return `[]` not error when no stations within radius
- **Feature 6 (API):** Must return `[]` not `null` or 404 for empty results
- **Feature 8 (Map):** Must show empty state message (not blank map)

### 1.2 Invalid Input
- **Feature 6:** `lat` outside ±90, `lng` outside ±180, `radius` ≤ 0 — must return 400 with descriptive error
- **Feature 9:** What if OSM gives null name in tags? Must handle gracefully (fallback to "Charging Station" or OSM ID)

### 1.3 Edge Radius
- **Feature 5:** radius=0 should return empty, not error
- **Feature 5:** radius > 100km should be capped or have reasonable upper limit

### 1.4 Database Unavailable
- **Feature 7:** must handle DB connection failure — return 503, not 500 crash
- **Feature 6:** must handle DB query failure — return 503, not 500

### 1.5 OSM Import Duplicates
- **Feature 3:** Overpass API may return duplicate nodes for the same OSM ID in different responses. Must handle `ON CONFLICT (osm_id) DO NOTHING`.

---

## 2. Missing APIs

| Missing | Impact | Suggested |
|---|---|---|
| Traefik routing config for `/api/v1/driver/*` | API unreachable without it | Add docker-compose + traefik config to Feature 1 |
| Docker Compose for driver-service | No orchestration | Extend Feature 1 or add Feature 6b |
| Dockerfile for driver-service | Cannot containerize | Add to Feature 6 |

---

## 3. Missing UI States (Feature 8)

| State | Missing? |
|---|---|
| Loading | ✅ — addressed in spec |
| Empty (no stations) | ✅ — addressed in spec |
| Error (API unreachable) | ✅ — addressed in spec |
| Error (invalid params) | ⚠️ — map could send invalid coords; should show error message |
| Success (stations visible) | ✅ — addressed |

---

## 4. Missing Domain Entities

| Entity | Concern |
|---|---|
| `Station` domain model | Feature 6 needs a domain-level Station struct with validation (lat/lng bounds, name constraints). Ensure it lives in `domain/`, not `api/`. |
| `Coordinates` value object | Encapsulate lat/lng validation at domain level |
| `NearbyQuery` domain object | Encapsulate lat/lng/radius + validation |

---

## 5. Architecture Deviations

| Deviation | Severity | Resolution |
|---|---|---|
| **Cross-schema write in Feature 4** (gis → inventory without admin-service) | HIGH | Accepted as validation-phase bootstrap exception. Documented in sprint spec. Post-validation, route through admin-service. |
| **No materialized views** | MEDIUM | Sprint 1 queries `inventory.stations` directly. In future, migrate to MV reads per architecture. Logged as tech debt. |
| **No Redis cache** | LOW | Acceptable for minimal scope. Cache added in later sprint. |

---

## 6. Known Bug Compliance

| Bug | Status | Verification |
|---|---|---|
| KNOWN-001: `WHERE s.is_test = FALSE` | ✅ Addressed in Feature 5 spec, Feature 2 schema includes `is_test` column |
| KNOWN-002: `deleted_at` on partners | ⏳ Not applicable — `partners` not created yet |
| KNOWN-003: single `/nearby` endpoint | ✅ Only in driver-service |
| KNOWN-004: `grep -E` flag | ⏳ Tooling fix — pending ci_guard.sh implementation |

---

## 7. Deferred Items (Tech Debt)

| Item | Reason | Target |
|---|---|---|
| nanoid(12) entity IDs on stations | OSM import uses synthetic IDs; nanoid migration needed post-import | Post-sprint cleanup |
| JWT auth on `/nearby` | Sprint 1 is unauthenticated per architecture (public for browse) | Sprint 002+ |
| Materialized views for geo reads | Sprint 1 queries raw table | Sprint 003+ |
| Redis cache for spatial queries | Not needed at current scale | Sprint 003+ |
| OpenAPI spec lock | Immutable after IMPLEMENT phase starts | SPEC phase |

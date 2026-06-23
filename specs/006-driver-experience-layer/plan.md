# Implementation Plan: Driver Experience Layer (UX + Product Polish)

**Branch**: `006-driver-experience-layer` | **Date**: 2026-06-22 | **Spec**: [spec.md](./spec.md)

**Input**: Sprint 5 — high-performance driver experience with favorites, preferences, offline cache, map UX, search, skeleton loaders, optimistic UI, session continuity.

## Summary

Deliver 7 user stories across 3 microservice boundaries: driver-service (favorites API, search endpoint, telemetry), frontend (map UX, offline cache, skeleton loaders, optimistic UI, session continuity), and auth-service (no changes). Zero schema expansion — all personalization uses existing `users.preferences` JSONB. Frontend remains strictly data-consumer-only; online search uses driver-service → Postgres trigram; offline search uses local cache.

## Technical Context

**Language/Version**: Frontend: TypeScript 5.x, React Native/Expo SDK 52+ | Backend: Rust 1.75+, actix-web 4.4

**Primary Dependencies**: Frontend: expo-router, react-native-maps (clustering), @tanstack/react-query (sync), zustand (state), AsyncStorage (offline), ui-kit (skeleton components) | Backend: actix-web, sqlx (pg_trgm extension), serde, uuid

**Storage**: `users.preferences` JSONB (favorites + preferences as separate top-level sections), AsyncStorage/IndexedDB (offline cache), Postgres GIS (station search)

**Testing**: Jest + React Native Testing Library (frontend), cargo test (backend), Playwright (web E2E)

**Target Platform**: iOS, Android, Web (Expo universal)

**Project Type**: Full-stack monorepo with Expo mobile/web frontend + Rust microservices

**Performance Goals**: Skeleton placeholders <150ms, optimistic UI <150ms, search P95 < 1s, map 60fps

**Constraints**:
1. No schema expansion — all personalization in existing `users.preferences` JSONB only
2. Frontend must remain data-consumer-only — no business logic leakage into UI
3. Offline functionality must not require any backend dependency
4. Only previously viewed map tiles available offline — full offline map is out of scope
5. Favorites stored in dedicated `favorites` section of JSONB, separate from `preferences`
6. Telemetry events must use existing driver-service telemetry pipeline (Sprint 3)
7. Authentication remains Keycloak-managed — session continuity is UI state only

**Scale/Scope**: 7 user stories, 23 functional requirements, primarily frontend with 2 new driver-service API endpoints (favorites + search enhancement) and telemetry event extensions.

## Enforcement Kernel Specification

### CI Execution DAG

**Stage Order** (strict linear sequence with artifact passing):

```
Stage 1: format_check
  ↓ Passes
  artifact: cargo fmt --check --all

Stage 2: type_check
  ↓ Passes, consumes format_check
  artifact: cargo clippy --all-targets

Stage 3: dependency_graph_validation
  ↓ Passes, consumes type_check
  artifact: AST-based forbidden edge detection

Stage 4: identity_validation
  ↓ Passes, consumes dependency_graph
  artifact: UUID/nanoid usage validation

Stage 5: schema_validation
  ↓ Passes, consumes identity_validation
  artifact: Database schema consistency check

Stage 6: sqlx_compile_check
  ↓ Passes, consumes schema_validation
  artifact: SQLx offline verification (new search queries)

Stage 7: analytics_write_gate
  ↓ Passes, consumes sqlx_compile_check
  artifact: Single-writer analytics enforcement

Stage 8: integration_tests
  ↓ Passes, consumes analytics_gate
  artifact: cargo test --all

Stage 9: build_success
  ↓ Passes, consumes integration_tests
  artifact: cargo build --release
```

**Failure Propagation Rules**:
- Hard-stop: Any stage failure immediately aborts all subsequent stages
- Deterministic exit codes: 0=success, 1=failure, 2=skipped
- No partial success allowed
- Each stage logs detailed failure reason to CI output

**Artifact Passing Model**:
- Each stage produces strict JSON artifact on success
- Next stage consumes previous artifact as input
- No side effects between stages
- All artifacts stored in `.specify/ci-artifacts/` for audit trail

### Enforcement Validator Specifications

#### 1. Preferences Isolation Gate

**Input**: AST of all frontend and backend files touching `users.preferences`

**Algorithm**:
- Scan all Rust/Cargo.toml for `users.preferences` references in migration files
- Scan all frontend code for direct database references
- FAIL if preferences stored outside `users.preferences` JSONB
- FAIL if any new migration file creates a new column or table for preferences/favorites

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Verified: all personalization uses existing users.preferences JSONB"
}
```

**Failure Signature**: Exit code 1 with details of schema violation

---

#### 2. Offline Storage Gate

**Input**: AST of all frontend files referencing offline storage

**Algorithm**:
- Scan for imports of AsyncStorage, IndexedDB, or other local storage
- Scan for API calls that would be required when offline
- FAIL if any offline functionality has a backend dependency
- FAIL if offline storage uses a backend service

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Verified: offline storage has no backend dependencies"
}
```

**Failure Signature**: Exit code 1 with details of backend dependency in offline code

---

#### 3. Search Safety Gate

**Input**: AST of search implementation code

**Algorithm**:
- Scan for search query construction patterns
- Verify online search uses SQLx (not dynamic SQL or external search service)
- FAIL if non-SQLx search implementation or external search service detected
- Verify frontend search (offline mode) queries local cache only

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Verified: online search uses SQLx, offline search uses local cache"
}
```

**Failure Signature**: Exit code 1 with details of unauthorized search mechanism

---

#### 4. UI Boundary Gate

**Input**: AST of frontend source code

**Algorithm**:
- Scan for business logic patterns (direct database calls, service topology decisions, identity validation)
- Scan for ui-kit import violations (direct component overrides outside ui-kit)
- FAIL if frontend contains business logic or ui-kit violated by direct overrides
- FAIL if frontend imports backend crates directly

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Verified: frontend is consumer-only, no business logic leaked"
}
```

**Failure Signature**: Exit code 1 with details of business logic leakage

---

#### 5. Performance Regression Gate

**Input**: Baseline and current benchmark results

**Algorithm**:
- Compare API response times from baseline
- Compare map rendering latency from baseline
- FAIL if API response time increases beyond baseline by >10%
- FAIL if map rendering exceeds latency budget (60fps target)

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Verified: no performance regression detected"
}
```

**Failure Signature**: Exit code 1 with details of performance regression

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Gate 1: Service Topology Lock (✅ PASS)

**Constitution Requirement**: Exactly three microservices MUST exist: auth-service (3000), driver-service (3001), admin-service (3002). No additional services, no port changes.

**Compliance Status**: ✅ PASS

**Justification**: Sprint 5 adds no new services. Favorites API extends driver-service (existing port 3001). Auth-service and admin-service unchanged. Map clustering and offline cache are entirely frontend-side.

**Verification**: CI topology check — verify only ports 3000/3001/3002 are active.

---

### Gate 2: Data Ownership (✅ PASS)

**Constitution Requirement**: Every data domain has exactly one owning service. Cross-service writes forbidden.

**Compliance Status**: ✅ PASS

**Justification**: Favorites are user-generated relational data owned by driver-service (following the GIS domain ownership pattern). FR-022 explicitly assigns favorites APIs to driver-service. Preferences are stored in `users.preferences` JSONB owned by auth-service. No cross-service writes.

**Verification**: Static analysis — verify favorites API implemented only in driver-service. Verify no other service writes to users.preferences.

---

### Gate 3: Identity Dual System (✅ PASS)

**Constitution Requirement**: UUID in users table only, nanoid(12) with PREFIX in entity tables.

**Compliance Status**: ✅ PASS

**Justification**: Favorites reference station IDs (STA-prefixed nanoid). User identity uses UUID from Keycloak. No mixing.

**Verification**: Static analysis — verify station IDs in favorites use STA-prefixed nanoid format.

---

### Gate 4: Contract-First (✅ PASS)

**Constitution Requirement**: Contract definition → Backend implementation → Frontend implementation. domain-types MUST NOT depend on backend frameworks.

**Compliance Status**: ✅ PASS

**Justification**: Favorites and search DTOs defined in domain-types before backend implementation. Frontend consumes contracts from domain-types. No framework dependencies leak into domain-types.

**Verification**: Dependency validation with AST analysis.

---

### Gate 5: SQLx Compile-Time Verification (✅ PASS)

**Constitution Requirement**: All SQL queries MUST be compile-time verified via SQLx. NO dynamic SQL construction.

**Compliance Status**: ✅ PASS

**Justification**: Online search uses Postgres trigram (pg_trgm) via SQLx queries. All search queries are compile-time verified. No dynamic SQL.

**Verification**: CI sqlx_compile_check — verify new search queries pass offline verification.

---

### Gate 6: Frontend Consumer-Only (✅ PASS)

**Constitution Requirement**: Frontend MUST NOT contain business logic, direct database imports, or service topology decisions.

**Compliance Status**: ✅ PASS

**Justification**: Map clustering, skeleton loaders, optimistic UI, session continuity, and offline cache are all presentation/local state concerns. Online search is delegated to driver-service. FR-019 explicitly separates online (driver-service) vs offline (local cache) search.

**Verification**: UI Boundary Gate — AST analysis confirms no business logic leakage.

---

### Gate 7: Single-Writer Analytics (✅ PASS)

**Constitution Requirement**: driver-service ONLY can write to analytics_db. admin-service and auth-service can ONLY read.

**Compliance Status**: ✅ PASS

**Justification**: Telemetry events (FR-021: favorite_added, search_executed, etc.) are ingested through the existing driver-service telemetry pipeline established in Sprint 3. No new analytics writers introduced.

**Verification**: CI analytics_write_gate — verify only driver-service emits telemetry events.

---

### Gate 8: Session Continuity vs Authentication (✅ PASS)

**Constitution Requirement**: Authentication session lifetime must remain Keycloak-managed.

**Compliance Status**: ✅ PASS

**Justification**: FR-017 and FR-023 explicitly separate UI session state (map position, filters, last section) from authentication. Session continuity stores UI state locally; Keycloak manages auth independently.

**Verification**: Static analysis — verify no changes to Keycloak token handling or auth middleware.

## Project Structure

### Documentation (this feature)

```text
specs/006-driver-experience-layer/
├── spec.md              # Feature specification (7 stories, 23 FRs)
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output
```

### Source Code (repository root)

```text
apps/
├── packages/
│   ├── domain-types/    # FavoritesDTO, SearchResultDTO, PreferenceDTO, TelemetryEvent
│   └── ui-kit/          # SkeletonLoader, StationCard, PreviewCard, ClusterMarker
├── mobile/              # Expo app: map UX, favorites UI, offline cache
└── web/                 # Next.js app: shares ui-kit and domain-types

services/
├── driver-service/
│   ├── src/
│   │   ├── api/
│   │   │   ├── favorites.rs    # POST/GET/DELETE /api/v1/driver/favorites
│   │   │   └── search.rs       # Enhanced fuzzy search with pg_trgm
│   │   └── db/
│   │       └── queries.rs      # Trigram search queries
│   └── migrations/             # None needed (no schema changes)
├── auth-service/               # No changes in Sprint 5
└── admin-service/              # No changes in Sprint 5
```

**Structure Decision**: No new modules. Favorites API extends existing driver-service API module. Search enhancement extends existing driver-service search. Frontend code organized within existing mobile/web app directories. Telemetry follows Sprint 3 event pipeline.

## Complexity Tracking

No constitution violations in this feature.

### Enforcement Kernel Complexity

The enforcement kernel introduces complexity to ensure constitutional compliance:

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| Preferences Isolation Gate | Prevents schema expansion violations | Manual review misses edge cases |
| Offline Storage Gate | Ensures offline works without backend | Cannot verify at runtime after deployment |
| Search Safety Gate | Prevents external search service dependency | SQLx compile check alone doesn't detect external services |
| UI Boundary Gate | Prevents business logic leakage to frontend | Manual code review inconsistent at scale |
| Performance Regression Gate | Ensures 60fps map and <150ms skeletons | Without gates, performance degrades silently |

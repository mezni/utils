# Research Report: Driver Experience Layer

**Branch**: `006-driver-experience-layer` | **Date**: 2026-06-22

## Overview

Technical research and decisions for Sprint 5 (Driver Experience Layer). Focus areas: JSONB storage architecture for preferences + favorites, Postgres trigram search, offline-first caching, telemetry extension, map clustering at scale.

## Research Tasks

### R1: JSONB Storage Architecture for Preferences + Favorites

**Decision**: Store preferences and favorites as separate top-level keys in `users.preferences` JSONB column.

**Rationale**:
- JSONB supports arbitrary top-level keys — `preferences` and `favorites` can coexist without conflict
- Separating concerns avoids large full-document rewrites when only one section changes
- Favorites (relational, potentially large) have different lifecycle than preferences (settings, stable)
- Postgres JSONB supports partial updates via `jsonb_set()` path notation, minimizing write overhead
- No schema migration needed — existing column accommodates both sections

**Alternatives Considered**:
1. New `user_favorites` table — rejected: violates FR-003 (no schema expansion)
2. Array column — rejected: less flexible than JSONB for future extensions
3. Single flat JSONB — rejected: mixing relational data with settings causes synchronization conflicts

**Data Structure**:
```json
{
  "preferences": {
    "connector_type": "CCS",
    "max_distance": 25,
    "last_region": {"lat": 46.948, "lng": 7.4474},
    "map_filters": {"available_only": true}
  },
  "favorites": [
    {"station_id": "STA-abc123def456", "added_at": "2026-06-22T10:00:00Z"},
    {"station_id": "STA-xyz789ghi012", "added_at": "2026-06-22T11:30:00Z"}
  ]
}
```

### R2: Postgres Trigram Search for Fuzzy Matching

**Decision**: Online search uses driver-service → Postgres `pg_trgm` extension with SQLx compile-time verified queries.

**Rationale**:
- `pg_trgm` provides built-in fuzzy matching via `similarity()` and `show_trgm()`
- No external search service dependency — satisfies Search Safety Gate
- SQLx compile-time verification ensures query safety — satisfies Constitution Gate 5
- Handles typos, partial matches, and case-insensitive search natively
- Performance adequate for 10,000+ station dataset: trigram indexes are GiST-indexable

**Alternatives Considered**:
1. Elasticsearch — rejected: violates Search Safety Gate, external dependency
2. Client-side fuzzy matching (fuse.js) — rejected: CPU/memory heavy on mobile for large datasets
3. LIKE/ILIKE queries — rejected: poor performance without proper indexing for fuzzy matching

**Key Query Pattern**:
```sql
SELECT station_id, name, address,
  similarity(name, $1) AS relevance
FROM gis.stations
WHERE name % $1 OR address % $1
ORDER BY relevance DESC
LIMIT 20
```

### R3: Offline-First Caching Architecture

**Decision**: Local cache uses AsyncStorage (mobile) / IndexedDB (web) with stale-while-revalidate strategy.

**Rationale**:
- AsyncStorage/IndexedDB are platform-native, zero backend dependency — satisfies Offline Storage Gate
- Stale-while-revalidate: display cached data immediately, refresh in background when online
- Only previously viewed map tiles cached — no full offline map download
- Pending writes (favorite toggles made offline) queued locally with timestamp for last-write-wins resolution

**Alternatives Considered**:
1. SQLite (via expo-sqlite) — rejected: overkill for KV storage needs, adds complexity
2. Redux Persist — rejected: couples state management with persistence, less portable
3. Custom file-based cache — rejected: platform-specific, maintenance burden

**Conflict Resolution**:
- Each favorite operation tagged with server timestamp on write
- Offline operations queued with local timestamp
- On reconnect: server timestamp wins (last-write-wins)
- Batch sync endpoint processes queue in timestamp order

### R4: Telemetry Event Schema Extension

**Decision**: New event types use the existing Sprint 3 telemetry schema (raw_events table) with `event_type` enum extension.

**Rationale**:
- Reuses existing driver-service telemetry pipeline — no new infrastructure
- Existing raw_events table supports arbitrary JSON payload in event_data column
- Adds event types to the existing event type enum (no schema change)

**New Event Types**:
- `FAVORITE_ADDED` — payload: `{station_id, user_id, timestamp}`
- `FAVORITE_REMOVED` — payload: `{station_id, user_id, timestamp}`
- `SEARCH_EXECUTED` — payload: `{query, result_count, timestamp}`
- `SEARCH_SELECTED` — payload: `{query, station_id, position, timestamp}`
- `FILTER_CHANGED` — payload: `{filter_type, old_value, new_value, timestamp}`
- `OFFLINE_MODE_ENTERED` — payload: `{duration_seconds, timestamp}`

**Alternatives Considered**:
1. New telemetry endpoint — rejected: duplicates existing Sprint 3 infrastructure
2. Direct analytics_db write from frontend — rejected: violates Single-Writer Analytics rule

### R5: Optimistic UI + Conflict Resolution

**Decision**: Optimistic UI updates apply immediately; on server failure, revert to previous state; cross-device conflicts resolved via last-write-wins.

**Rationale**:
- Optimistic updates are purely presentational — no business logic in frontend
- Revert on failure uses the previous UI state snapshot
- Last-write-wins based on server timestamp prevents undefined behavior across devices
- Match pattern established by modern frameworks (TanStack Query, SWR)

**Edge Cases**:
- Favorite toggle while offline: queued locally, synced on reconnect (server timestamp wins on conflict)
- Rapid toggle (favorite → unfavorite → favorite) before server response: last action wins, pending requests cancelled

### R6: Map Clustering Performance at Scale

**Decision**: Client-side clustering using react-native-maps clustering features (supercluster/marker clustering).

**Rationale**:
- Clustering is purely presentational — no business logic
- react-native-maps built-in clustering handles 10,000+ markers without jank
- Progressive loading: only markers in viewport rendered, clusters break apart on zoom
- Custom color-coding by connector type and availability rendered client-side from station data

**Performance Target**: 60fps during zoom/pan with up to 1,000 visible markers (clustered).

### R7: Session Continuity — UI State Only

**Decision**: Session continuity stores and restores UI state (map position, filters, last section) from local cache. Authentication remains Keycloak-managed.

**Rationale**:
- UI state is presentation-only — no business logic or auth tokens stored in session state
- Keycloak handles auth independently; session continuity does not extend token lifetime
- 30-minute UI state retention is independent of auth token expiry
- No changes to auth middleware or Keycloak configuration

**Stored State**:
```json
{
  "map_region": {"latitude": 46.948, "longitude": 7.4474, "latitudeDelta": 0.1, "longitudeDelta": 0.1},
  "filters": {"connector_type": "CCS", "available_only": true},
  "last_section": "favorites",
  "timestamp": "2026-06-22T15:30:00Z"
}
```

## Decision Log

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | Separate `preferences` + `favorites` sections in JSONB | Prevents sync conflicts, enables partial updates |
| D2 | Postgres pg_trgm for online fuzzy search | SQLx-safe, no external dependencies, indexable |
| D3 | AsyncStorage/IndexedDB for offline cache | Platform-native, zero backend dependency |
| D4 | Reuse Sprint 3 telemetry pipeline | No new infrastructure, extends existing event enum |
| D5 | Last-write-wins for cross-device conflicts | Simple, deterministic, well-understood semantics |
| D6 | Client-side clustering via react-native-maps | Presentation-only, handles 10K+ markers at 60fps |
| D7 | UI state only for session continuity | No auth changes, local storage only |
| D8 | skeleton loaders in ui-kit | Shared components, consistent across mobile/web |

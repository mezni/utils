# Data Model: Driver Experience Layer

## Overview

Entities for Sprint 5 personalization and search features. All personalization data stored in existing `users.preferences` JSONB column — zero schema expansion.

## Entity: UserPreferences

**Storage**: `platform_db.users.preferences` JSONB column

**Description**: User-customizable settings for app behavior. Stored under the `preferences` top-level key in the JSONB document.

**Data Structure**:
```json
{
  "connector_type": "CCS",
  "max_distance": 25,
  "last_region": {"lat": 46.948, "lng": 7.4474},
  "map_filters": {"available_only": true}
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| connector_type | string | No | Preferred charger type: CCS, CHAdeMO, Type2 |
| max_distance | integer | No | Max search radius in km (default: 50) |
| last_region | object | No | Last viewed map center: {lat, lng} |
| map_filters | object | No | Map filter preferences |

**Validation Rules**:
- `connector_type` must be one of: CCS, CHAdeMO, Type2, or null (no preference)
- `max_distance` must be between 1 and 500 km
- `last_region.lat` must be between -90 and 90
- `last_region.lng` must be between -180 and 180

**Ownership**: auth-service (users schema)

## Entity: UserFavorites

**Storage**: `platform_db.users.preferences` JSONB column, `favorites` top-level key

**Description**: Collection of station IDs saved as favorites by a user. Stored as a JSON array under the `favorites` key, separate from `preferences`.

**Data Structure**:
```json
{
  "favorites": [
    {"station_id": "STA-abc123def456", "added_at": "2026-06-22T10:00:00Z"},
    {"station_id": "STA-xyz789ghi012", "added_at": "2026-06-22T11:30:00Z"}
  ]
}
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| station_id | string (nanoid) | Yes | STA-prefixed station identifier |
| added_at | datetime (ISO 8601) | Yes | Timestamp when favorite was added |

**Validation Rules**:
- `station_id` must match pattern `^STA-[A-Za-z0-9]{12}$`
- `added_at` must be a valid ISO 8601 timestamp
- Maximum 1000 favorites per user (soft limit)
- Duplicate `station_id` not allowed per user

**Ownership**: driver-service (API), auth-service (storage)

## Entity: StationSearchResult

**Source**: driver-service → Postgres GIS query

**Description**: Search response returned by driver-service for online queries, or from local cache for offline queries.

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| station_id | string (nanoid) | Yes | STA-prefixed station identifier |
| name | string | Yes | Station display name |
| address | string | Yes | Station street address |
| distance_km | float | Yes | Distance from search center or user location |
| relevance | float | Yes | Search relevance score (0.0 to 1.0) |
| connector_types | string[] | Yes | Available connector types |
| available | boolean | Yes | Whether station has available connectors |
| lat | float | Yes | Latitude |
| lng | float | Yes | Longitude |

**Validation Rules**:
- Search queries minimum 2 characters
- Results limited to top 20 by relevance
- Online: relevance calculated by pg_trgm `similarity()`
- Offline: relevance calculated by local fuzzy match against cached data

## Entity: SessionState

**Storage**: Local device cache (AsyncStorage/IndexedDB)

**Description**: Last app UI state for session continuity. Not stored on server — purely local.

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| map_region | object | No | {latitude, longitude, latitudeDelta, longitudeDelta} |
| filters | object | No | Active map filters (connector_type, available_only) |
| last_section | string | No | Last viewed section: map, favorites, search, settings |
| timestamp | datetime (ISO 8601) | Yes | When state was saved |

**Validation Rules**:
- State expires after 30 minutes — cleared on read if older
- Authentication tokens not stored in session state

## Entity: TelemetryEvent (extended)

**Source**: driver-service telemetry pipeline (Sprint 3)

**Description**: Existing telemetry event schema extended with new event types for Sprint 5.

**New Event Types**:
| Event Type | Payload |
|------------|---------|
| FAVORITE_ADDED | `{station_id, user_id, timestamp}` |
| FAVORITE_REMOVED | `{station_id, user_id, timestamp}` |
| SEARCH_EXECUTED | `{query_text, result_count, timestamp}` |
| SEARCH_SELECTED | `{query_text, station_id, position, timestamp}` |
| FILTER_CHANGED | `{filter_type, old_value, new_value, timestamp}` |
| OFFLINE_MODE_ENTERED | `{duration_seconds, timestamp}` |

## Relationships

```
User (auth-service)
  ├── has many Favorites (JSONB) → references Station (nanoid STA-*)
  ├── has Preferences (JSONB)
  └── emits TelemetryEvents → driver-service → analytics_db

Station (driver-service GIS)
  └── referenced by Favorites (via station_id)

Search (driver-service)
  └── returns StationSearchResult[] → frontend
```

## State Transitions

### Favorite State Machine
```
[unfavorited] → tap heart → [favorited (optimistic)] → server confirm → [favorited (persisted)]
                                                    → server error → [unfavorited (revert)]
[favorited] → tap heart → [unfavorited (optimistic)] → server confirm → [unfavorited (persisted)]
                                                     → server error → [favorited (revert)]
```

### Offline Sync State Machine
```
[online] → connectivity lost → [offline]
  ├── user actions queued locally
  └── changes pending: {action, timestamp}

[offline] → connectivity restored → [syncing]
  ├── queued actions sent in timestamp order
  ├── conflicts resolved: server timestamp wins
  └── [online] on sync complete
```

## Indexing Requirements

No new indexes required. Existing indexes on `gis.stations` (name, address, location) sufficient for trigram search. `users.preferences` JSONB is not indexed for favorites access — favorites are read via the user's primary key lookup.

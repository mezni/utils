# Data Model: Mobile & Web Driver Apps

**Feature**: MVP-1 Phase 4 - Mobile & Web Driver Apps
**Branch**: `004-driver-apps`
**Date**: 2026-06-12

## Overview

This document defines the data entities consumed by the driver apps. These entities are already defined in the backend schemas but are documented here for completeness and frontend consumption.

---

## Entities

### 1. Station

**Description**: A physical location where charging occurs.

**Source**: `inventory.station` table (PostgreSQL)

**Schema**:

```typescript
interface Station {
  id: string;              // STA-[nanoid]
  name: string;            // "Tunis Central Station"
  address: string;         // "123 Blvd de la Liberté, Tunis"
  geometry: {
    type: "Point";
    coordinates: [number, number]; // [longitude, latitude]
  };
  amenities: string[];     // ["WiFi", "Parking", "Cafe", "Shelter"]
  operating_hours: string; // "24/7" or "06:00 - 22:00"
  created_at: string;      // ISO 8601 UTC (e.g., "2026-06-10T12:00:00Z")
  updated_at: string;      // ISO 8601 UTC
}
```

**Validation Rules**:
- `id` must be unique (entity-prefixed nanoid: STA-...)
- `name` required, max 100 characters
- `address` required, max 255 characters
- `geometry.coordinates` must be valid longitude/latitude ([-180, 180], [-90, 90])
- `amenities` array, max 10 items
- `operating_hours` required, format "HH:MM - HH:MM" or "24/7"
- `created_at` and `updated_at` must be valid ISO 8601 UTC timestamps

**Relationships**:
- One-to-many with `Charger` (each station has multiple chargers)
- Many-to-many with `StationImage` (stations can have multiple images)

**Usage in Frontend**:
- Display in station list, map markers, and detail screens
- Required for navigation (external mapping app destination)
- Used for distance calculations (Haversine formula)

---

### 2. Charger

**Description**: An individual charging unit at a station.

**Source**: `inventory.charger` table (PostgreSQL)

**Schema**:

```typescript
interface Charger {
  id: string;              // CHR-[nanoid]
  station_id: string;      // STA-[nanoid] (foreign key to Station)
  charger_type: "CCS" | "CHAdeMO" | "AC";  // Charging standard
  connector_count: number; // Number of connectors (1-4)
  availability_status: "available" | "in_use" | "maintenance";
  power_kw: number;        // Power output in kilowatts (e.g., 50, 150, 350)
  is_active: boolean;      // Whether charger is currently operational
  created_at: string;      // ISO 8601 UTC
  updated_at: string;      // ISO 8601 UTC
}
```

**Validation Rules**:
- `id` must be unique (entity-prefixed nanoid: CHR-...)
- `station_id` must reference valid `Station.id`
- `charger_type` must be one of: CCS, CHAdeMO, AC
- `connector_count` required, 1-4 (integers only)
- `availability_status` must be one of: available, in_use, maintenance
- `power_kw` required, positive number (10-350)
- `is_active` boolean, default true
- `created_at` and `updated_at` must be valid ISO 8601 UTC timestamps

**State Transitions**:

```
[available] → [in_use] → [available]
[in_use] → [maintenance] → [available] (after repair)
```

**Usage in Frontend**:
- Display in station detail screen
- Used for charging estimates (power × time = distance)
- Filter stations by charger type (e.g., "Need CCS charging")

**Relationships**:
- Many-to-one with `Station` (belongs to one station)
- Many-to-many with `ChargerType` (type metadata)

---

### 3. StationImage

**Description**: Visual documentation of a station.

**Source**: `inventory.station_image` table (PostgreSQL)

**Schema**:

```typescript
interface StationImage {
  id: string;              // IMG-[nanoid]
  station_id: string;      // STA-[nanoid] (foreign key to Station)
  url: string;             // HTTPS URL to image file
  caption: string;         // Optional description (e.g., "Exterior view")
  is_primary: boolean;     // Whether this is the main image
  created_at: string;      // ISO 8601 UTC
}
```

**Validation Rules**:
- `id` must be unique (entity-prefixed nanoid: IMG-...)
- `station_id` must reference valid `Station.id`
- `url` required, HTTPS only, valid URL format
- `caption` optional, max 200 characters
- `is_primary` boolean, max 1 true value per station (default false)
- `created_at` must be valid ISO 8601 UTC timestamp

**Usage in Frontend**:
- Display in station detail screen (lazy loaded)
- `is_primary` image shown first
- Maximum 5 images per station (enforced by backend)

**Relationships**:
- Many-to-one with `Station` (belongs to one station)
- No explicit foreign key in schema (handled by backend validation)

**Lazy Loading Strategy**:
- Images loaded only when station detail screen is visible
- Use `expo-image` with lazy loading (React Native)
- Use native loading state (shimmer effect)
- Fallback to station logo or generic placeholder if no images

---

## Entity Relationships

```
Station (1) ──< Charger >── (N) Charger
       │
       ├──< StationImage >── (N) StationImage
       │       (max 5 images)
       │
       └──< [StationImage] >── (N) StationImage (only primary)
```

**Key Points**:
- One station has many chargers
- One station has many images (max 5)
- One station has exactly 1 primary image (or none)
- Chargers belong to exactly one station

---

## API Response Format

### GET /api/v1/stations

**Response**: JSON array of Station objects (paginated)

```json
{
  "data": [
    {
      "id": "STA-abc123",
      "name": "Tunis Central Station",
      "address": "123 Blvd de la Liberté, Tunis",
      "geometry": {
        "type": "Point",
        "coordinates": [10.1815, 36.8065]
      },
      "amenities": ["WiFi", "Parking", "Cafe"],
      "operating_hours": "24/7",
      "created_at": "2026-06-10T12:00:00Z",
      "updated_at": "2026-06-12T10:30:00Z"
    }
  ],
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 150,
    "total_pages": 8
  }
}
```

### GET /api/v1/stations/{id}

**Response**: Single Station object with associated Charger objects

```json
{
  "id": "STA-abc123",
  "name": "Tunis Central Station",
  "address": "123 Blvd de la Liberté, Tunis",
  "geometry": {
    "type": "Point",
    "coordinates": [10.1815, 36.8065]
  },
  "amenities": ["WiFi", "Parking", "Cafe"],
  "operating_hours": "24/7",
  "created_at": "2026-06-10T12:00:00Z",
  "updated_at": "2026-06-12T10:30:00Z",
  "chargers": [
    {
      "id": "CHR-xyz789",
      "station_id": "STA-abc123",
      "charger_type": "CCS",
      "connector_count": 2,
      "availability_status": "available",
      "power_kw": 50,
      "is_active": true,
      "created_at": "2026-06-10T12:00:00Z",
      "updated_at": "2026-06-12T10:30:00Z"
    }
  ],
  "images": [
    {
      "id": "IMG-123456",
      "station_id": "STA-abc123",
      "url": "https://cdn.bornemap.com/stations/STA-abc123/main.jpg",
      "caption": "Main entrance",
      "is_primary": true,
      "created_at": "2026-06-10T12:00:00Z"
    }
  ]
}
```

---

## Data Volume & Scale

### Expected Station Count
- **Tunisia**: ~150 stations (initial MVP)
- **Growth**: Target 1000+ stations by Phase 2
- **Search Radius**: 10km default, expands to 25km if <5 results

### Storage Requirements

**Station**:
- Average size: ~1KB (JSON)
- 150 stations = ~150KB
- 1000 stations = ~1MB

**Charger**:
- Average size: ~300 bytes
- 150 stations × 2 chargers = ~90KB
- 1000 stations × 4 chargers = ~1.2MB

**StationImage**:
- Image size: ~200KB each (compressed)
- 5 images × 150 stations = ~150MB
- **Critical**: Lazy load only (avoid loading all images at once)

**Total Data Volume (with images)**:
- ~150MB (worst case, all images loaded)
- ~50MB (reasonable, primary images only)

---

## Caching Strategy

### React Query Cache

**Cache Duration**: 5 minutes
- Fetches are cached for 5 minutes
- Cache automatically expires (stale data after 5 minutes)
- Users can manually refresh via pull-to-refresh

**Cache Size Limit**:
- React Query's default cache is sufficient for ~50 stations
- Cache automatically evicts old entries when full
- Last viewed stations prioritized in cache

### Offline Cache (AsyncStorage/localStorage)

**Persisted Data**:
- Last 50 stations + details
- Theme preference (dark/light mode)
- Last search queries (autocomplete suggestions)

**Cache Invalidation**:
- Mark as stale when network returns
- Fetch fresh data from API
- Update local cache
- Persist changes

---

## Validation Rules Summary

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| Station.id | string | Yes | STA-[nanoid], unique |
| Station.name | string | Yes | max 100 chars |
| Station.address | string | Yes | max 255 chars |
| Station.geometry | object | Yes | valid Point |
| Station.amenities | array | Yes | max 10 items |
| Station.operating_hours | string | Yes | "HH:MM-HH:MM" or "24/7" |
| Station.created_at | string | Yes | ISO 8601 UTC |
| Station.updated_at | string | Yes | ISO 8601 UTC |
| Charger.id | string | Yes | CHR-[nanoid], unique |
| Charger.station_id | string | Yes | references Station.id |
| Charger.charger_type | enum | Yes | CCS, CHAdeMO, AC |
| Charger.connector_count | number | Yes | 1-4 |
| Charger.availability_status | enum | Yes | available, in_use, maintenance |
| Charger.power_kw | number | Yes | 10-350 |
| Charger.is_active | boolean | Yes | default true |
| Charger.created_at | string | Yes | ISO 8601 UTC |
| Charger.updated_at | string | Yes | ISO 8601 UTC |
| StationImage.id | string | Yes | IMG-[nanoid], unique |
| StationImage.station_id | string | Yes | references Station.id |
| StationImage.url | string | Yes | HTTPS only |
| StationImage.caption | string | No | max 200 chars |
| StationImage.is_primary | boolean | Yes | default false |
| StationImage.created_at | string | Yes | ISO 8601 UTC |

---

## Data Migration Notes

**No migration needed for Phase 4**: All entities already exist in backend schemas from Phase 1-2.

**Frontend Data Types**:
- Use TypeScript interfaces for type safety
- Ensure strict mode compliance
- Validate data on receipt (prevent runtime errors)

**Error Handling**:
- If API returns unexpected format, log error and show generic UI
- Don't crash app due to invalid API response
- Provide retry button for failed requests

---

## Future Enhancements

**Phase 2** (out of scope for MVP-1):
- Station images uploaded by admins
- Image lazy loading with thumbnails
- Video reviews

**Phase 3** (out of scope for MVP-1):
- Real-time availability updates via WebSocket
- Station favorites with user account
- Charging session history

**Phase 4+** (out of scope for MVP-1):
- Multi-language station names
- Station images with gallery view
- User ratings and reviews

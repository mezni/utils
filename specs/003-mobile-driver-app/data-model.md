# Data Model: Web Driver Client

**Branch**: `004-web-driver-client` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](../004-web-driver-client/spec.md)

## Overview

This document defines the client-side data models used by the web driver client application. All server-side entities (Station, Location) are consumed via the existing Driver Service API — no new backend entities are introduced in this sprint.

## Data Flow

```text
Device GPS ──> Browser Geolocation API ──> useNearbyStations hook ──> /api/v1/nearby
                                        │                            │
                                        ▼                            ▼
                              localStorage cache ────> render StationMarker on Leaflet Map
```

## Entities

### StationMarker

Represents a charging station displayed on the map. Data is fetched from `GET /api/v1/nearby` and cached in localStorage.

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `station_id` | `string` | API | NanoID prefixed `STA_` |
| `station_name` | `string` | API | Display name on marker callout |
| `latitude` | `number` | API | WGS84 decimal degrees |
| `longitude` | `number` | API | WGS84 decimal degrees |
| `distance_meters` | `number` | API | Distance from driver's current location |
| `partner_name` | `string` | API | Operating partner display name |
| `is_private` | `boolean` | API | Whether station is a private home charger |

### MapViewport

Tracks the current visible map area for debouncing and API queries.

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `latitude` | `number` | Device GPS / Tunis default | Center latitude |
| `longitude` | `number` | Device GPS / Tunis default | Center longitude |
| `latitudeDelta` | `number` | Map viewport | Span in degrees |
| `longitudeDelta` | `number` | Map viewport | Span in degrees |
| `zoomLevel` | `number` | Map viewport | Derived from delta; threshold at 8 |
| `lastUpdated` | `number` | Timestamp | Monotonic timestamp for debounce comparison |

### CacheEntry

Persisted station data written to localStorage on every successful API response.

| Field | Type | Notes |
|-------|------|-------|
| `viewportKey` | `string` | `"{rounded_lat},{rounded_lng}"` — coordinates rounded to 2dp for privacy |
| `stations` | `StationMarker[]` | Array of station data from last successful fetch |
| `cachedAt` | `number` | Unix timestamp of when this cache entry was written |
| `viewportCenter` | `{lat: number, lng: number}` | Rounded to 2dp |

### ApiFetchState

Discriminated union controlling the map area UI rendering.

| State | Condition | UI |
|-------|-----------|-----|
| `loading` | API call in progress or no cache | ShimmerSkeleton overlay |
| `success` | API returned 2xx with stations | StationMarker pins on Leaflet map |
| `empty` | API returned 2xx with empty array | EmptyState guidance message |
| `error` | API failed (timeout/non-2xx after retries) | ErrorBoundary with Retry Connection |
| `offline` | Network unavailable, cache read | Cached markers + OfflineBanner |

## State Transitions

```text
                    ┌─────────┐
                    │ loading │
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         ┌────────┐ ┌────────┐ ┌────────┐
         │success │ │ empty  │ │ error  │
         └───┬────┘ └────────┘ └───┬────┘
             │                      │
             ▼                      │
      ┌───────────┐                │
      │  offline  │◄───────────────┘
      └───────────┘     (network drop)
             │
             ▼
      ┌───────────┐
      │  success  │  (manual refresh after reconnect)
      └───────────┘
```

## Validation Rules

| Rule | Applies To | Description |
|------|-----------|-------------|
| Latitude bounds | MapViewport | Must be within -90 to 90; Tunisia constraint 30-38°N |
| Longitude bounds | MapViewport | Must be within -180 to 180; Tunisia constraint 7-12°E |
| Zoom threshold | MapViewport | Below 4: Zoom-out overlay active |
| Debounce | MapViewport.lastUpdated | Minimum 300ms between viewport changes before API call |
| Cache rounding | CacheEntry | Coordinates rounded to 2 decimal places before storage |
| API timeout | ApiFetchState | 10s timeout on fetch; non-2xx treated as error |
| Max retries | ApiFetchState | 3 retries per manual refresh attempt |

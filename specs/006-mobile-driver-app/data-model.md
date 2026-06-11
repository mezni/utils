# Data Model: Mobile Driver App (Core UX)

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Entities

### Station

Represents a charging station displayed on the map and in the bottom sheet. Retrieved from the Driver Service.

| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `id` | `string` | UUID | Driver Service |
| `name` | `string` | Station display name | Driver Service |
| `latitude` | `number` | WGS84 latitude | Driver Service |
| `longitude` | `number` | WGS84 longitude | Driver Service |
| `distance_m` | `number \| null` | Distance from user in meters (nearby response only) | Driver Service |
| `chargers` | `Charger[]` | List of chargers at this station (detail response only) | Driver Service |
| `address` | `string \| null` | Human-readable address | Driver Service |

### Charger

A single charging unit at a station.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Charger UUID |
| `connector_type` | `'type2' \| 'ccs' \| 'chademo' \| 'wall'` | Connector standard |
| `power_kw` | `number` | Maximum power output in kW |
| `status` | `'available' \| 'occupied' \| 'offline'` | Current operational status |

### MapRegion

Represents the visible area of the map used for nearby searches.

| Field | Type | Description |
|-------|------|-------------|
| `latitude` | `number` | Center latitude |
| `longitude` | `number` | Center longitude |
| `latitudeDelta` | `number` | North-south span in degrees |
| `longitudeDelta` | `number` | East-west span in degrees |

### ClickstreamEvent

An interaction event sent to the Clickstream Service.

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | `'map_open' \| 'map_pan' \| 'map_zoom' \| 'station_click' \| 'station_view' \| 'nearby_search'` | Event category |
| `timestamp` | `string` | ISO 8601 timestamp |
| `station_id` | `string \| null` | Station UUID (for station_click, station_view) |
| `latitude` | `number \| null` | Map center latitude (for nearby_search, map_pan) |
| `longitude` | `number \| null` | Map center longitude (for nearby_search, map_pan) |
| `radius_m` | `number \| null` | Search radius in meters (for nearby_search) |

### Hook Return Types

```ts
interface UseNearbyStationsResult {
  stations: Station[];
  loading: boolean;
  error: string | null;
  refetch: (region: MapRegion) => void;
}

interface UseStationDetailResult {
  station: Station | null;
  loading: boolean;
  error: string | null;
  refetch: (id: string) => void;
}

interface UseLocationResult {
  location: { latitude: number; longitude: number } | null;
  permissionDenied: boolean;
  error: string | null;
}

interface UseClickstreamResult {
  track: (event: ClickstreamEvent) => void;
}
```

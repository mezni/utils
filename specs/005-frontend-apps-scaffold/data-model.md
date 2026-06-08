# Data Model: Frontend Apps Scaffold

> TypeScript interfaces consumed by the three frontend apps. These reflect the API contracts of driver-service and admin-service.

## Shared Types

```typescript
// A charging station as returned by driver-service GET /api/v1/stations/nearby
interface Station {
  id: string;              // e.g. "STN-abc123"
  name: string;            // Station display name
  latitude: number;        // WGS84 latitude
  longitude: number;       // WGS84 longitude
  address: string;         // Human-readable address
  available_chargers: number; // Count of currently available chargers
  total_chargers: number;  // Total chargers at this station
}

// Geographic bounding box for map view
interface Bounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

// Coordinates for location
interface Coordinates {
  latitude: number;
  longitude: number;
}
```

## Driver Web Types

```typescript
// State for the station map component
interface StationMapState {
  stations: Station[];
  loading: boolean;
  error: string | null;
}

// Leaflet-compatible marker data
interface StationMarker {
  position: [number, number];  // [lat, lng] tuple for Leaflet
  station: Station;
}
```

## Driver Mobile Types

```typescript
// Location state for the mobile app
interface LocationState {
  status: 'granted' | 'denied' | 'undetermined';
  coordinates: Coordinates | null;
}

// Map region for react-native-maps
interface MapRegion {
  latitude: number;
  longitude: number;
  latitudeDelta: number;
  longitudeDelta: number;
}
```

## Dashboard Types

```typescript
// Sidebar navigation structure
type NavItemId = 'overview' | 'partners' | 'stations' | 'chargers';

interface NavItem {
  id: NavItemId;
  label: string;           // Display text (English in Sprint 1.5)
  path: string;            // Route path e.g. "/partners"
  icon?: string;           // Icon identifier (future use)
}

// Overview page stat card
interface StatCard {
  label: string;           // e.g. "Total Partners"
  value: number;           // e.g. 3
  icon?: string;           // Icon identifier (future use)
}

// Admin service entity types (used for Dashboard stat counts)
interface Partner {
  id: string;              // e.g. "PRT-abc"
  name: string;
  created_at: string;
}

// Dashboard overview state
interface DashboardOverview {
  totalPartners: number;
  totalStations: number;
  totalChargers: number;
  loading: boolean;
  error: string | null;
}
```

## Validation Rules

| Field | Rule | Applies To |
|-------|------|------------|
| Station coordinates | Must be valid WGS84 lat/lng (-90 to 90, -180 to 180) | All apps |
| Station ID | Must match pattern `STN-[a-z0-9]+` | All apps |
| Latitude/longitude | Parsed as float from API response | All apps |
| Available chargers | Integer >= 0 | All apps |
| Total chargers | Integer >= 0, must be >= available_chargers | All apps |
| Map center | Default to Tunisia (34.0, 9.0) zoom 7 | Driver Web |
| Mobile fallback coords | Tunis (36.8065, 10.1815) | Driver Mobile |

## State Transitions

### Driver Web — Station Loading

```
idle → loading → success (stations loaded)
                → error (API unavailable)
```

### Driver Mobile — Location Permission

```
undetermined → granted → coordinates available
             → denied → fallback to default coords
```

### Dashboard — Overview Stats Loading

```
idle → loading → loaded (counts displayed)
                → error (fallback message)
```

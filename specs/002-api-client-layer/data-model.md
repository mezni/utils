# Data Model: API Client Layer

## Entities

### ApiClient

The configured client instance exposing typed methods.

| Field | Type | Description |
|-------|------|-------------|
| `baseUrl` | `string` | Base URL for the driver-service backend |

**Methods**:
- `getStations()` → `Promise<Station[]>`
- `getStationById(id)` → `Promise<Station>`
- `getNearbyStations(lat, lng, radius)` → `Promise<Station[]>`

### Station (from @bm/types)

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Station identifier (format: STA-xxx or OSM-derived) |
| `name` | `string` | Human-readable station name |
| `status` | `"active" \| "maintenance"` | Operational status |
| `latitude` | `number` | WGS84 latitude |
| `longitude` | `number` | WGS84 longitude |
| `location` | `object` | GEOGRAPHY POINT (PostGIS) |
| `distance` | `number \| null` | Computed distance from query origin (nearby only) |

### ApiError

Typed error thrown on failed requests.

| Field | Type | Description |
|-------|------|-------------|
| `status` | `number \| null` | HTTP status code (null for network errors) |
| `message` | `string` | Human-readable error description |
| `data` | `unknown \| null` | Optional response body for debugging |

**Extends**: `Error`

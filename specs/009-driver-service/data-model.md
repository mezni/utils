# Data Model: Driver Service

**Phase**: Phase 1 — Response types for Sprint 2.3

**Date**: 2026-06-09

## Response Types

### StationSummary (nearby, markers, search)

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` | Station identifier (NanoID) |
| `name` | `String` | Station display name |
| `address` | `String?` | Physical address |
| `latitude` | `f64` | WGS84 latitude |
| `longitude` | `f64` | WGS84 longitude |
| `availability_status` | `String?` | Current availability: `available`, `partial`, `unavailable` |

### StationNearby (extends StationSummary, nearby only)

| Field | Type | Description |
|-------|------|-------------|
| `distance_meters` | `f64` | Distance from query point in meters |

### StationDetail

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` | Station identifier (NanoID) |
| `name` | `String` | Station display name |
| `address` | `String?` | Physical address |
| `latitude` | `f64` | WGS84 latitude |
| `longitude` | `f64` | WGS84 longitude |
| `chargers` | `Vec<ChargerInfo>` | List of chargers at this station |

### ChargerInfo

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` | Charger identifier (NanoID) |
| `connector_type` | `String` | Connector standard: `type2`, `type3`, `ccs`, `chademo` |
| `power_kw` | `f64` | Power rating in kilowatts |
| `status` | `String` | Operational status: `available`, `in_use`, `maintenance`, `offline` |

### ApiError

| Field | Type | Description |
|-------|------|-------------|
| `error` | `ErrorBody` | Error details |

### ErrorBody

| Field | Type | Description |
|-------|------|-------------|
| `code` | `String` | Machine-readable error code |
| `message` | `String` | Human-readable error description |

### HealthResponse

| Field | Type | Description |
|-------|------|-------------|
| `status` | `String` | Always `"ok"` |
| `version` | `String` | Service version from Cargo.toml |

### ReviewsStubResponse

| Field | Type | Description |
|-------|------|-------------|
| `station_id` | `String` | The requested station ID |
| `message` | `String` | Placeholder message: `"Reviews are coming soon"` |

## Relationships

```
Station 1───* Charger
Station 1───* StationAvailability (historical)
Station *───1 Partner (visibility filter)
```

## Validation Rules

- `latitude`: BETWEEN -90 AND 90 (enforced by DB CHECK)
- `longitude`: BETWEEN -180 AND 180 (enforced by DB CHECK)
- `connector_type`: One of `type2`, `type3`, `ccs`, `chademo` (enforced by DB CHECK)
- `power_kw`: > 0 (enforced by DB CHECK)
- `availability_status`: One of `available`, `partial`, `unavailable`
- `charger.status`: One of `available`, `in_use`, `maintenance`, `offline`

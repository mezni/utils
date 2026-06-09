# Data Model: Driver Mobile App

## Entities

### Partner

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Primary key |
| `name` | `string` | Partner display name |
| `is_verified` | `boolean` | Partner verification status |
| `is_live` | `boolean` | Partner live status |
| `is_active` | `boolean` | Partner active status |

**Visibility rule**: A station is shown on the map only when the owning partner has `is_verified=true AND is_live=true AND is_active=true`.

**Source**: `GET /api/partners`

### Station

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Primary key |
| `partner_id` | `string` | FK → Partner.id |
| `name` | `string` | Station name |
| `address` | `string` | Street address |
| `latitude` | `number` | Latitude (WGS84) |
| `longitude` | `number` | Longitude (WGS84) |

**Computed fields** (client-side):
- `availableCount`: number of chargers with `status === 'available'`
- `totalChargers`: total number of chargers at the station

**Source**: `GET /api/stations`

### Charger

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Primary key |
| `station_id` | `string` | FK → Station.id |
| `connector_type` | `string` | Connector type (e.g., "Type 2", "CCS", "CHAdeMO") |
| `power_kw` | `number` | Power rating in kW |
| `status` | `string` | Operational status: `available`, `in_use`, `maintenance`, `offline` |

**Status colors**:
- `available`: green (`#00E676`)
- `in_use`: orange/amber (`#FF9800`)
- `maintenance`: gray (`#9E9E9E`)
- `offline`: red (`#EF4444`)

**Source**: `GET /api/chargers` (all), `GET /api/chargers?station_id={id}` (by station)

## Relationships

```
Partner (1) ──── (N) Station (1) ──── (N) Charger
```

- A Partner owns many Stations
- A Station belongs to exactly one Partner
- A Station has many Chargers
- A Charger belongs to exactly one Station

## State Transitions

Not applicable — the mobile app is read-only (no CRUD). State changes happen on the server via the Dashboard app.

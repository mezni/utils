# Data Model: BorneMap Platform Scaffold

## StationHub

Represents a physical EV charging station location.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | YES | nanouuid with `stn-` prefix, e.g. `stn-e3b0c442` |
| name | string | YES | Human-readable station name, e.g. "LES BERGES DU LAC 2 HUB" |
| provider_id | string | YES | nanouuid referencing the owning provider, e.g. `prv-k9x2m47a` |
| provider_name | string | YES | Display name of the provider, e.g. "TotalEnergies Tunisia" |
| latitude | f64 | YES | WGS 84 latitude (SRID 4326) |
| longitude | f64 | YES | WGS 84 longitude (SRID 4326) |
| status | string | YES | `"Available"` or `"Occupied"` |
| chargers | Charger[] | YES | List of charging units at this station |
| updated_at | datetime | YES | ISO 8601 UTC timestamp of last status change |

**Constraints**:
- id MUST match `^stn-[a-f0-9]{8}$`
- Coordinates MUST be valid WGS 84 (lat: -90 to 90, lon: -180 to 180)
- At least one charger MUST be present

## Charger

Represents an individual charging unit at a station.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | YES | nanouuid with `chg-` prefix, e.g. `chg-7b2a19f4` |
| plug_type | string | YES | Connector standard, e.g. `CCS2`, `CHAdeMO`, `Type2` |
| power_output | u32 | YES | Maximum power in kilowatts (kW) |
| status | string | YES | `"Available"` or `"Occupied"` |

**Constraints**:
- id MUST match `^chg-[a-f0-9]{8}$`
- power_output MUST be positive (>= 1 kW)

## Provider

Represents the organization operating one or more stations.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | YES | nanouuid with `prv-` prefix, e.g. `prv-k9x2m47a` |
| name | string | YES | Provider display name, e.g. "TotalEnergies Tunisia" |

**Constraints**:
- id MUST match `^prv-[a-f0-9]{8}$`

## Relationships

- Provider (1) ──< (N) StationHub: A provider operates many stations.
- StationHub (1) ──< (N) Charger: A station contains many chargers.

## Status Lifecycle

For the MVP scaffold, station and charger status values are static mock data:
- `"Available"` / `"Occupied"` — assigned during mock data generation
- No dynamic state transitions in the scaffold phase
- Future: real-time telemetry will drive status updates

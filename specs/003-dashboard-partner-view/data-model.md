# Data Model: Dashboard Partner View

This feature does not introduce new entities. It consumes the same four API resources from Sprint 1.1 with partner-scoped filtering.

## Scoped Queries

### Stations (scoped to partner)

```
GET /api/stations?partner_id=PRT001
```

Returns only stations where `partner_id` matches the selected partner.

### Chargers (scoped to partner's stations)

Two-step query:
1. `GET /api/stations?partner_id=PRT001` → get partner's station IDs
2. `GET /api/chargers?station_id=STN001&station_id=STN002&...` → get chargers for those stations

### Station Availability (scoped to partner's stations)

```
GET /api/station_availability?station_id=STN001&station_id=STN002&...
```

Returns all availability records for the partner's stations. Current status is the latest `updated_at` per station.

### Create Availability Record

```
POST /api/station_availability
Content-Type: application/json

{
  "station_id": "STN001",
  "status": "available" | "partial" | "unavailable",
  "updated_by": "USR-PRT001",
  "updated_at": "2026-06-09T12:00:00Z"
}
```

## Frontend State Shapes

### Partner Overview State

```typescript
interface PartnerOverviewState {
  partner: Partner | null;
  stations: StationRow[];
  chargers: Charger[];
  stats: {
    stationCount: number;
    chargerCount: number;
    availableChargerCount: number;
  };
}
```

### Availability Toggle State

```typescript
interface AvailabilityState {
  stationId: string;
  currentStatus: 'available' | 'partial' | 'unavailable' | 'unknown';
  toggling: boolean; // true while POST is in flight
}
```

## State Transitions

| Entity | Action | Before | After |
|--------|--------|--------|-------|
| Station Availability | Toggle Available | any status | available |
| Station Availability | Toggle Partial | any status | partial |
| Station Availability | Toggle Unavailable | any status | unavailable |
| Station | Add (partner) | — | Created with partner_id locked |
| Station | Edit (partner) | old values | updated values |
| Station | Delete (partner) | exists | removed |
| Charger | Add (partner) | — | Created with station_id |
| Charger | Edit (partner) | old values | updated values |
| Charger | Delete (partner) | exists | removed |

## Derived / Computed Fields

- **Own stations count**: `GET /api/stations?partner_id={id}` → array length
- **Own chargers count**: sum of chargers across partner's station IDs
- **Available chargers count**: chargers where `status === 'available'`
- **Current availability**: latest `station_availability` record by `updated_at` per station

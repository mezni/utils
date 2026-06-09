# Driver Mobile App Contracts

## Overview

The Driver Mobile App exposes two screens (Map, Station Detail) to the user. This document defines the contracts between screens (navigation params) and to external systems (API).

## Screen Navigation Contract

### Stack Navigator

```
NativeStackNavigator
├── "Map"              → MapScreen (no params)
└── "StationDetail"    → StationDetailScreen { stationId: string }
```

### Route Param Types

```typescript
export type RootStackParamList = {
  Map: undefined;
  StationDetail: { stationId: string };
};
```

### Screen Behaviors

| Screen | Entry | Exit | Back Behavior |
|--------|-------|------|---------------|
| Map | App launch | → StationDetail (via callout tap) | N/A (root) |
| StationDetail | Callout tap on map | → Map (via back button) | Returns to map at the same position |

## API Contract

Same as json-server API used by Dashboard and Driver Web:

```
GET /api/partners       → Partner[]
GET /api/stations       → Station[]
GET /api/chargers       → Charger[]
GET /api/stations/:id   → Station
GET /api/chargers?station_id=:id  → Charger[]
```

See `data-model.md` for entity definitions.

## Data Fetching Contract

### MapScreen

Fetches on mount via `@tanstack/react-query`:
- `useQuery(['partners'], () => list<Partner>('partners'))`
- `useQuery(['stations'], () => list<Station>('stations'))`
- `useQuery(['chargers'], () => list<Charger>('chargers'))`

Computes visible stations (partner filter + availability count) client-side.

### StationDetailScreen

Fetches on mount:
- `useQuery(['station', stationId], () => get<Station>('stations', stationId))`
- `useQuery(['chargers', stationId], () => list<Charger>('chargers', { station_id: stationId }))`

## Error Contract

| Scenario | User Experience |
|----------|----------------|
| API unreachable | Error message with activity indicator hidden; no crash |
| Loading | Activity indicator visible |
| Empty data | Map: empty terrain (no crash); Detail: "No chargers" message |
| Location denied | Map centered on Tunisia; no error shown |

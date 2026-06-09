# Data Model: Driver Web App

This feature does not introduce new entities. It consumes the same three API resources (partners, stations, chargers) with client-side filtering.

## Consumed Entities

### Partner (from API)

```typescript
interface Partner {
  id: string;
  name: string;
  is_verified: boolean;
  is_live: boolean;
  is_active: boolean;
}
```

Used for visibility filtering.

### Station (from API)

```typescript
interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
}
```

Used for map markers and detail display.

### Charger (from API)

```typescript
interface Charger {
  id: string;
  station_id: string;
  connector_type: string;
  power_kw: number;
  status: 'available' | 'in_use' | 'maintenance' | 'offline';
}
```

Used for available_count computation and detail display.

## Computed / Derived Types

```typescript
interface VisibleStation extends Station {
  availableCount: number;
  totalChargers: number;
}

interface MarkerData {
  station: VisibleStation;
  position: [number, number]; // [lat, lng]
  color: 'green' | 'red';
}
```

## Computed Fields

- **Visible stations**: Stations where `partner.is_verified && partner.is_live && partner.is_active` all true
- **Available count**: `chargers.filter(c => c.status === 'available').length` per station
- **Total chargers**: `chargers.filter(c => c.station_id === station.id).length` per station
- **Marker color**: Green if `availableCount > 0`, red if `availableCount === 0`
- **Distance from center**: Haversine formula result in km from Tunisia center (33.8869, 9.5375)

## State Transitions

| Trigger | Before | After |
|---------|--------|-------|
| Map mounts | empty state | fetch partners + stations + chargers |
| API data arrives | loading | visible stations computed, markers rendered |
| User pans/zooms | position A | position B (state tracked in component) |
| User clicks marker | popup closed | popup open with station info |
| User clicks View Detail | map screen | station detail screen |
| User clicks back | detail screen | map screen with previous position restored |
| API error | any | error state with retry |
| Retry clicked | error | loading → data |

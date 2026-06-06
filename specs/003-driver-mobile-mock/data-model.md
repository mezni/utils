# Data Model: Driver Mobile App with Mock Data

## Entities

All entities use the same shape as the Driver Web App (Sprint 1.2). Types are defined in `apps/driver-mobile/src/types/index.ts`.

### Station

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | NanoID format: `STN-XXX` |
| `name` | `string` | Station name (Arabic or French) |
| `address` | `string` | Tunisian street address |
| `coordinates` | `{ lat: number; lng: number }` | GPS coordinates |
| `distance` | `number` | Distance in km from user |
| `chargerCount` | `number` | Total chargers at station |
| `availableCount` | `number` | Currently available chargers |
| `availability` | `'available' \| 'unavailable'` | Overall station status |
| `rating` | `number` | Average star rating (0-5) |
| `reviewCount` | `number` | Number of reviews |
| `imageUrl` | `string` | Station image URL (empty string for mock) |

### Charger

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | NanoID format: `CHG-XXX` |
| `stationId` | `string` | Foreign key to Station.id |
| `connectorType` | `'Type2' \| 'CCS' \| 'CHAdeMO'` | Connector standard |
| `powerKw` | `number` | Power output in kW |
| `availability` | `'available' \| 'unavailable'` | Current charger status |
| `pricePerKwh` | `number` | Price per kWh in TND |
| `lastMaintained` | `string` | ISO date of last maintenance |

### Review

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | NanoID format: `REV-XXX` |
| `stationId` | `string` | Foreign key to Station.id |
| `authorName` | `string` | Reviewer display name |
| `rating` | `number` | Star rating (1-5) |
| `text` | `string` | Review content (Arabic or French) |
| `date` | `string` | ISO date of review |
| `language` | `'ar' \| 'fr' \| 'en'` | Content language |

### DriverUser

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | NanoID format: `USR-XXX` |
| `name` | `string` | User's full name |
| `email` | `string` | Email address |
| `phone` | `string` | Phone number |
| `avatarUrl` | `string` | Avatar image URL |
| `favoriteStationIds` | `string[]` | Array of Station IDs |
| `language` | `'ar' \| 'fr' \| 'en'` | Preferred language |

### FilterState

| Field | Type | Description |
|-------|------|-------------|
| `chargerType` | `'all' \| 'Type2' \| 'CCS' \| 'CHAdeMO'` | Connector type filter |
| `availability` | `'all' \| 'available'` | Availability filter |
| `searchQuery` | `string` | Debounced search text |

## Data Relationships

```
Station (1) ──── (N) Charger
Station (1) ──── (N) Review
DriverUser (N) ──── (N) Station  (via favoriteStationIds)
```

## Mock Data Summary

- **15 stations** — Tunisian addresses, mixed Arabic/French names
- **~50 chargers** — 2-4 per station, mixed Type2/CCS/CHAdeMO
- **~60 reviews** — 3-5 per station, mixed Arabic/French content
- **1 DriverUser** — mock logged-in user profile

## Future Considerations

In Phase 5, these mock types will be replaced by API response types. The shape is designed to match the expected API contract:
- `Station` maps to `GET /api/stations` response
- `Charger` maps to `GET /api/stations/:id/chargers` response
- `Review` maps to `GET /api/stations/:id/reviews` response
- `DriverUser` maps to `GET /api/users/me` response

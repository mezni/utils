# Data Model: Driver Web App (Mock Data)

## Overview

This document defines the TypeScript types and interfaces for the mock data used by the Driver Web App. These types will be the foundation for the real API integration in Phase 5.

## Type: Station

Represents an EV charging station location.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique identifier, NanoID prefix `STN-` |
| `name` | `string` | Station display name (e.g., "Station de recharge Ariana") |
| `address` | `string` | Tunisian street address |
| `coordinates` | `{ lat: number; lng: number }` | GPS coordinates in WGS84 |
| `distance` | `number` | Distance from user's location in km |
| `chargerCount` | `number` | Total number of charging points |
| `availableCount` | `number` | Number of currently available chargers |
| `availability` | `'available' \| 'unavailable'` | Overall station status |
| `rating` | `number` | Average star rating (0–5, 1 decimal) |
| `reviewCount` | `number` | Number of reviews |
| `imageUrl` | `string` | Station photo URL (placeholder for mock) |

## Type: Charger

Represents an individual charging point at a station.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique identifier, NanoID prefix `CHG-` |
| `stationId` | `string` | Parent station ID (`STN-*`) |
| `connectorType` | `'Type2' \| 'CCS' \| 'CHAdeMO'` | Connector standard |
| `powerKw` | `number` | Power output in kW (3.7–350) |
| `availability` | `'available' \| 'unavailable'` | Current status |
| `pricePerKwh` | `number` | Price per kWh in TND |
| `lastMaintained` | `string` | ISO 8601 date string of last maintenance |

## Type: Review

Represents a user review for a station.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique identifier, NanoID prefix `REV-` |
| `stationId` | `string` | Station ID (`STN-*`) |
| `authorName` | `string` | Reviewer display name |
| `rating` | `number` | Star rating (1–5, integer) |
| `text` | `string` | Review content (Arabic or French) |
| `date` | `string` | ISO 8601 date string |
| `language` | `'ar' \| 'fr' \| 'en'` | Review language |

## Type: DriverUser

Represents a mock driver user profile.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique identifier, NanoID prefix `USR-` |
| `name` | `string` | Full display name |
| `email` | `string` | Email address |
| `phone` | `string` | Tunisian phone number |
| `avatarUrl` | `string` | Profile photo URL (placeholder) |
| `favoriteStationIds` | `string[]` | Array of `STN-*` IDs |
| `language` | `'ar' \| 'fr' \| 'en'` | Preferred language |

## Type: FilterState

Represents the active filter selections.

| Field | Type | Description |
|-------|------|-------------|
| `chargerType` | `'all' \| 'Type2' \| 'CCS' \| 'CHAdeMO'` | Selected connector type filter |
| `availability` | `'all' \| 'available'` | Availability filter |
| `searchQuery` | `string` | Current search text |

## Mock Data Specifications

- **Stations**: 15 stations across Tunisian cities (Tunis, Ariana, Ben Arous, Sfax, Sousse, Nabeul, Bizerte)
- **Chargers**: 2–4 chargers per station, mixed connector types, 3.7–350 kW
- **Reviews**: 3–5 reviews per station, mixed Arabic and French content
- **Users**: 1 mock user with some pre-saved favorites

### ID Format

```
Station:  STN-001 through STN-015
Charger:  CHG-{stationNum}-{index} (e.g., CHG-001-1, CHG-001-2)
Review:   REV-{stationNum}-{index} (e.g., REV-001-1, REV-001-2)
User:     USR-001
```

### Data Relationships

```
Station 1──N Charger
Station 1──N Review
User    N──M Station (via favoriteStationIds)
```

## Validation Rules

- Rating: integer 1–5
- Power: 3.7–350 kW
- Coordinates: valid lat/lng within Tunisia bounds (lat: 30–38, lng: 7–12)
- Phone: Tunisian format (+216 XX XXX XXX)
- Text: non-empty for reviews with content

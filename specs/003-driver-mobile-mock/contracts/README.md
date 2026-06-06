# Mock Data Contracts — Driver Mobile App

This directory documents the shape and source of mock data used in Sprint 1.3.

## Source Files

All mock data lives in `apps/driver-mobile/src/mocks/`:

| File | Entity | Count | Notes |
|------|--------|-------|-------|
| `stations.ts` | `Station` | 15 | Tunisian addresses, mixed Arabic/French names |
| `chargers.ts` | `Charger` | ~50 | 2–4 per station, mixed connector types |
| `reviews.ts` | `Review` | ~60 | 3–5 per station, Arabic and French content |
| `users.ts` | `DriverUser` | 1 | Mock logged-in user |

## Data Shape

All entities match the types defined in `apps/driver-mobile/src/types/index.ts`. See [data-model.md](../data-model.md) for complete field definitions.

## Key Conventions

- **IDs**: NanoID-style prefixes (`STN-`, `CHG-`, `REV-`, `USR-`)
- **Coordinates**: Tunisian GPS coordinates (lat/lng around Tunis, Ariana, La Marsa, Sidi Bou Said, Carthage)
- **Names**: Mixed Arabic and French station names
- **Review content**: Realistic text in Arabic (`language: 'ar'`) and French (`language: 'fr'`)
- **Chargers**: Type2 (22kW), CCS (50-350kW), CHAdeMO (50kW) connector types
- **Prices**: 0.200–0.800 TND per kWh
- **Dates**: ISO format strings in 2025-2026 range

## Mock User

```ts
{
  id: 'USR-001',
  name: 'Ahmed Ben Ali',
  email: 'ahmed.benali@example.com',
  phone: '+216 50 123 456',
  avatarUrl: '',
  favoriteStationIds: ['STN-001', 'STN-003', 'STN-007'],
  language: 'fr'
}
```

## Reuse Strategy

These mock files are copies of the Sprint 1.2 web mock data with identical shapes. When the API is ready in Phase 5, these files will be replaced by API client calls. The type interfaces remain stable.

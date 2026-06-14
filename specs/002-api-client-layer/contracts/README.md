# Contracts: API Client Layer

The `@bm/api-client` package exposes the following public contract.

## Factory

```typescript
function createApiClient(baseUrl: string): ApiClient
```

## ApiClient

```typescript
interface ApiClient {
  getStations(): Promise<Station[]>
  getStationById(id: string): Promise<Station>
  getNearbyStations(lat: number, lng: number, radius: number): Promise<Station[]>
}
```

## Station (from @bm/types)

```typescript
interface Station {
  id: string
  name: string
  status: 'active' | 'maintenance'
  latitude: number
  longitude: number
  location: { type: 'Point'; coordinates: [number, number] }
  distance?: number
}
```

## ApiError

```typescript
class ApiError extends Error {
  status: number | null
  data: unknown | null
}
```

## Parameter Validation

- `lat`: MUST be in range [-90, 90]
- `lng`: MUST be in range [-180, 180]
- `radius`: MUST be > 0 (meters)
- `id`: MUST be a non-empty string

# Quickstart: @bm/api-client

## Installation

The package is available as a workspace dependency:

```json
{
  "dependencies": {
    "@bm/api-client": "*"
  }
}
```

## Usage

```typescript
import { createApiClient } from '@bm/api-client'

const client = createApiClient('http://localhost:3000')

// Fetch all stations
const stations = await client.getStations()

// Fetch station by ID
const station = await client.getStationById('STA-001')

// Fetch nearby stations (lat, lng, radius in meters)
const nearby = await client.getNearbyStations(36.8, 10.18, 5000)

// Error handling
try {
  const station = await client.getStationById('invalid-id')
} catch (error) {
  if (error instanceof ApiError) {
    console.error(`API error ${error.status}: ${error.message}`)
  }
}
```

## Configuration

Pass the driver-service base URL to `createApiClient`. In web apps this typically comes from an environment variable; in mobile apps from runtime config.

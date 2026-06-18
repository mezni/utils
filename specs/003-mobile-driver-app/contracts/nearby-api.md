# Contract: Nearby Stations API (Web Client)

**Branch**: `004-web-driver-client` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](../004-web-driver-client/spec.md)

## Overview

The web driver client communicates with the existing Driver Service `/api/v1/nearby` endpoint. This contract documents the request/response format as consumed by the web client.

## Base URL

| Environment | Base URL | Source |
|-------------|----------|--------|
| Development | `http://localhost:3001` | Direct to driver-service port 3001 |
| Production | `https://bornemap-api.example.com` | Via Traefik on production |

## Endpoint

### GET /api/v1/nearby

Fetches charging stations near a given location within a radius.

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `lat` | `number` | Yes | — | Latitude in WGS84 decimal degrees |
| `lng` | `number` | Yes | — | Longitude in WGS84 decimal degrees |
| `radius` | `number` | Yes | `10000` | Search radius in meters (fixed at 10km) |

#### Request Example

```
GET /api/v1/nearby?lat=36.8065&lng=10.1815&radius=10000
```

#### Success Response (200 OK)

```json
{
  "stations": [
    {
      "station_id": "STA_001",
      "station_name": "Tunis Centre",
      "latitude": 36.8005,
      "longitude": 10.181,
      "distance_meters": 105.09,
      "is_private": false,
      "partner_name": "BorneMap Tunisia"
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `stations` | `array` | Ordered by `distance_meters` ascending. Empty `[]` if no stations within radius. |

#### Error Responses

| Status Code | Body | Description |
|-------------|------|-------------|
| 400 | `{"error": "Latitude must be between -90 and 90"}` | Invalid latitude |
| 400 | `{"error": "Longitude must be between -180 and 180"}` | Invalid longitude |
| 400 | `{"error": "Radius must be between 1 and 200000 meters"}` | Invalid radius |
| 500 | `{"error": "Failed to query nearby stations"}` | Internal server error |

### GET /health

Health check endpoint used for debugging connectivity.

#### Response (200 OK)

```json
{
  "status": "ok"
}
```

#### Response (503 Service Unavailable)

```json
{
  "status": "degraded"
}
```

## Client Configuration

The `API_BASE_URL` is set via environment variable or runtime config:

```typescript
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:3001';
```

### Via `.env` file:

```env
VITE_API_BASE_URL=http://localhost:3001
```

### Via browser config (for production):

```json
// vite.config.js
export default defineConfig({
  define: {
    import.meta.env.VITE_API_BASE_URL: JSON.stringify('https://api.bornemap.com')
  }
})
```

## Web Client Behavior

| Scenario | Behavior |
|----------|----------|
| Network available | Fetch from API, render markers, update localStorage cache |
| Network unavailable + cache exists | Read localStorage, render cached markers, show OfflineBanner |
| Network unavailable + no cache | Show ErrorBoundary with "Retry Connection" |
| API timeout (>10s) | Show ErrorBoundary with "Retry Connection" |
| API returns 4xx | Show ErrorBoundary (driver cannot fix bad request) |
| API returns 5xx | Retry up to 3 times, then show ErrorBoundary |
| Manual refresh | Clear existing state, re-fetch from API |

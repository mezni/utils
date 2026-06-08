# Admin Service API Contract

> Contract between the Dashboard app and the Admin Service backend.

## Base URL

- **Development**: `http://localhost:3002/api/v1`
- **Dashboard Proxy**: `/api/v1` (Vite proxy → `http://localhost:3002`)
- **Production**: Via Traefik routing

## Endpoints

### Health Check

```
GET /api/v1/health
```

**Response 200**:
```json
{
  "status": "ok",
  "service": "admin-service",
  "db": "ok"
}
```

### List Partners

```
GET /api/v1/partners
```

**Response 200**:
```json
[
  {
    "id": "PRT-abc123",
    "name": "ENIM Charging",
    "created_at": "2026-06-07T12:00:00Z"
  }
]
```

### List Stations

```
GET /api/v1/stations
```

**Response 200**:
```json
[
  {
    "id": "STN-abc123",
    "name": " Station",
    "partner_id": "PRT-abc123",
    "latitude": 36.8188,
    "longitude": 10.1657,
    "address": " Tunis",
    "created_at": "2026-06-07T12:00:00Z"
  }
]
```

### List Chargers

```
GET /api/v1/chargers
```

**Response 200**:
```json
[
  {
    "id": "CHG-abc123",
    "station_id": "STN-abc123",
    "status": "available",
    "created_at": "2026-06-07T12:00:00Z"
  }
]
```

## Dashboard Usage

For the Sprint 1.5 Overview page, the Dashboard fetches:
1. `GET /api/v1/partners` → count partners (length of array)
2. `GET /api/v1/stations` → count stations
3. `GET /api/v1/chargers` → count chargers

All three calls happen in parallel on page load.

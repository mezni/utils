# Sprint 001 — Driver API Contracts

Base URL: `/api/v1/driver`

## GET /health
```json
{ "status": "ok", "database": "connected", "timestamp": "2026-06-20T12:00:00Z" }
```

## GET /nearby?lat=36.8065&lon=10.1815&radius=10000

Returns stations sorted by distance with power tier and availability.

## GET /stations/:id

Returns full station detail with charger and connector breakdown.

## PostgreSQL Function: find_nearby_stations(lat, lon, radius, limit)

Queries only `mv_stations_geo` using `ST_DWithin` + `ST_Distance`.

See `specs/001-ev-charging-foundation/contracts/README.md` for full contract details.

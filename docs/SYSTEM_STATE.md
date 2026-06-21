# BorneMap — System State
**Version:** 0.1.0
**Date:** June 2026
**Status:** Sprint 001 — Implementation Phase

---

## Deployment

| Component | Status | Port |
|---|---|---|
| PostgreSQL 16 + PostGIS | Configured | 5432 |
| Traefik Gateway | Configured | 80 |
| driver-service (Rust) | Implemented | 3001 |
| Web App (React + Leaflet) | Implemented | 5173 |

## Services

### driver-service (:3001)

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/driver/nearby` | GET | Nearby stations query (lat, lng, radius) |
| `/api/v1/driver/health` | GET | Service + DB health check |

## Schema

### inventory schema
- access_types, data_sources, connector_types, current_types, connector_statuses, station_statuses, charger_statuses (lookup)
- partners (PAR- nanoid)
- partner_users
- stations (STA- nanoid, GEOGRAPHY, HSTORE)
- chargers (CHR- nanoid)
- connectors (CON- nanoid)

### gis schema
- osm_charging_stations_temp

### Functions
- `inventory.find_nearby_stations(lat, lng, radius_meters)` — spatial query
- `inventory.sync_osm_charging_stations()` — OSM normalization

## Data Flow
```
OSM Overpass API → import.sh → gis.osm_charging_stations_temp
  → sync_osm_charging_stations() → inventory.stations
  → find_nearby_stations() → driver-service API → Web Map UI
```

## Known Issues
- Cross-schema write (gis → inventory) without admin-service mediation (bootstrap exception)
- OSM import requires manual execution of `platform/scripts/import.sh`

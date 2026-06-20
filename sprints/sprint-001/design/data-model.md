# Sprint 001 — Design Document

## 6-Story Architecture

```
Phase 1: Infrastructure (Docker + PostGIS)
  ↓
Phase 2: OSM Ingestion (fetch → parse → staging table)
  ↓
Phase 3: Inventory Schema (partners → stations → chargers → connectors)
  ↓
Phase 4: Sync + Nearby (staging → inventory + spatial queries)
  ↓
Phase 5: Driver API (health check + nearby endpoint)
  ↓
Phase 6: Web App (map view + markers)
```

## Entity Hierarchy

```
Partner (PAR-) ──→ Station (STA-) ──→ Charger (CHR-) ──→ Connector (CON-)
```

All entities use typed nanoid(12) identifiers and have strict FK cascade:

- ON DELETE CASCADE on Station → Charger → Connector
- No orphan records allowed (constitution principle IV)

## Spatial Query Pattern

```
mv_stations_geo (read-only view)
    ↓ ST_DWithin + ST_Distance
find_nearby_stations(lat, lon, radius, limit)
    ↓ called by driver-api
GET /nearby API endpoint
```

**Design rule**: Never query base tables directly for location data (constitution principle II).

## Power Tier Classification

- `ultra_fast`: max_power_kw ≥ 150kW
- `fast`: max_power_kw ≥ 50kW
- `medium`: max_power_kw ≥ 22kW
- `slow`: max_power_kw < 22kW

Computed in `mv_stations_geo` as a computed column.

## Idempotent Import Pattern

```sql
INSERT INTO stations (...) SELECT ... FROM staging
ON CONFLICT (osm_id, ST_DWithin(...)...) DO UPDATE SET ...
```

Tracks each import in `sync_jobs` table with status, result counts, timestamps.

See `specs/001-ev-charging-foundation/data-model.md` for full schemas.

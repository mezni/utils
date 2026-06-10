# Verification Contracts: MVP-2 Hardening

**Branch**: `012-mvp2-hardening` | **Date**: 2026-06-09

## Overview

This sprint does not introduce new endpoints. All contracts below document the expected behavior of existing endpoints under hardening verification scenarios.

## Endpoint Verification Matrix

### Admin Service (port 8081)

| Endpoint | Method | Verification Required | Expected Result |
|----------|--------|-----------------------|-----------------|
| `/api/health` | GET | Returns healthy | `{"status":"ok"}` with 200 |
| `/api/partners` | POST | Create partner with default flags | `is_active=true, is_verified=false, is_live=false` |
| `/api/partners/{id}` | GET | CRUD after creation | 200 with partner data |
| `/api/partners/{id}` | PATCH | Set `is_live=true` | 200, is_live reflected in response |
| `/api/partners/{id}/verify` | PATCH | Verify partner | 200, `is_verified=true` |
| `/api/partners/{id}/deactivate` | PATCH | Deactivate partner | 200, `is_active=false` |
| `/api/partners/{id}/reactivate` | PATCH | Reactivate partner | 200, `is_active=true` |
| `/api/stations` | POST | Create station for partner | 201 with station data |
| `/api/stations/{id}` | GET | CRUD after creation | 200 with station data |
| `/api/stations/{id}` | DELETE | Delete station | 204 No Content |
| `/api/chargers` | POST | Create charger for station | 201 with charger data |
| `/api/chargers/{id}` | GET | CRUD after creation | 200 with charger data |
| `/api/chargers/{id}` | DELETE | Delete charger | 204 No Content |

### Driver Service (port 8080)

| Endpoint | Method | Verification Required | Expected Result |
|----------|--------|-----------------------|-----------------|
| `/api/health` | GET | Returns healthy | `{"status":"ok"}` with 200 |
| `/api/stations/nearby?lat=...&lng=...&radius_km=...` | GET | Spatial query returns correct stations | 200, only verified/live/active partners' stations |
| `/api/stations/markers?sw_lat=...&ne_lat=...&sw_lng=...&ne_lng=...` | GET | Bbox query for map markers | 200, same visibility filter |
| `/api/stations/search?q=...&connector_type=...` | GET | Text search with connector filter | 200, same visibility filter |
| `/api/stations/{id}` | GET | Detail with charger list | 200, 404 for invisible station |
| `/api/stations/{id}/reviews` | GET | Stub endpoint | 200, empty array or stub response |

## Visibility Rule Contract

```text
GIVEN a partner P with (is_active=false OR is_verified=false OR is_live=false)
  AND stations S that belong to P
WHEN querying ANY Driver Service endpoint that returns stations
THEN S MUST NOT appear in the result set
```

## Full Product Loop Contract

```text
1. POST /api/partners                                → Create partner (default flags)
2. PATCH /api/partners/{id}/verify                    → is_verified = true
3. PATCH /api/partners/{id} with is_live=true         → is_live = true
4. POST /api/stations  (with partner_id)              → Create station
5. POST /api/chargers (with station_id)               → Create charger(s)
6. GET  /api/stations/nearby  (Driver Service)        → Station appears in results
7. PATCH /api/partners/{id}/deactivate                → is_active = false
8. GET  /api/stations/nearby  (Driver Service)        → Station disappears from results
```

## Spatial Query Contract

- The nearby query MUST use an index scan (GIST index on station geometry)
- Query template (captured from service logs):
  ```sql
  SELECT s.*, p.is_active, p.is_verified, p.is_live
  FROM inventory.station s
  JOIN inventory.partner p ON s.partner_id = p.id
  WHERE p.is_active = true AND p.is_verified = true AND p.is_live = true
    AND ST_DWithin(s.location, ST_SetSRID(ST_MakePoint($1, $2), 4326), $3)
  ORDER BY s.location <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
  LIMIT $4
  ```
- EXPLAIN ANALYZE should show: `Index Scan using idx_station_coordinates` (not `Seq Scan`)
- Expected cost per 10k+ stations: < 100ms

## Test Database Cleanup Contract

- Integration tests MUST clean up after themselves (transaction rollback or explicit DELETE)
- Tests requiring DATABASE_URL MUST skip gracefully when env var is absent
- Skip message format: `"[SKIP] test_name requires DATABASE_URL — skipping"`

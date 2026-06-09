# Data Model: Mock API and Design System Foundation

**Phase**: 1 — Design & Contracts
**Date**: 2026-06-09

## Entity: Partner

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | string | yes | NanoID PRT-... |
| name | string | yes | Partner display name |
| type | string | yes | "business" or "personal" (default: "business") |
| is_verified | boolean | yes | Default: false |
| is_live | boolean | yes | Default: false. Cannot be true without is_verified |
| is_active | boolean | yes | Default: true |
| created_at | string (ISO) | yes | Immutable timestamp |
| created_by | string | no | USR-... nullable |
| updated_at | string (ISO) | yes | Updated on every write |
| updated_by | string | no | USR-... nullable |

**Validation**: type must be "business" or "personal"; is_live cannot be true when is_verified is false

## Entity: Station

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | string | yes | NanoID STN-... |
| partner_id | string | yes | References partner.id |
| name | string | yes | Station display name |
| address | string | no | Nullable |
| latitude | number | yes | -90 to 90 |
| longitude | number | yes | -180 to 180 |
| created_at | string (ISO) | yes | Immutable timestamp |
| created_by | string | no | USR-... nullable |
| updated_at | string (ISO) | yes | Updated on every write |
| updated_by | string | no | USR-... nullable |

**Validation**: latitude BETWEEN -90 AND 90; longitude BETWEEN -180 AND 180

**Relationships**: Belongs to Partner (partner_id FK)

## Entity: Charger

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | string | yes | NanoID CHG-... |
| station_id | string | yes | References station.id |
| connector_type | string | yes | "type2", "ccs", "chademo", or "type1" |
| power_kw | number | yes | Must be > 0 |
| status | string | yes | "available", "in_use", "maintenance", or "offline" (default: "available") |
| created_at | string (ISO) | yes | Immutable timestamp |
| created_by | string | no | USR-... nullable |
| updated_at | string (ISO) | yes | Updated on every write |
| updated_by | string | no | USR-... nullable |

**Validation**: connector_type IN (type2, ccs, chademo, type1); power_kw > 0; status IN (available, in_use, maintenance, offline)

**Relationships**: Belongs to Station (station_id FK)

## Entity: Station Availability

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| id | string | yes | Auto-generated |
| station_id | string | yes | References station.id |
| status | string | yes | "available", "partial", or "unavailable" |
| updated_by | string | no | USR-... nullable |
| updated_at | string (ISO) | yes | Set on insert |

**Validation**: status IN (available, partial, unavailable)

**Rules**: Append-only log. Current availability is the most recent row by updated_at for a given station_id. Never updated in place.

## Seed Data Counts

| Entity | Records |
|--------|---------|
| Partner | 3 |
| Station | 15 |
| Charger | 24 |
| Station Availability | 15 |

## Partner Visibility Rule (for driver apps — enforced client-side in MVP-1)

A partner's stations are visible only when ALL three conditions are true:
- is_active = true
- is_verified = true
- is_live = true

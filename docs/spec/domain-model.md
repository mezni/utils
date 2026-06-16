# Domain Model

## Overview

```
Partner (OPR) ──1:N──> Station (STA) ──1:N──> Charger (CHG)
                               │
                               └── 1:1 ──> Location (embedded geography)
                               
Driver (USR) ──M:N──> Station (favorites)
```

All infrastructure entities (Partner, Station, Charger) support soft delete. Non-infrastructure entities (Driver profile, audit log) use hard delete.

---

## Partner (`inventory.partner`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | `varchar(32)` PK | Y | Prefixed nanoid `OPR_` |
| `name` | `varchar(255)` | Y | Legal or trading name |
| `type` | `partner_type` enum | Y | `commercial`, `private` |
| `email` | `varchar(255)` | Y | Business contact email |
| `phone` | `varchar(50)` | N | Business contact phone |
| `address` | `text` | N | Physical address |
| `website` | `varchar(255)` | N | Optional URL |
| `status` | `partner_status` enum | Y | `active`, `suspended`, `closed` |
| `keycloak_id` | `uuid` | Y* | Keycloak user ID for partner auth (* set after Keycloak setup, MVP-3) |
| `created_at` | `timestamptz` | Y | |
| `updated_at` | `timestamptz` | Y | |
| `deleted_at` | `timestamptz` | N | Soft delete timestamp |

### Business Rules

- `commercial` partner may own unlimited stations
- `private` partner may own up to 3 stations (soft limit, enforced at application level)
- Partner deletion cascades soft-delete to all owned stations and chargers
- A partner must have at least one station before their profile is visible in partner listings

---

## Station (`inventory.station`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | `varchar(32)` PK | Y | Prefixed nanoid `STA_` |
| `partner_id` | `varchar(32)` FK | Y | References `inventory.partner.id` |
| `name` | `varchar(255)` | Y | Display name |
| `location` | `geography(Point, 4326)` | Y | Coordinates (lat, lon) |
| `address` | `text` | Y | Street address in Tunisia |
| `city` | `varchar(100)` | Y | Tunisian city name |
| `postal_code` | `varchar(20)` | N | |
| `status` | `station_status` enum | Y | `draft`, `active`, `inactive`, `closed` |
| `visibility` | `station_visibility` enum | Y | `commercial`, `private_home` |
| `photo_url` | `varchar(500)` | N | |
| `description` | `text` | N | Free-text description |
| `access_notes` | `text` | N | How to access (gate code, hours, etc.) |
| `opening_hours` | `varchar(255)` | N | OSM-style opening hours string |
| `has_24h_access` | `boolean` | N | Default `false` |
| `created_at` | `timestamptz` | Y | |
| `updated_at` | `timestamptz` | Y | |
| `deleted_at` | `timestamptz` | N | Soft delete timestamp |

### Business Rules

- Only `active` stations are visible on the public map
- A station must have at least one charger to be activated
- `partner_id` ownership verified on every write — partner can only modify own stations
- Admin can modify any station (ownership check bypassed)
- Deletion cascades: soft-deleting a station soft-deletes all its chargers
- Location coordinates must be within Tunisia bounding box (lat ~30-38, lon ~7-12)

### Station State Machine

```
draft ──>[active]──>inactive──>active
                  └──>closed (soft delete)
```

---

## Charger (`inventory.charger`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | `varchar(32)` PK | Y | Prefixed nanoid `CHG_` |
| `station_id` | `varchar(32)` FK | Y | References `inventory.station.id` |
| `charger_type` | `charger_type` enum | Y | `ac`, `dc` — distinct from connector standard |
| `connector` | `connector_standard` enum | Y | `ccs2`, `type2`, `chademo` |
| `power_kw` | `decimal(6,1)` | Y | Rated power output (e.g. 7.4, 22.0, 150.0) |
| `identifier_code` | `varchar(50)` | N | Partner-assigned code (e.g. CHG-A1) |
| `status` | `charger_status` enum | Y | `available`, `occupied`, `offline`, `maintenance` |
| `created_at` | `timestamptz` | Y | |
| `updated_at` | `timestamptz` | Y | |
| `deleted_at` | `timestamptz` | N | Soft delete timestamp |

### Business Rules

- A charger's `status` is informational (no OCPP real-time tracking per exclusion)
- Status may be manually set by partner via dashboard (available/offline/maintenance)
- A charger cannot be `available` if its station is not `active`
- Power ranges: AC 3.7–22 kW, DC 50–350 kW
- At least one charger per station required before activation

---

## Driver Profile (`users.driver_profile`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `id` | `varchar(32)` PK | Y | Prefixed nanoid `USR_` |
| `keycloak_id` | `uuid` | Y | Keycloak user ID |
| `display_name` | `varchar(100)` | N | |
| `email` | `varchar(255)` | Y | Verified via Keycloak |
| `created_at` | `timestamptz` | Y | |

### Business Rules

- Profile created automatically on first Keycloak login (not during registration)
- No soft delete on user profiles
- Account deletion (hard delete) requests handled admin-manually during validation phase

---

## Favorite (`users.driver_favorite`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `driver_id` | `varchar(32)` FK | Y | References `users.driver_profile.id` |
| `station_id` | `varchar(32)` FK | Y | References `inventory.station.id` |
| `created_at` | `timestamptz` | Y | |

PK is composite `(driver_id, station_id)`.

---

## Enums Summary

### `partner_type`
`commercial`, `private`

### `partner_status`
`pending`, `active`, `suspended`, `closed`, `rejected`

### `station_status`
`draft`, `active`, `inactive`, `closed`

### `station_visibility`
`commercial`, `private_home`

### `charger_type`
`ac`, `dc`

### `connector_standard`
`ccs2`, `type2`, `chademo`

### `charger_status`
`available`, `occupied`, `offline`, `maintenance`

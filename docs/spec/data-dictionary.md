# Data Dictionary — Canonical Domain Terms

## Infrastructure Entities

| Term | ID Prefix | Definition |
|------|-----------|------------|
| **Station** | `STA` | A physical location containing one or more chargers; has an address, geographic coordinates, and belongs to a partner; can be commercial or private home |
| **Charger** | `CHG` | A single charging unit at a station; has a connector type and power level; belongs to a station |
| **Partner** | `OPR` | An entity (business or private individual) that owns and manages one or more stations; created by admin invite or validated by admin after self-registration |

## User Entities

| Term | Keycloak Realm | Definition |
|------|----------------|------------|
| **Public Driver** | (none) | An unauthenticated user who can discover and view stations on the map; no account required |
| **Registered Driver** | `bornemap-drivers` | An authenticated user with a BorneMap account (username/password or social login); can save favorites and access personalized features; auto-validated on registration |
| **Admin** | `bornemap-staff` | A BorneMap platform administrator; manages partners, stations, and platform data; belongs to the admin/partner Keycloak realm |
| **Partner User** | `bornemap-staff` | An authenticated user representing a partner entity; can manage their partner's stations and chargers; belongs to the admin/partner Keycloak realm; must be invited by admin or approved by admin after self-registration |

## Geographic & Spatial Entities

| Term | Schema | Definition |
|------|--------|------------|
| **OSM Station** | `gis.osm_stations` | A charging station imported from OpenStreetMap via the ETL importer; lives in gis schema; read-only reference data; may or may not correspond to an inventory station |
| **OSM Road** | `gis.osm_roads` | A road segment imported from OpenStreetMap; used for spatial context and routing reference; lives in gis schema |
| **OSM City** | `gis.osm_cities` | An administrative boundary or populated place imported from OpenStreetMap; used for geographic filtering and display; lives in gis schema |
| **Nearby** | (query result) | A spatial query result: a ranked list of stations within a given radius (meters) from a given coordinate (lat/lon); computed via PostGIS `ST_DWithin` on the station table |
| **Coordinate** | (value type) | A geographic point expressed as `(latitude, longitude)` in WGS84 (EPSG:4326); used consistently across all spatial operations |

## Auth & Identity

| Term | Definition |
|------|------------|
| **Realm** | A Keycloak tenant boundary; BorneMap uses two: `bornemap-drivers` (registered drivers) and `bornemap-staff` (admins and partner users) |
| **Identity Provider (IdP)** | An external authentication source; BorneMap supports Google and Facebook as social IdPs for driver self-registration in `bornemap-drivers` realm |
| **JWT** | A signed token issued by Keycloak upon successful authentication; presented by clients on every authenticated API request; validated by services via JWKS (no Keycloak call per request) |
| **Invite** | An admin-initiated action that creates a pending partner user account; the invited user receives a link to complete registration; auto-approved on completion |
| **Approval** | An admin action that validates a partner user who self-registered from the dashboard; partner has restricted access until approved |
| **Session** | An authenticated user session managed by Keycloak; clients hold an access token (short-lived) and refresh token (longer-lived) |

## Platform & Operations

| Term | Definition |
|------|------------|
| **MVP** | A Minimum Viable Product phase; a discrete, independently demonstrable increment of the platform; BorneMap has six MVPs (see constitution Section 8) |
| **Soft Delete** | Marking an infrastructure entity (station, charger, partner) as deleted via `deleted_at` timestamp without removing the DB row; soft-deleted entities are excluded from all queries by default |
| **Audit Log** | A MongoDB record of every infrastructure change (create, update, soft delete) on stations, chargers, and partners; immutable append-only |
| **Cache Bust** | A synchronous invalidation of Redis cache keys triggered immediately after a write to `inventory.station` or `inventory.charger`; ensures GIS Service serves fresh data |
| **Health Check** | A lightweight endpoint (`/api/v1/health`) on every service confirming the process is running; a separate readiness check (`/api/v1/health/ready`) confirms DB connectivity |
| **ETL Importer** | A docker-compose service that fetches OpenStreetMap data and populates the gis schema; runs on-demand or on a schedule; does not touch inventory schema |

## Partner States

| State | Definition |
|-------|------------|
| `pending` | Partner self-registered, awaiting admin approval; restricted dashboard access |
| `active` | Approved by admin; full access to manage stations |
| `suspended` | Temporarily disabled by admin; API returns `OPR_004` on actions |
| `closed` | Permanently removed (soft delete); filtered from all queries |
| `rejected` | Denied by admin; login returns `AUTH_007`; cannot re-register without new invite |

## Station States

| State | Definition |
|-------|------------|
| `draft` | Being set up by partner; not visible on public map |
| `active` | Operational and visible on public map |
| `inactive` | Temporarily unavailable; hidden from public map |
| `closed` | Permanently removed by partner or admin; soft-deleted |

## Charger Types

| Type | Definition |
|------|------------|
| `ac` | Alternating Current (slow charging, typically 3.7-22 kW) |
| `dc` | Direct Current (fast charging, typically 50-350 kW) |

## Connector Standards

| Connector | Regions | Typical Power |
|-----------|---------|---------------|
| `ccs2` | EU (incl. Tunisia) | DC up to 350 kW |
| `type2` | EU (incl. Tunisia) | AC up to 22 kW |
| `chademo` | Japan, some EU | DC up to 62.5 kW |

## Station Visibility Categories

| Category | Definition | Default on Map |
|----------|------------|----------------|
| **Commercial** | Station operated by a business partner, typically with multiple chargers, may have pricing | Yes |
| **Private Home** | Station at a private residence; owner has opted to share it publicly | Yes |

## Schema Owners

| Schema | Purpose | Owner Service |
|--------|---------|---------------|
| `gis` | OSM reference data (stations, cities, roads) | GIS Service (read) / OSM Importer (write) |
| `inventory` | Operational entities (partner, station, charger) | Driver Service / Admin Service |
| `users` | Application user profiles linked to Keycloak IDs | Auth Service |

## Services

| Service | Port | Purpose |
|---------|------|---------|
| **Auth Service** | `:3000` | Identity management — owns `users` schema, integrates Keycloak |
| **Driver Service** | `:3001` | Driver-facing API — station ops, favorites, nearby (via GIS) |
| **Admin Service** | `:3002` | Admin/partner management — partner CRUD, station admin |
| **GIS Service** | `:3003` | Read-optimized spatial API — serves `/api/v1/nearby`, Redis-cached |

## Infrastructure Components

| Component | Purpose |
|-----------|---------|
| **Traefik** | API Gateway — TLS termination, path-based routing to services (MVP-6) |
| **Redis** | GIS query cache — reduces PostGIS load for repeated nearby queries |
| **Map Tiles** | OSM/Mapbox raster tiles for base map rendering in web + mobile apps |

## Database Instances

| Database | Purpose |
|----------|---------|
| `platform_db` | Main application database (gis + inventory + users schemas) |
| `keycloak_db` | Dedicated Postgres for Keycloak identity data |
| `analytics_db` | Separate Postgres for analytics/usage data |

## Cache

| Cache | Type | Used By |
|-------|------|---------|
| **GIS Query Cache** | Redis `:6379` | GIS Service — caches nearby query results with TTL |

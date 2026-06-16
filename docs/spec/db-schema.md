# Database Schema Specification

## Databases

| Database | Image | Purpose |
|----------|-------|---------|
| `platform_db` | `postgis/postgis:15-3.4` | Main application data |
| `keycloak_db` | `postgres:16-alpine` | Keycloak identity store |
| `analytics_db` | `postgres:16-alpine` | Analytics/usage data |

---

## `platform_db` — Schema: `gis`

### `gis.osm_stations`

Stations imported from OpenStreetMap. Read-mostly; written by OSM importer only.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `bigserial` PK | | Internal ID |
| `osm_id` | `bigint` UNIQUE | NOT NULL | OSM node/way ID |
| `name` | `varchar(255)` | | |
| `location` | `geography(Point, 4326)` | NOT NULL | |
| `address` | `text` | | |
| `city` | `varchar(100)` | | |
| `operator` | `varchar(255)` | | |
| `capacity` | `int` | | Number of charging points |
| `raw_tags` | `jsonb` | | Full OSM tags |
| `imported_at` | `timestamptz` | NOT NULL DEFAULT now() | |

**Indexes**: GIST on `location`, btree on `osm_id`, btree on `city`.

### `gis.osm_cities`

Tunisian city boundaries and points from OSM.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `bigserial` PK | | |
| `osm_id` | `bigint` UNIQUE | NOT NULL | |
| `name` | `varchar(100)` | NOT NULL | |
| `name_ar` | `varchar(100)` | | Arabic name |
| `location` | `geography(Point, 4326)` | NOT NULL | Centroid |
| `boundary` | `geometry(Polygon, 4326)` | | City boundary polygon |
| `population` | `int` | | |
| `imported_at` | `timestamptz` | NOT NULL DEFAULT now() | |

**Indexes**: GIST on `location`, btree on `name`.

### `gis.osm_roads`

Major road network from OSM for spatial context.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `bigserial` PK | | |
| `osm_id` | `bigint` UNIQUE | NOT NULL | |
| `name` | `varchar(255)` | | |
| `road_class` | `varchar(50)` | | motorway, trunk, primary, secondary, etc. |
| `geom` | `geometry(MultiLineString, 4326)` | NOT NULL | |
| `imported_at` | `timestamptz` | NOT NULL DEFAULT now() | |

**Indexes**: GIST on `geom`, btree on `road_class`.

### `gis.nearby(lat, lon, radius_m, max_results)`

SQL function — see `docs/spec/gis-spec.md` for full spec.

---

## `platform_db` — Schema: `inventory`

### `inventory.partner`

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `varchar(32)` PK | | `OPR_` nanoid |
| `name` | `varchar(255)` | NOT NULL | |
| `type` | `partner_type` | NOT NULL | `commercial` or `private` |
| `email` | `varchar(255)` | NOT NULL | |
| `phone` | `varchar(50)` | | |
| `address` | `text` | | |
| `website` | `varchar(255)` | | |
| `status` | `partner_status` | NOT NULL DEFAULT 'active' | |
| `keycloak_id` | `uuid` | | Set during MVP-3 Keycloak integration |
| `created_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `deleted_at` | `timestamptz` | | |

**Indexes**: btree on `email`, btree on `keycloak_id`, btree on `status`.
**Audit trigger**: INSERT/UPDATE/DELETE logged to audit collection (MongoDB, MVP-5+).

### `inventory.station`

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `varchar(32)` PK | | `STA_` nanoid |
| `partner_id` | `varchar(32)` | NOT NULL FK -> inventory.partner(id) | |
| `name` | `varchar(255)` | NOT NULL | |
| `location` | `geography(Point, 4326)` | NOT NULL | |
| `address` | `text` | NOT NULL | |
| `city` | `varchar(100)` | NOT NULL | |
| `postal_code` | `varchar(20)` | | |
| `status` | `station_status` | NOT NULL DEFAULT 'draft' | |
| `visibility` | `station_visibility` | NOT NULL DEFAULT 'commercial' | |
| `photo_url` | `varchar(500)` | | |
| `description` | `text` | | |
| `access_notes` | `text` | | |
| `opening_hours` | `varchar(255)` | | |
| `has_24h_access` | `boolean` | NOT NULL DEFAULT false | |
| `created_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `deleted_at` | `timestamptz` | | |

**Indexes**: GIST on `location`, btree on `partner_id`, btree on `status`, btree on `city`.
**Foreign key**: `partner_id` -> `inventory.partner(id)` ON DELETE RESTRICT.
**Check constraint**: `ST_Within(location, tunisia_bbox)` (application-level enforced, MVP-2+).

### `inventory.charger`

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `varchar(32)` PK | | `CHG_` nanoid |
| `station_id` | `varchar(32)` | NOT NULL FK -> inventory.station(id) | |
| `charger_type` | `charger_type` | NOT NULL | |
| `connector` | `connector_standard` | NOT NULL | |
| `power_kw` | `decimal(6,1)` | NOT NULL | |
| `identifier_code` | `varchar(50)` | | |
| `status` | `charger_status` | NOT NULL DEFAULT 'available' | |
| `created_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT now() | |
| `deleted_at` | `timestamptz` | | |

**Indexes**: btree on `station_id`, btree on `status`, GIST on no geometry columns needed.
**Foreign key**: `station_id` -> `inventory.station(id)` ON DELETE CASCADE.

---

## `platform_db` — Schema: `users`

### `users.driver_profile`

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `varchar(32)` PK | | `USR_` nanoid |
| `keycloak_id` | `uuid` | NOT NULL UNIQUE | |
| `display_name` | `varchar(100)` | | |
| `email` | `varchar(255)` | NOT NULL | |
| `created_at` | `timestamptz` | NOT NULL DEFAULT now() | |

**Indexes**: btree on `keycloak_id`, btree on `email`.

### `users.driver_favorite`

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `driver_id` | `varchar(32)` | NOT NULL FK -> users.driver_profile(id) | |
| `station_id` | `varchar(32)` | NOT NULL FK -> inventory.station(id) | |
| `created_at` | `timestamptz` | NOT NULL DEFAULT now() | |

**PK**: `(driver_id, station_id)` composite.
**Foreign keys**: CASCADE on delete (both sides).

---

## `analytics_db` — Schema: `public`

Created in MVP-5. Tables defined at that time.

---

## Enums (custom types)

```sql
CREATE TYPE partner_type AS ENUM ('commercial', 'private');
CREATE TYPE partner_status AS ENUM ('pending', 'active', 'suspended', 'closed', 'rejected');
CREATE TYPE station_status AS ENUM ('draft', 'active', 'inactive', 'closed');
CREATE TYPE station_visibility AS ENUM ('commercial', 'private_home');
CREATE TYPE charger_type AS ENUM ('ac', 'dc');
CREATE TYPE connector_standard AS ENUM ('ccs2', 'type2', 'chademo');
CREATE TYPE charger_status AS ENUM ('available', 'occupied', 'offline', 'maintenance');
```

## Soft Delete Convention

All `inventory` schema tables use soft delete (`deleted_at`). All reads must filter:
```sql
WHERE deleted_at IS NULL
```
Enforced via repository layer, optionally via views in later MVPs.

## Migration Order (MVP-1)

1. Create enums
2. Create `inventory.partner`
3. Create `inventory.station`
4. Create `inventory.charger`
5. Create `gis.osm_stations`, `gis.osm_cities`, `gis.osm_roads`
6. Create `users.driver_profile`, `users.driver_favorite`

# Inventory Schema

Business entities for stations, partners, and chargers.

**Database**: `ev_platform`

**Schema**: `inventory`

---

## Tables

### partner

Partners who own and operate charging stations.

```sql
CREATE TABLE inventory.partner (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

**Fields**:
- `id` (UUID): Unique identifier
- `name` (TEXT): Partner name (e.g., "SolaRent Tunisia")
- `created_at` (TIMESTAMPTZ): Creation timestamp

**Indexes**:
- PK: `id`

**Constraints**:
- `name` NOT NULL

---

### station

Charging stations owned by partners.

```sql
CREATE TABLE inventory.station (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_id UUID NOT NULL REFERENCES inventory.partner(id),
  name TEXT NOT NULL,
  address TEXT,
  latitude NUMERIC(10,7) NOT NULL,
  longitude NUMERIC(10,7) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

**Fields**:
- `id` (UUID): Unique identifier
- `partner_id` (UUID): Foreign key to `partner`
- `name` (TEXT): Station name (e.g., "Tunis Central Hub")
- `address` (TEXT): Physical address (nullable for now)
- `latitude` (NUMERIC): Latitude coordinate (-90 to 90)
- `longitude` (NUMERIC): Longitude coordinate (-180 to 180)
- `created_at` (TIMESTAMPTZ): Creation timestamp
- `updated_at` (TIMESTAMPTZ): Last update timestamp

**Indexes**:
- PK: `id`
- FK: `partner_id`

**Constraints**:
- `partner_id` NOT NULL, REFERENCES `inventory.partner(id)`
- `name` NOT NULL
- `-90 <= latitude <= 90`
- `-180 <= longitude <= 180`

---

### charger

Individual charging points at a station.

```sql
CREATE TABLE inventory.charger (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  station_id UUID NOT NULL REFERENCES inventory.station(id),
  connector_type TEXT NOT NULL,
  power_kw NUMERIC(6,2) NOT NULL,
  status TEXT NOT NULL DEFAULT 'available',
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

**Fields**:
- `id` (UUID): Unique identifier
- `station_id` (UUID): Foreign key to `station`
- `connector_type` (TEXT): Type of connector (Type2, CCS, CHAdeMO, etc.)
- `power_kw` (NUMERIC): Power rating in kilowatts
- `status` (TEXT): Charger state (available, in_use, maintenance)
- `updated_at` (TIMESTAMPTZ): Last update timestamp

**Indexes**:
- PK: `id`
- FK: `station_id`

**Constraints**:
- `station_id` NOT NULL, REFERENCES `inventory.station(id)`
- `connector_type` NOT NULL
- `power_kw` NOT NULL, > 0
- `status` IN ('available', 'in_use', 'maintenance')

**Enum Values** (as TEXT):
- `available` — Charger is free and ready to use
- `in_use` — Charger is actively charging a vehicle
- `maintenance` — Charger is offline or under maintenance

---

## Migrations

### Migration 0001_schemas.sql

Create `inventory` and `gis` schemas.

```sql
CREATE SCHEMA inventory;
CREATE SCHEMA gis;
```

### Migration 0002_inventory_tables.sql

Create all tables in `inventory` schema.

```sql
CREATE TABLE inventory.partner (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE inventory.station (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  partner_id UUID NOT NULL REFERENCES inventory.partner(id),
  name TEXT NOT NULL,
  address TEXT,
  latitude NUMERIC(10,7) NOT NULL,
  longitude NUMERIC(10,7) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE inventory.charger (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  station_id UUID NOT NULL REFERENCES inventory.station(id),
  connector_type TEXT NOT NULL,
  power_kw NUMERIC(6,2) NOT NULL,
  status TEXT NOT NULL DEFAULT 'available',
  updated_at TIMESTAMPTZ DEFAULT now()
);
```

### Migration 0003_indexes.sql

Add indexes for foreign keys.

```sql
CREATE INDEX idx_station_partner_id ON inventory.station(partner_id);
CREATE INDEX idx_charger_station_id ON inventory.charger(station_id);
```

---

## Seed Data

MVP-1 includes seed data for testing and demos:
- **3 partners** with real Tunisian names
- **15 stations** across major Tunisian cities (Tunis, Sfax, Sousse, Bizerte, Nabeul, Hammamet, Monastir, Djerba, Kairouan, Gabès)
- **24 chargers** with mixed connector types (Type2, CCS, CHAdeMO) and power ratings

Seed script: `source/services/bornemap-service/seed.py` (to be created in Sprint 1.1)

---

## Relationships

```
partner (1) ─────── (N) station
  id                    partner_id
  
station (1) ─────── (N) charger
  id                    station_id
```

---

## MVP Evolution

**MVP-1**: `inventory` schema only. GIS schema created empty and reserved.

**MVP-2**: Add PostGIS extension. Create spatial indexes on station coordinates.

**MVP-3**: Add `users` schema. Partner membership and user data separate.

**MVP-4**: Populate `gis` schema via trigger from `inventory.station`. Add spatial indexes.

**MVP-5**: Add `analytics` schema. Event tracking separate.

---

## Notes

- All IDs are UUID v4 in MVP-1. NanoID prefixed identifiers (STN-..., PRT-..., CHG-...) introduced in MVP-2.
- Latitude and longitude stored as NUMERIC for precision. Converted to GEOGRAPHY in PostGIS after MVP-2 migration.
- Charger status is TEXT enum (not separate table) for simplicity in MVP-1.
- `updated_at` on charger reflects status changes and manual availability updates.

---

**Last Updated**: Sprint 1.1 (in progress)

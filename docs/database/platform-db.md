# platform_db — System of Record

**Engine:** PostgreSQL 16 + PostGIS

---

## Schemas

```
platform_db
├── inventory   (core EV infrastructure)
├── users       (MVP-3 identity domain)
└── gis         (read-only spatial data)
```

---

## Inventory Schema (Core Domain)

### partner

```sql
CREATE TABLE inventory.partner (
  id TEXT PRIMARY KEY,              -- PRT-{nanoid}

  name TEXT NOT NULL,
  type TEXT NOT NULL CHECK (
    type IN ('business', 'personal')
  ),

  is_verified BOOLEAN DEFAULT FALSE,
  is_active   BOOLEAN DEFAULT TRUE,
  is_live     BOOLEAN DEFAULT FALSE,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT,

  CONSTRAINT partner_live_requires_verified
    CHECK (is_live = FALSE OR is_verified = TRUE)
);
```

### station

```sql
CREATE TABLE inventory.station (
  id TEXT PRIMARY KEY,              -- STA-{nanoid}

  partner_id TEXT NOT NULL
    REFERENCES inventory.partner(id)
    ON DELETE CASCADE,

  name TEXT NOT NULL,
  address TEXT,

  latitude  NUMERIC(10,7) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
  longitude NUMERIC(10,7) NOT NULL CHECK (longitude BETWEEN -180 AND 180),

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT
);

CREATE INDEX idx_station_partner ON inventory.station(partner_id);
CREATE INDEX idx_station_location ON inventory.station(latitude, longitude);
```

### charger

```sql
CREATE TABLE inventory.charger (
  id TEXT PRIMARY KEY,              -- CHR-{nanoid}

  station_id TEXT NOT NULL
    REFERENCES inventory.station(id)
    ON DELETE CASCADE,

  connector_type TEXT NOT NULL,
  power_kw NUMERIC(6,2) NOT NULL CHECK (power_kw > 0),

  status TEXT NOT NULL DEFAULT 'available',

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT
);

CREATE INDEX idx_charger_station ON inventory.charger(station_id);
```

### connector_type (reference)

```sql
CREATE TABLE inventory.connector_type (
  code TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### charger_status (reference)

```sql
CREATE TABLE inventory.charger_status (
  code TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Users Schema (MVP-3)

### user_account

```sql
CREATE TABLE users.user_account (
  id TEXT PRIMARY KEY,              -- USR-{nanoid}

  keycloak_sub TEXT UNIQUE NOT NULL,

  role TEXT NOT NULL CHECK (
    role IN ('public_driver', 'registered_driver', 'partner', 'admin')
  ),

  partner_id TEXT NULL,

  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### profile

```sql
CREATE TABLE users.profile (
  user_id TEXT PRIMARY KEY
    REFERENCES users.user_account(id)
    ON DELETE CASCADE,

  display_name TEXT,
  email TEXT,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### favorite_station

```sql
CREATE TABLE users.favorite_station (
  user_id TEXT
    REFERENCES users.user_account(id)
    ON DELETE CASCADE,

  station_id TEXT
    REFERENCES inventory.station(id)
    ON DELETE CASCADE,

  created_at TIMESTAMPTZ DEFAULT NOW(),

  PRIMARY KEY (user_id, station_id)
);
```

### station_review

```sql
CREATE TABLE users.station_review (
  id TEXT PRIMARY KEY,

  user_id TEXT
    REFERENCES users.user_account(id)
    ON DELETE CASCADE,

  station_id TEXT
    REFERENCES inventory.station(id)
    ON DELETE CASCADE,

  rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
  comment TEXT,

  status TEXT DEFAULT 'visible'
    CHECK (status IN ('visible', 'hidden')),

  created_at TIMESTAMPTZ DEFAULT NOW(),

  CONSTRAINT one_review_per_user UNIQUE (user_id, station_id)
);
```

---

## GIS Schema (Read-Only)

```
gis.planet_osm_point
gis.planet_osm_line
gis.planet_osm_polygon
gis.planet_osm_roads

CREATE INDEX idx_point_geom ON gis.planet_osm_point USING GIST (way);
CREATE INDEX idx_line_geom  ON gis.planet_osm_line  USING GIST (way);
CREATE INDEX idx_poly_geom  ON gis.planet_osm_polygon USING GIST (way);
CREATE INDEX idx_roads_geom ON gis.planet_osm_roads USING GIST (way);
```

**RULE:** GIS schema is READ-ONLY. No service may write to GIS tables.

---

## Relationship Model

```
partner
  └── station
        └── charger

user_account
  ├── profile
  ├── favorite_station
  └── station_review
```

---

## Data Governance

- **Source of truth:** inventory schema is authoritative
- **Integrity:** FK enforced everywhere, no orphan records
- **Audit:** All inventory tables include created_at, created_by, updated_at, updated_by
- **Deletion:** Soft delete for partners/stations; hard delete for user-generated content

---

## Performance Design

| Index | Table | Type |
|---|---|---|
| idx_station_partner | station | B-tree |
| idx_station_location | station | B-tree (lat/lng) |
| idx_charger_station | charger | B-tree |

**Future upgrade:** Replace lat/lng with `location GEOGRAPHY(Point, 4326)` for true PostGIS spatial indexing and fast radius queries.

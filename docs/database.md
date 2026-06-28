# BorneMap Database Schema — Final v2

## 1. Extensions

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS postgis;
```

## 2. Schemas

```sql
CREATE SCHEMA IF NOT EXISTS users;
CREATE SCHEMA IF NOT EXISTS ev;
CREATE SCHEMA IF NOT EXISTS gis;
```

## 3. Users Schema (Auth Service)

### 3.1 `users.accounts`

```sql
CREATE TABLE users.accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'partner', 'driver')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

## 4. EV Schema (Source of Truth)

### 4.1 `ev.partners`

```sql
CREATE TABLE ev.partners (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_partners_name UNIQUE (name)
);
```

### 4.2 `ev.stations`

```sql
CREATE TABLE ev.stations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_id UUID NOT NULL,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_station_partner
        FOREIGN KEY (partner_id) REFERENCES ev.partners(id) ON DELETE CASCADE,
    CONSTRAINT chk_latitude CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT chk_longitude CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT uq_station_partner_name UNIQUE (partner_id, name)
);
```

### 4.3 `ev.connectors`

```sql
CREATE TABLE ev.connectors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    station_id UUID NOT NULL,
    type TEXT NOT NULL,
    power_kw NUMERIC NOT NULL CHECK (power_kw > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_connector_station
        FOREIGN KEY (station_id) REFERENCES ev.stations(id) ON DELETE CASCADE
);
```

### 4.4 Automatic `updated_at` Triggers

```sql
CREATE OR REPLACE FUNCTION ev.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_partners_updated_at
BEFORE UPDATE ON ev.partners
FOR EACH ROW EXECUTE FUNCTION ev.set_updated_at();

CREATE TRIGGER trg_stations_updated_at
BEFORE UPDATE ON ev.stations
FOR EACH ROW EXECUTE FUNCTION ev.set_updated_at();

CREATE TRIGGER trg_connectors_updated_at
BEFORE UPDATE ON ev.connectors
FOR EACH ROW EXECUTE FUNCTION ev.set_updated_at();
```

### 4.5 Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_stations_partner_id ON ev.stations(partner_id);
CREATE INDEX IF NOT EXISTS idx_connectors_station_id ON ev.connectors(station_id);
CREATE INDEX IF NOT EXISTS idx_stations_geo_hint ON ev.stations(latitude, longitude);
```

## 5. GIS Schema (Projection Layer)

### 5.1 `gis.station_projection`

```sql
CREATE TABLE gis.station_projection (
    station_id   TEXT PRIMARY KEY,
    geom         GEOGRAPHY(POINT, 4326) NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_station_projection_geom
ON gis.station_projection
USING GIST (geom);
```

### 5.2 `gis.station_projection_sync_log` (Optional Audit)

```sql
CREATE TABLE gis.station_projection_sync_log (
    id           BIGSERIAL PRIMARY KEY,
    station_id   TEXT NOT NULL,
    operation    TEXT NOT NULL,
    synced_at    TIMESTAMPTZ DEFAULT NOW()
);
```

## 6. EV → GIS Synchronization (Trigger System)

### 6.1 Sync Function

```sql
CREATE OR REPLACE FUNCTION gis.sync_station_projection()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM gis.station_projection
        WHERE station_id = OLD.id;

        INSERT INTO gis.station_projection_sync_log (station_id, operation)
        VALUES (OLD.id, 'DELETE');

        RETURN OLD;
    END IF;

    INSERT INTO gis.station_projection (
        station_id, geom, latitude, longitude, updated_at
    )
    VALUES (
        NEW.id,
        ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography,
        NEW.latitude,
        NEW.longitude,
        NOW()
    )
    ON CONFLICT (station_id)
    DO UPDATE SET
        geom = EXCLUDED.geom,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        updated_at = NOW();

    INSERT INTO gis.station_projection_sync_log (station_id, operation)
    VALUES (NEW.id, TG_OP);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### 6.2 Trigger Binding

```sql
CREATE TRIGGER trg_station_projection_sync
AFTER INSERT OR UPDATE OR DELETE
ON ev.stations
FOR EACH ROW
EXECUTE FUNCTION gis.sync_station_projection();
```

## 7. GIS Query Layer

### 7.1 `gis.get_nearby_stations()`

```sql
CREATE OR REPLACE FUNCTION gis.get_nearby_stations(
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    radius_meters INTEGER DEFAULT 5000
)
RETURNS TABLE (
    station_id TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        sp.station_id,
        sp.latitude,
        sp.longitude,
        ST_Distance(
            sp.geom,
            ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography
        ) AS distance_meters
    FROM gis.station_projection sp
    WHERE ST_DWithin(
        sp.geom,
        ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
        radius_meters
    )
    ORDER BY distance_meters ASC;
END;
$$ LANGUAGE plpgsql;
```

## 8. Design Guarantees (Enforced)

| Guarantee | Mechanism |
|-----------|-----------|
| Single Source of Truth | `ev` = authoritative; `gis` = derived projection only |
| No App-Level GIS Writes | Only DB trigger writes to GIS |
| Strong Consistency | Any station update automatically updates spatial layer |
| High Performance | GiST index on geography; `ST_DWithin` filter pushdown; join optimized via indexed FK |

## 9. Service Permissions

| Service | Schema Access | Operations |
|---------|--------------|------------|
| Admin Service | `ev` | Write |
| Driver Service | `gis` (execute `get_nearby_stations`) | Read-only |
| GIS system | `gis` | Internal trigger only |
| Auth Service | `users` | Write |

## 10. Runtime Flows

### Write Path (Admin Service → Trigger → GIS)
```
Admin Service
   ↓ INSERT/UPDATE/DELETE
ev.stations
   ↓ (DB trigger: trg_station_projection_sync)
gis.sync_station_projection()
   ↓
gis.station_projection (upsert/delete)
   ↓
gis.station_projection_sync_log (audit)
```

### Read Path (Driver Service → GIS)
```
Driver Service API
   ↓
gis.get_nearby_stations(lat, lng, radius)
   ↓
PostGIS GiST index (ST_DWithin)
   ↓
Filtered + sorted results (distance ASC)
   ↓
Response to client
```

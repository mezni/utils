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

### 5.1 `gis.station_locations`

```sql
CREATE TABLE gis.station_locations (
    station_id UUID PRIMARY KEY,
    geom GEOGRAPHY(POINT, 4326) NOT NULL,
    CONSTRAINT fk_gis_station
        FOREIGN KEY (station_id) REFERENCES ev.stations(id) ON DELETE CASCADE
);
```

### 5.2 Spatial Index (CRITICAL)

```sql
CREATE INDEX idx_station_locations_geom
ON gis.station_locations
USING GIST (geom);
```

## 6. EV → GIS Synchronization (Trigger System)

### 6.1 Sync Function

```sql
CREATE OR REPLACE FUNCTION gis.sync_station_location()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO gis.station_locations (station_id, geom)
    VALUES (
        NEW.id,
        ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography
    )
    ON CONFLICT (station_id)
    DO UPDATE SET geom = EXCLUDED.geom;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### 6.2 Trigger

```sql
CREATE TRIGGER trg_sync_station_location
AFTER INSERT OR UPDATE OF latitude, longitude
ON ev.stations
FOR EACH ROW
EXECUTE FUNCTION gis.sync_station_location();
```

## 7. GIS Query Layer

### 7.1 `gis.nearby_stations()`

```sql
CREATE OR REPLACE FUNCTION gis.nearby_stations(
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    radius DOUBLE PRECISION
)
RETURNS TABLE (
    station_id UUID,
    partner_id UUID,
    distance DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.id,
        s.partner_id,
        ST_Distance(
            l.geom,
            ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography
        ) AS distance
    FROM gis.station_locations l
    JOIN ev.stations s ON s.id = l.station_id
    WHERE ST_DWithin(
        l.geom,
        ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
        radius
    )
    ORDER BY distance ASC;
END;
$$ LANGUAGE plpgsql;
```

## 8. Data Integrity & Indexes

```sql
CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE INDEX idx_connectors_station_id ON ev.connectors(station_id);
CREATE UNIQUE INDEX uniq_station_per_partner ON ev.stations(partner_id, name);
```

## 9. Automatic `updated_at` Trigger

```sql
CREATE OR REPLACE FUNCTION ev.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_stations_updated_at
BEFORE UPDATE ON ev.stations
FOR EACH ROW
EXECUTE FUNCTION ev.touch_updated_at();
```

## 10. Design Guarantees (Enforced)

| Guarantee | Mechanism |
|-----------|-----------|
| Single Source of Truth | `ev` = authoritative; `gis` = derived projection only |
| No App-Level GIS Writes | Only DB trigger writes to GIS |
| Strong Consistency | Any station update automatically updates spatial layer |
| High Performance | GiST index on geography; `ST_DWithin` filter pushdown; join optimized via indexed FK |

## 11. Service Permissions

| Service | Schema Access | Operations |
|---------|--------------|------------|
| Admin Service | `ev` | Write |
| Driver Service | `ev` (read) + `gis` (execute `nearby_stations`) | Read-only |
| GIS system | `gis` | Internal trigger only |
| Auth Service | `users` | Write |

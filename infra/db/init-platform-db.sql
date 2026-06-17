-- BorneMap Platform DB — MVP-1 Initialization
-- Applied automatically by docker-entrypoint-initdb.d

-- ============================================================
-- Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- Enums
-- ============================================================
CREATE TYPE partner_type AS ENUM ('commercial', 'private');
CREATE TYPE partner_status AS ENUM ('pending', 'active', 'suspended', 'closed', 'rejected');
CREATE TYPE station_status AS ENUM ('draft', 'active', 'inactive', 'closed');
CREATE TYPE station_visibility AS ENUM ('commercial', 'private_home');
CREATE TYPE charger_type AS ENUM ('ac', 'dc');
CREATE TYPE connector_standard AS ENUM ('ccs2', 'type2', 'chademo');
CREATE TYPE charger_status AS ENUM ('available', 'occupied', 'offline', 'maintenance');

-- ============================================================
-- Schema: inventory
-- ============================================================
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.partner (
    id              varchar(32) PRIMARY KEY,
    name            varchar(255) NOT NULL,
    type            partner_type NOT NULL,
    email           varchar(255) NOT NULL,
    phone           varchar(50),
    address         text,
    website         varchar(255),
    status          partner_status NOT NULL DEFAULT 'active',
    keycloak_id     uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz
);

CREATE INDEX idx_partner_email ON inventory.partner (email);
CREATE INDEX idx_partner_keycloak_id ON inventory.partner (keycloak_id);
CREATE INDEX idx_partner_status ON inventory.partner (status);

CREATE TABLE inventory.station (
    id              varchar(32) PRIMARY KEY,
    partner_id      varchar(32) REFERENCES inventory.partner(id) ON DELETE RESTRICT,
    name            varchar(255) NOT NULL,
    location        geography(Point, 4326) NOT NULL,
    address         text NOT NULL,
    city            varchar(100) NOT NULL,
    postal_code     varchar(20),
    status          station_status NOT NULL DEFAULT 'draft',
    visibility      station_visibility NOT NULL DEFAULT 'commercial',
    photo_url       varchar(500),
    description     text,
    access_notes    text,
    opening_hours   varchar(255),
    has_24h_access  boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz
);

CREATE INDEX idx_station_partner ON inventory.station (partner_id);
CREATE INDEX idx_station_status ON inventory.station (status);
CREATE INDEX idx_station_visibility ON inventory.station (visibility);
CREATE INDEX idx_station_city ON inventory.station (city);
CREATE INDEX idx_station_location ON inventory.station USING GIST (location);

CREATE TABLE inventory.charger (
    id              varchar(32) PRIMARY KEY,
    station_id      varchar(32) NOT NULL REFERENCES inventory.station(id) ON DELETE CASCADE,
    charger_type    charger_type NOT NULL,
    connector       connector_standard NOT NULL,
    power_kw        decimal(6,1) NOT NULL,
    identifier_code varchar(50),
    status          charger_status NOT NULL DEFAULT 'available',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz
);

CREATE INDEX idx_charger_station ON inventory.charger (station_id);
CREATE INDEX idx_charger_status ON inventory.charger (status);

-- ============================================================
-- Schema: gis
-- ============================================================
CREATE SCHEMA IF NOT EXISTS gis;

CREATE TABLE gis.osm_stations (
    id              bigserial PRIMARY KEY,
    osm_id          bigint UNIQUE NOT NULL,
    name            varchar(255),
    location        geography(Point, 4326) NOT NULL,
    address         text,
    city            varchar(100),
    operator        varchar(255),
    capacity        int,
    raw_tags        jsonb,
    imported_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_osm_stations_location ON gis.osm_stations USING GIST (location);
CREATE INDEX idx_osm_stations_city ON gis.osm_stations (city);

CREATE TABLE gis.osm_cities (
    id              bigserial PRIMARY KEY,
    osm_id          bigint UNIQUE NOT NULL,
    name            varchar(100) NOT NULL,
    name_ar         varchar(100),
    location        geography(Point, 4326) NOT NULL,
    boundary        geometry(Polygon, 4326),
    population      int,
    imported_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_osm_cities_location ON gis.osm_cities USING GIST (location);

CREATE TABLE gis.osm_roads (
    id              bigserial PRIMARY KEY,
    osm_id          bigint UNIQUE NOT NULL,
    name            varchar(255),
    road_class      varchar(50),
    geom            geometry(MultiLineString, 4326) NOT NULL,
    imported_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_osm_roads_geom ON gis.osm_roads USING GIST (geom);

-- ============================================================
-- GIS Functions
-- ============================================================

-- Function to find nearby stations
-- Parameters: latitude, longitude, radius_in_km, max_results, status_filter, visibility_filter
-- Returns: paginated list of nearby stations with distance
CREATE OR REPLACE FUNCTION gis.nearby(
    p_lat float,
    p_lon float,
    p_radius_km float DEFAULT 10.0,
    p_limit int DEFAULT 50,
    p_status_filter station_status DEFAULT 'active',
    p_visibility_filter text DEFAULT 'all'
)
RETURNS TABLE (
    id varchar(32),
    name varchar(255),
    visibility station_visibility,
    location geography(POINT, 4326),
    distance_m float,
    address text,
    city varchar(100),
    connector_types text[],
    connector_power decimal(6,1)[]
)
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
BEGIN
    RETURN QUERY
    WITH nearby_stations AS (
        SELECT
            s.id,
            s.name,
            s.visibility,
            s.location,
            s.address,
            s.city,
            ARRAY_AGG(c.connector) FILTER (WHERE c.connector IS NOT NULL) AS connector_types,
            ARRAY_AGG(c.power_kw) FILTER (WHERE c.power_kw IS NOT NULL) AS connector_power,
            ST_Distance(
                s.location,
                ST_MakePoint($2, $1)::geography
            ) AS distance_m
        FROM inventory.station s
        LEFT JOIN inventory.charger c ON c.station_id = s.id AND c.deleted_at IS NULL
        WHERE
            s.deleted_at IS NULL
            AND s.status = p_status_filter
            AND (p_visibility_filter = 'all' OR s.visibility = p_visibility_filter)
            AND ST_DWithin(
                s.location::geography,
                ST_MakePoint($2, $1)::geography,
                p_radius_km * 1000
            )
        GROUP BY
            s.id, s.name, s.visibility, s.location, s.address, s.city
    )
    SELECT
        id,
        name,
        visibility,
        location,
        distance_m,
        address,
        city,
        connector_types,
        connector_power
    FROM nearby_stations
    ORDER BY distance_m
    LIMIT p_limit;
END;
$$;

-- Function to find all stations (for filtering) - only active stations
CREATE OR REPLACE FUNCTION gis.find_all_active_stations(
    p_limit int DEFAULT 1000
)
RETURNS TABLE (
    id varchar(32),
    name varchar(255),
    visibility station_visibility,
    status station_status,
    location geography(POINT, 4326),
    address text,
    city varchar(100)
)
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        id,
        name,
        visibility,
        status,
        location,
        address,
        city
    FROM inventory.station
    WHERE
        deleted_at IS NULL
        AND status = 'active'
    ORDER BY id
    LIMIT p_limit;
END;
$$;

-- Function to get import statistics
CREATE OR REPLACE FUNCTION gis.get_import_stats()
RETURNS TABLE (
    region varchar(100),
    total_imports int,
    total_stations int,
    last_import_time timestamptz
) 
LANGUAGE plpgsql
STABLE
PARALLEL SAFE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        region,
        COUNT(*) as total_imports,
        SUM(stations_imported) as total_stations,
        MAX(start_time) as last_import_time
    FROM gis.import_log
    GROUP BY region
    ORDER BY last_import_time DESC;
END;
$$;

CREATE TABLE gis.import_log (
    id              bigserial PRIMARY KEY,
    region          varchar(100) NOT NULL,
    bbox            jsonb NOT NULL,
    stations_imported int DEFAULT 0,
    stations_updated int DEFAULT 0,
    stations_failed int DEFAULT 0,
    status          varchar(50) NOT NULL DEFAULT 'pending', -- pending, running, completed, failed
    error_message   text,
    start_time      timestamptz NOT NULL DEFAULT now(),
    end_time        timestamptz
);

CREATE INDEX idx_import_log_region ON gis.import_log (region);
CREATE INDEX idx_import_log_status ON gis.import_log (status);
CREATE INDEX idx_import_log_start_time ON gis.import_log (start_time DESC);

-- ============================================================
-- Schema: users
-- ============================================================
CREATE SCHEMA IF NOT EXISTS users;

CREATE TABLE users.driver_profile (
    id              varchar(32) PRIMARY KEY,
    keycloak_id     uuid NOT NULL UNIQUE,
    display_name    varchar(100),
    email           varchar(255) NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_driver_keycloak ON users.driver_profile (keycloak_id);
CREATE INDEX idx_driver_email ON users.driver_profile (email);

CREATE TABLE users.driver_favorite (
    driver_id       varchar(32) NOT NULL REFERENCES users.driver_profile(id) ON DELETE CASCADE,
    station_id      varchar(32) NOT NULL REFERENCES inventory.station(id) ON DELETE CASCADE,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (driver_id, station_id)
);

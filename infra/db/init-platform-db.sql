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
    partner_id      varchar(32) NOT NULL REFERENCES inventory.partner(id) ON DELETE RESTRICT,
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

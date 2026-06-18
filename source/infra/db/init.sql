-- BorneMap Platform Database
-- PostgreSQL 16 + PostGIS 3.4
-- Initialization script (idempotent)

-- ============================================================================
-- Extensions
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;

-- ============================================================================
-- Schemas
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS gis;
CREATE SCHEMA IF NOT EXISTS inventory;

-- ============================================================================
-- GIS schema — spatial analysis & mapping layers
-- ============================================================================
-- Tables are created by osm2pgsql during import (osm_roads, osm_point, etc.)

-- --------------------------------------------------------------------------
-- gis.osm_stations — station geometry mirrored from inventory for GIS analyses
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gis.osm_stations (
    osm_id      TEXT    PRIMARY KEY,        -- Matches inventory.station.id (e.g. STA_001)
    name        TEXT    NOT NULL,
    tags        JSONB   DEFAULT '{}'::jsonb,
    way         GEOMETRY(Point, 4326) NOT NULL   -- 'way' matches osm2pgsql convention
);

CREATE INDEX IF NOT EXISTS idx_osm_stations_way
    ON gis.osm_stations USING GIST (way);

-- ============================================================================
-- Inventory schema — application infrastructure
-- ============================================================================

-- --------------------------------------------------------------------------
-- inventory.partner
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory.partner (
    id            TEXT          PRIMARY KEY,          -- NanoID prefix OPR_
    name          TEXT          NOT NULL,
    contact_email TEXT,
    contact_phone TEXT,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMPTZ                      -- Soft-delete
);

-- --------------------------------------------------------------------------
-- inventory.station
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory.station (
    id            TEXT            PRIMARY KEY,       -- NanoID prefix STA_
    partner_id    TEXT            REFERENCES inventory.partner(id),
    name          TEXT            NOT NULL,
    address       TEXT,
    city          TEXT,
    latitude      DOUBLE PRECISION NOT NULL
                    CHECK (latitude >= -90 AND latitude <= 90),
    longitude     DOUBLE PRECISION NOT NULL
                    CHECK (longitude >= -180 AND longitude <= 180),
    location      GEOGRAPHY(Point, 4326)
                    GENERATED ALWAYS AS (
                        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                    ) STORED,
    is_private    BOOLEAN         NOT NULL DEFAULT FALSE,
    metadata      JSONB,
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMPTZ                          -- Soft-delete
);

-- --------------------------------------------------------------------------
-- inventory.charger
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory.charger (
    id              TEXT            PRIMARY KEY,     -- NanoID prefix CHG_
    station_id      TEXT            NOT NULL
                        REFERENCES inventory.station(id),
    connector_type  TEXT            NOT NULL,         -- Type2, CCS, CHAdeMO
    power_kw        NUMERIC(5,1)    NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'unknown',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ                        -- Soft-delete
);

-- --------------------------------------------------------------------------
-- Indexes
-- --------------------------------------------------------------------------
-- Station spatial index (GIST on geography column)
CREATE INDEX IF NOT EXISTS idx_station_location
    ON inventory.station USING GIST (location);

-- Station BTREE indexes
CREATE INDEX IF NOT EXISTS idx_station_partner_id
    ON inventory.station (partner_id);
CREATE INDEX IF NOT EXISTS idx_station_city
    ON inventory.station (city);

-- Charger BTREE indexes
CREATE INDEX IF NOT EXISTS idx_charger_station_id
    ON inventory.charger (station_id);

-- ============================================================================
-- Sync outbox — station change capture for GIS layer replication
-- ============================================================================

CREATE TABLE IF NOT EXISTS inventory.sync_outbox (
    id            BIGSERIAL    PRIMARY KEY,
    entity_type   VARCHAR(50)  NOT NULL,       -- 'STATION'
    entity_id     VARCHAR(50)  NOT NULL,       -- e.g. 'STA_001'
    action_type   VARCHAR(20)  NOT NULL,       -- 'INSERT', 'UPDATE', 'DELETE'
    processed     BOOLEAN      DEFAULT FALSE,
    retry_count   INT          DEFAULT 0,
    created_at    TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP,
    processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_unprocessed
    ON inventory.sync_outbox (processed, created_at);

-- ============================================================================
-- Seed data
-- ============================================================================

INSERT INTO inventory.partner (id, name, contact_email)
VALUES ('OPR_001', 'BorneMap Tunisia', 'contact@bornemap.tn')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.station (id, partner_id, name, address, city, latitude, longitude) VALUES
    ('STA_001', 'OPR_001', 'Tunis Centre', 'Avenue Habib Bourguiba', 'Tunis',   36.8005, 10.1810),
    ('STA_002', 'OPR_001', 'Tunis Lafayette', 'Rue de Marseille', 'Tunis',   36.8100, 10.1750),
    ('STA_003', 'OPR_001', 'Tunis Berges du Lac', 'Les Berges du Lac', 'Tunis',   36.8300, 10.1900),
    ('STA_004', 'OPR_001', 'Tunis Carthage', 'Route de La Goulette', 'Tunis',   36.8550, 10.2000),
    ('STA_005', 'OPR_001', 'Sousse Corniche', 'Boulevard de la Corniche', 'Sousse',  35.8280, 10.6430),
    ('STA_006', 'OPR_001', 'Sousse Centre', 'Avenue Mohamed V', 'Sousse',  35.8250, 10.6380),
    ('STA_007', 'OPR_001', 'Sousse Port', 'Port de Plaisance', 'Sousse',  35.8320, 10.6500),
    ('STA_008', 'OPR_001', 'Sfax Centre', 'Avenue Habib Bourguiba', 'Sfax',  34.7390, 10.7550),
    ('STA_009', 'OPR_001', 'Sfax Gare', 'Gare de Sfax', 'Sfax',  34.7430, 10.7600),
    ('STA_010', 'OPR_001', 'Sfax Plage', 'Route de la Plage', 'Sfax',  34.7310, 10.7700)
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- Trigger: queue station changes into sync_outbox (created AFTER seed data)
-- ============================================================================
CREATE OR REPLACE FUNCTION inventory.tr_queue_station_sync()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO inventory.sync_outbox (entity_type, entity_id, action_type)
        VALUES ('STATION', NEW.id, 'INSERT');
    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL THEN
            INSERT INTO inventory.sync_outbox (entity_type, entity_id, action_type)
            VALUES ('STATION', NEW.id, 'DELETE');
        ELSE
            INSERT INTO inventory.sync_outbox (entity_type, entity_id, action_type)
            VALUES ('STATION', NEW.id, 'UPDATE');
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO inventory.sync_outbox (entity_type, entity_id, action_type)
        VALUES ('STATION', OLD.id, 'DELETE');
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER station_sync_trigger
AFTER INSERT OR UPDATE OR DELETE ON inventory.station
FOR EACH ROW
EXECUTE FUNCTION inventory.tr_queue_station_sync();

-- ============================================================================
-- Functions
-- ============================================================================

DROP FUNCTION IF EXISTS inventory.get_nearby_stations;

-- --------------------------------------------------------------------------
-- gis.get_nearby_stations — geodesic proximity search
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gis.get_nearby_stations(
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
)
RETURNS TABLE(
    station_id      TEXT,
    station_name    TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION,
    is_private      BOOLEAN,
    partner_name    TEXT
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    search_point GEOGRAPHY;
BEGIN
    IF lat < -90 OR lat > 90 THEN
        RAISE EXCEPTION 'Latitude must be between -90 and 90, got %', lat;
    END IF;
    IF lng < -180 OR lng > 180 THEN
        RAISE EXCEPTION 'Longitude must be between -180 and 180, got %', lng;
    END IF;
    IF radius_meters <= 0 THEN
        RAISE EXCEPTION 'Radius must be positive, got %', radius_meters;
    END IF;

    search_point := ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography;

    RETURN QUERY
    SELECT
        s.id,
        s.name,
        s.latitude,
        s.longitude,
        ST_Distance(s.location, search_point) AS distance_meters,
        s.is_private,
        p.name AS partner_name
    FROM inventory.station s
    LEFT JOIN inventory.partner p ON p.id = s.partner_id
    WHERE
        s.deleted_at IS NULL
        AND ST_DWithin(s.location, search_point, radius_meters)
    ORDER BY distance_meters ASC;
END;
$$;

-- --------------------------------------------------------------------------
-- gis.sync_station — upsert a single station into gis.osm_stations
-- Shared by seed data and process_sync_outbox
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gis.sync_station(target_id TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gis.osm_stations (osm_id, name, tags, way)
    SELECT
        s.id,
        s.name,
        jsonb_build_object(
            'operator', s.partner_id,
            'source', 'bornemap_inventory',
            'city', s.city,
            'address', s.address
        ) || COALESCE(s.metadata, '{}'::jsonb),
        ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)
    FROM inventory.station s
    WHERE s.id = target_id AND s.deleted_at IS NULL
    ON CONFLICT (osm_id) DO UPDATE SET
        name = EXCLUDED.name,
        tags = gis.osm_stations.tags || EXCLUDED.tags,
        way  = EXCLUDED.way;
END;
$$;

-- --------------------------------------------------------------------------
-- gis.process_sync_outbox — drains outbox into gis.osm_stations
-- Call periodically (e.g. via pg_cron or app-level scheduler)
-- Failed rows retain processed=FALSE and increment retry_count for retry
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gis.process_sync_outbox(
    max_retries INT DEFAULT 3
)
RETURNS TABLE(
    processed_id    BIGINT,
    entity_id       TEXT,
    action_type     TEXT,
    status          TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT o.id, o.entity_id, o.action_type
        FROM inventory.sync_outbox o
        WHERE o.processed = FALSE AND o.retry_count < max_retries
        ORDER BY o.id ASC
        FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            IF r.action_type = 'DELETE' THEN
                DELETE FROM gis.osm_stations WHERE osm_id = r.entity_id;
            ELSE
                PERFORM gis.sync_station(r.entity_id);
            END IF;

            UPDATE inventory.sync_outbox
            SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
            WHERE id = r.id;

            processed_id   := r.id;
            entity_id      := r.entity_id;
            action_type    := r.action_type;
            status         := 'OK';
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            UPDATE inventory.sync_outbox
            SET retry_count = retry_count + 1
            WHERE id = r.id;

            processed_id   := r.id;
            entity_id      := r.entity_id;
            action_type    := r.action_type;
            status         := 'ERROR: ' || SQLERRM;
            RETURN NEXT;
        END;
    END LOOP;
END;
$$;

-- ============================================================================
-- Seed gis.osm_stations from existing inventory data
-- ============================================================================
INSERT INTO gis.osm_stations (osm_id, name, tags, way)
SELECT
    s.id,
    s.name,
    jsonb_build_object(
        'operator', s.partner_id,
        'source', 'bornemap_inventory',
        'city', s.city,
        'address', s.address
    ) || COALESCE(s.metadata, '{}'::jsonb),
    ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)
FROM inventory.station s
WHERE s.deleted_at IS NULL
ON CONFLICT (osm_id) DO NOTHING;

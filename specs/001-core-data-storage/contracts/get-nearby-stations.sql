-- Contract: gis.get_nearby_stations
--
-- Finds charging stations within a given radius of a coordinate,
-- sorted by geodesic distance ascending.
--
-- Version: 1.1.0
-- Status: DRAFT
-- Owner: Driver Service (called at the API layer in Sprint 1.2)

-- Input:
--   lng            DOUBLE PRECISION   — WGS84 longitude (-180 to 180)
--   lat            DOUBLE PRECISION   — WGS84 latitude (-90 to 90)
--   radius_meters  DOUBLE PRECISION   — Search radius in meters (> 0)
--
-- Returns:
--   station_id       TEXT             — STA_ prefixed NanoID
--   station_name     TEXT             — Display name
--   latitude         DOUBLE PRECISION — WGS84 latitude
--   longitude        DOUBLE PRECISION — WGS84 longitude
--   distance_meters  DOUBLE PRECISION — Geodesic distance from input point
--   is_private       BOOLEAN          — True if home charger
--   partner_name     TEXT             — Operator name (NULL if no partner)
--
-- Errors:
--   P0001 (raise_exception) if lat/lng out of valid range
--   P0001 (raise_exception) if radius_meters <= 0
--
-- Empty result: Returns 0 rows (not NULL) when no stations match
--
-- Note: Moved from inventory to gis schema in Sprint 1.1 sync refactor.
--       The function still queries inventory.station and inventory.partner.

CREATE OR REPLACE FUNCTION gis.get_nearby_stations(
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
)
RETURNS TABLE(
    station_id TEXT,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION,
    is_private BOOLEAN,
    partner_name TEXT
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    search_point GEOGRAPHY;
BEGIN
    -- Validate inputs
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

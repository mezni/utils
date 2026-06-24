CREATE OR REPLACE FUNCTION gis.find_nearby_stations(
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    radius INTEGER DEFAULT 5000,
    limit INTEGER DEFAULT 50
)
RETURNS TABLE(
    station_id TEXT,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    distance_km DOUBLE PRECISION
)
LANGUAGE sql STABLE
AS $$
    SELECT
        s.station_id,
        s.name,
        s.lat,
        s.lon,
        (6371 * acos(
            cos(radians(lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(lon)) +
            sin(radians(lat)) * sin(radians(s.lat))
        ))::DOUBLE PRECISION AS distance_km
    FROM gis.osm_charging_stations s
    WHERE s.deleted_at IS NULL
      AND s.is_test = FALSE
      AND (6371 * acos(
            cos(radians(lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(lon)) +
            sin(radians(lat)) * sin(radians(s.lat))
          ) * 1000) <= radius
    ORDER BY distance_km ASC, s.station_id ASC
    LIMIT limit;
$$;

CREATE OR REPLACE FUNCTION gis.find_nearby_stations(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_radius INTEGER DEFAULT 5000,
    p_max_results INTEGER DEFAULT 50
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
            cos(radians(p_lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(p_lon)) +
            sin(radians(p_lat)) * sin(radians(s.lat))
        ))::DOUBLE PRECISION AS distance_km
    FROM gis.osm_charging_stations s
    WHERE s.deleted_at IS NULL
      AND s.is_test = FALSE
      AND (6371 * acos(
            cos(radians(p_lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(p_lon)) +
            sin(radians(p_lat)) * sin(radians(s.lat))
          ) * 1000) <= p_radius
    ORDER BY distance_km ASC, s.station_id ASC
    LIMIT p_max_results;
$$;

CREATE OR REPLACE FUNCTION inventory.find_nearby_stations(
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
)
RETURNS TABLE (
    station_id VARCHAR,
    name VARCHAR,
    distance_meters DOUBLE PRECISION
)
LANGUAGE SQL
STABLE
AS $$
    SELECT
        s.station_id,
        s.name,
        ST_Distance(s.location, ST_SetSRID(ST_MakePoint(lng, lat), 4326)::GEOGRAPHY) AS distance_meters
    FROM inventory.stations s
    WHERE
        s.is_deleted = FALSE
        AND s.is_test = FALSE
        AND ST_DWithin(s.location, ST_SetSRID(ST_MakePoint(lng, lat), 4326)::GEOGRAPHY, radius_meters)
    ORDER BY distance_meters ASC
$$;

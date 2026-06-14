-- source/database/platform_db/functions.sql

CREATE OR REPLACE FUNCTION gis.get_nearby_stations(
    driver_longitude DOUBLE PRECISION,
    driver_latitude DOUBLE PRECISION,
    search_radius_meters DOUBLE PRECISION DEFAULT 5000.0
)
RETURNS TABLE (
    station_id VARCHAR(64),
    station_name VARCHAR(255),
    station_address TEXT,
    distance_meters DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    available_chargers JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.id AS station_id,
        s.name AS station_name,
        s.address AS station_address,
        -- 1. Compute exact spherical distance using optimized native geodesics
        ST_Distance(
            s.coordinates::geography, 
            ST_SetSRID(ST_MakePoint(driver_longitude, driver_latitude), 4326)::geography
        ) AS distance_meters,
        ST_Y(s.coordinates) AS latitude,
        ST_X(s.coordinates) AS longitude,
        
        -- 2. Aggregate child chargers into a single, structured JSONB array payload
        COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'charger_id', c.id,
                        'code', c.identifier_code,
                        'plug_type', c.plug_type_code,
                        'max_power_kw', c.max_power_kw,
                        'status', c.status
                    )
                )
                FROM inventory.chargers c
                WHERE c.station_id = s.id
            ), 
            '[]'::jsonb
        ) AS available_chargers
    FROM 
        gis.osm_stations s
    WHERE 
        -- 3. Utilize GIST structural indexing for fast bounding-box filtering
        ST_DWithin(
            s.coordinates::geography, 
            ST_SetSRID(ST_MakePoint(driver_longitude, driver_latitude), 4326)::geography, 
            search_radius_meters
        )
        AND s.is_available = TRUE -- Only return active, operational discovery pins
    ORDER BY 
        distance_meters ASC;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

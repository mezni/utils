CREATE OR REPLACE FUNCTION gis.find_nearby_stations(
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    radius INTEGER DEFAULT 5000,
    limit_count INTEGER DEFAULT 50
)
RETURNS TABLE(
    station_id TEXT,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    distance_km DOUBLE PRECISION
)
LANGUAGE SQL
STABLE
PARALLEL SAFE
AS $$
    SELECT
        cs.station_id,
        cs.name,
        cs.lat,
        cs.lon,
        (
            6371.0 * 2.0 * ASIN(
                SQRT(
                    POWER(SIN(RADIANS(cs.lat - lat) / 2.0), 2)
                    + COS(RADIANS(lat))
                    * COS(RADIANS(cs.lat))
                    * POWER(SIN(RADIANS(cs.lon - lon) / 2.0), 2)
                )
            )
        )::DOUBLE PRECISION AS distance_km
    FROM gis.osm_charging_stations cs
    WHERE
        (
            6371.0 * 2.0 * ASIN(
                SQRT(
                    POWER(SIN(RADIANS(cs.lat - lat) / 2.0), 2)
                    + COS(RADIANS(lat))
                    * COS(RADIANS(cs.lat))
                    * POWER(SIN(RADIANS(cs.lon - lon) / 2.0), 2)
                )
            )
        ) * 1000.0 <= radius
        AND cs.lat IS NOT NULL
        AND cs.lon IS NOT NULL
    ORDER BY distance_km ASC
    LIMIT limit_count;
$$;

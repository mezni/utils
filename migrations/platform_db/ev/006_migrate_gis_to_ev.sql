INSERT INTO ev.stations (station_id, osm_id, name, location, created_at)
SELECT
    'STA-' || substr(md5(random()::text || g.station_id), 1, 12),
    g.osm_id::BIGINT,
    g.name,
    ST_SetSRID(ST_MakePoint(g.lon, g.lat), 4326)::geography,
    NOW()
FROM gis.osm_charging_stations g
WHERE g.deleted_at IS NULL
  AND g.is_test = FALSE
  AND g.osm_id IS NOT NULL
ON CONFLICT (osm_id) DO NOTHING;

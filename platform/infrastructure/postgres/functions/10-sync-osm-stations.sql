CREATE OR REPLACE FUNCTION inventory.sync_osm_charging_stations()
RETURNS TABLE (
    station_id_out VARCHAR,
    name_out TEXT,
    imported BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    new_id VARCHAR;
BEGIN
    FOR rec IN
        SELECT * FROM gis.osm_charging_stations_temp
        WHERE osm_id NOT IN (
            SELECT osm_id FROM inventory.stations WHERE osm_id IS NOT NULL AND source_id = (
                SELECT id FROM inventory.data_sources WHERE name = 'osm'
            )
        )
        ORDER BY fetched_at ASC
    LOOP
        new_id := 'STA-' || encode(gen_random_bytes(9), 'base64');
        new_id := regexp_replace(new_id, '[^A-Za-z0-9_-]', 'X', 'g');
        new_id := left(new_id, 12);
        new_id := 'STA-' || new_id;

        INSERT INTO inventory.stations (
            station_id, name, location, tags, source_id,
            is_test, osm_id
        ) VALUES (
            new_id,
            COALESCE(rec.raw_tags -> 'name', 'Charging Station ' || rec.osm_id),
            ST_SetSRID(ST_MakePoint(rec.lng, rec.lat), 4326)::GEOGRAPHY,
            rec.raw_tags,
            (SELECT id FROM inventory.data_sources WHERE name = 'osm'),
            FALSE,
            rec.osm_id
        )
        ON CONFLICT (osm_id) DO NOTHING;

        station_id_out := new_id;
        name_out := COALESCE(rec.raw_tags -> 'name', 'Charging Station ' || rec.osm_id);
        imported := TRUE;
        RETURN NEXT;
    END LOOP;
END;
$$;

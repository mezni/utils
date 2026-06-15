CREATE OR REPLACE FUNCTION inventory.update_modified_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_timestamp_plug_types
    BEFORE UPDATE ON configuration.plug_types
    FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();

CREATE TRIGGER trg_auto_timestamp_partners
    BEFORE UPDATE ON inventory.partners
    FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();

CREATE TRIGGER trg_auto_timestamp_stations
    BEFORE UPDATE ON inventory.stations
    FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();

CREATE TRIGGER trg_auto_timestamp_chargers
    BEFORE UPDATE ON inventory.chargers
    FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();

CREATE OR REPLACE FUNCTION gis.sync_inventory_station_to_gis_cache()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_live = FALSE THEN
        DELETE FROM gis.osm_stations WHERE id = NEW.id;
        RETURN NEW;
    END IF;

    INSERT INTO gis.osm_stations (
        id, name, address, coordinates, source, is_available, last_modified_at
    )
    VALUES (
        NEW.id, NEW.name, NEW.address,
        ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326),
        'PLATFORM_SYNC', (NEW.availability = 'AVAILABLE'), NEW.updated_at
    )
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name,
        address = EXCLUDED.address,
        coordinates = EXCLUDED.coordinates,
        is_available = EXCLUDED.is_available,
        last_modified_at = EXCLUDED.last_modified_at;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER trg_replicate_station_to_gis_cache
    AFTER INSERT OR UPDATE ON inventory.stations
    FOR EACH ROW
    EXECUTE FUNCTION gis.sync_inventory_station_to_gis_cache();

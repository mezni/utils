-- Rollback: GIS schema

-- Revoke permissions
REVOKE ALL PRIVILEGES ON SCHEMA gis FROM bornemap_driver;
REVOKE ALL PRIVILEGES ON TABLE gis.osm_charging_stations_temp FROM bornemap_driver;
REVOKE ALL PRIVILEGES ON TABLE gis.osm_charging_stations FROM bornemap_driver;
REVOKE USAGE ON SCHEMA gis FROM bornemap_admin, bornemap_analytics_reader;
REVOKE SELECT ON TABLE gis.osm_charging_stations FROM bornemap_admin, bornemap_analytics_reader;

-- Drop tables
DROP TABLE IF EXISTS gis.osm_charging_stations;
DROP TABLE IF EXISTS gis.osm_charging_stations_temp;

-- Drop schema
DROP SCHEMA IF EXISTS gis;

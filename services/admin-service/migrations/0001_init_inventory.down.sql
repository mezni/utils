-- Rollback: inventory schema

-- Revoke permissions
REVOKE ALL PRIVILEGES ON SCHEMA inventory FROM bornemap_admin;
REVOKE ALL PRIVILEGES ON TABLE inventory.stations FROM bornemap_admin;
REVOKE ALL PRIVILEGES ON TABLE inventory.partners FROM bornemap_admin;
REVOKE ALL PRIVILEGES ON TABLE inventory.chargers FROM bornemap_admin;
REVOKE ALL PRIVILEGES ON TABLE inventory.connectors FROM bornemap_admin;
REVOKE USAGE ON SCHEMA inventory FROM bornemap_driver;
REVOKE SELECT ON TABLE inventory.stations FROM bornemap_driver;
REVOKE SELECT ON TABLE inventory.chargers FROM bornemap_driver;

-- Drop tables
DROP TABLE IF EXISTS inventory.connectors;
DROP TABLE IF EXISTS inventory.chargers;
DROP TABLE IF EXISTS inventory.partners;
DROP TABLE IF EXISTS inventory.stations;

-- Drop schema
DROP SCHEMA IF EXISTS inventory;

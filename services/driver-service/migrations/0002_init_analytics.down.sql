-- Rollback: analytics schemas

-- Revoke permissions
REVOKE ALL PRIVILEGES ON SCHEMA telemetry FROM bornemap_analytics_writer;
REVOKE ALL PRIVILEGES ON TABLE telemetry.raw_events FROM bornemap_analytics_writer;
REVOKE ALL PRIVILEGES ON TABLE telemetry.analytics_events FROM bornemap_analytics_writer;
REVOKE ALL PRIVILEGES ON TABLE telemetry.system_events FROM bornemap_analytics_writer;
REVOKE USAGE ON SCHEMA telemetry FROM bornemap_analytics_reader, bornemap_driver;
REVOKE SELECT ON TABLE telemetry.raw_events FROM bornemap_analytics_reader, bornemap_driver;
REVOKE SELECT ON TABLE telemetry.analytics_events FROM bornemap_analytics_reader;
REVOKE SELECT ON TABLE telemetry.system_events FROM bornemap_analytics_reader, bornemap_driver;

-- Drop tables
DROP TABLE IF EXISTS telemetry.system_events;
DROP TABLE IF EXISTS telemetry.analytics_events;
DROP TABLE IF EXISTS telemetry.raw_events;

-- Drop schemas
DROP SCHEMA IF EXISTS telemetry;

-- Note: analytics_events and system_events schemas are dropped with telemetry

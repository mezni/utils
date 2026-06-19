-- Schemas
\c platform_db

CREATE SCHEMA IF NOT EXISTS gis;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS users;

-- Roles for platform_db
CREATE ROLE auth_service_role WITH LOGIN PASSWORD 'auth_dev_pass';
GRANT USAGE ON SCHEMA users TO auth_service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA users TO auth_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA users GRANT ALL ON TABLES TO auth_service_role;

CREATE ROLE admin_service_role WITH LOGIN PASSWORD 'admin_dev_pass';
GRANT USAGE ON SCHEMA gis TO admin_service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gis TO admin_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA gis GRANT ALL ON TABLES TO admin_service_role;
GRANT USAGE ON SCHEMA inventory TO admin_service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA inventory TO admin_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA inventory GRANT ALL ON TABLES TO admin_service_role;

CREATE ROLE driver_service_role WITH LOGIN PASSWORD 'driver_dev_pass';
GRANT USAGE ON SCHEMA inventory TO driver_service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO driver_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA inventory GRANT SELECT ON TABLES TO driver_service_role;

-- analytics_db permissions
\c analytics_db

CREATE SCHEMA IF NOT EXISTS public;

CREATE ROLE admin_analytics_role WITH LOGIN PASSWORD 'analytics_dev_pass';
GRANT USAGE ON SCHEMA public TO admin_analytics_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin_analytics_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO admin_analytics_role;

-- Keycloak internal user
\c keycloak_db
CREATE ROLE keycloak WITH LOGIN PASSWORD 'keycloak_dev_pass';
GRANT ALL PRIVILEGES ON DATABASE keycloak_db TO keycloak;
GRANT ALL ON SCHEMA public TO keycloak;

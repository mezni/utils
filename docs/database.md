# Database Design

## Schema Overview

```
users
├── accounts          # User authentication data
│   ├── id            UUID PK
│   ├── email         VARCHAR UNIQUE
│   ├── password_hash VARCHAR
│   ├── role          VARCHAR (driver|partner|admin)
│   └── timestamps

ev
├── partners           # Charging network operators
│   ├── id             UUID PK
│   ├── name           VARCHAR
│   ├── contact_email  VARCHAR
│   └── timestamps
│
├── charging_stations  # Physical charging locations
│   ├── id             UUID PK
│   ├── partner_id     UUID FK → partners.id
│   ├── name           VARCHAR
│   ├── address        TEXT
│   └── timestamps
│
├── connectors         # Individual charging points
│   ├── id             UUID PK
│   ├── station_id     UUID FK → charging_stations.id
│   ├── status         ENUM (available|charging|out_of_order|offline)
│   ├── connector_type VARCHAR
│   ├── power_kw       NUMERIC
│   └── timestamps

gis
├── station_locations  # PostGIS spatial data
│   ├── id             UUID PK
│   ├── station_id     UUID FK → ev.charging_stations.id
│   ├── location       GEOGRAPHY(Point, 4326)
│   └── created_at
│
│   INDEX: GiST on location
```

## Design Rules

- All primary keys are UUID v4
- FK constraints enforce referential integrity
- Schema separation provides logical isolation without multiple databases
- Station may exist without GIS location (optional spatial data)
- Station status is derived from connectors (never stored)

## Spatial Query Pattern

```sql
SELECT s.id, s.name, ST_Distance(l.location, ST_MakePoint($1, $2)::geography) AS distance
FROM ev.charging_stations s
JOIN gis.station_locations l ON l.station_id = s.id
WHERE ST_DWithin(l.location, ST_MakePoint($1, $2)::geography, $3)
ORDER BY distance;
```

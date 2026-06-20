# Data Model: EV Charging Platform Foundation

## Entity Relationship

```
Partner (1) ──→ (N) Station (1) ──→ (N) Charger (1) ──→ (N) Connector
```

## Entities

### Partner
| Field | Type | Constraints |
|-------|------|-------------|
| id | VARCHAR(15) | PK, prefix `PAR-` + nanoid(12) |
| name | VARCHAR(255) | NOT NULL |
| type | ENUM | `INDIVIDUAL`, `COMPANY` |
| verification_status | ENUM | `PENDING`, `VERIFIED`, `SUSPENDED` |
| metadata | JSONB | DEFAULT '{}' |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

### Station
| Field | Type | Constraints |
|-------|------|-------------|
| id | VARCHAR(15) | PK, prefix `STA-` + nanoid(12) |
| partner_id | VARCHAR(15) | FK → Partner(id), NOT NULL |
| name | VARCHAR(255) | NOT NULL |
| location | GEOGRAPHY(POINT, 4326) | NOT NULL, GiST indexed |
| address | TEXT | |
| status | ENUM | `ACTIVE`, `MAINTENANCE`, `CLOSED` |
| osm_id | BIGINT | nullable, for OSM linking |
| data_source_id | INTEGER | FK → data_sources(id), nullable |
| tags | HSTORE | |
| is_test | BOOLEAN | DEFAULT FALSE |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

### Charger
| Field | Type | Constraints |
|-------|------|-------------|
| id | VARCHAR(15) | PK, prefix `CHR-` + nanoid(12) |
| station_id | VARCHAR(15) | FK → Station(id) ON DELETE CASCADE |
| vendor | VARCHAR(255) | |
| model | VARCHAR(255) | |
| firmware_version | VARCHAR(50) | |
| serial_number | VARCHAR(100) | |
| max_power_kw | DECIMAL(6,2) | NOT NULL |
| status | ENUM | `ACTIVE`, `OFFLINE`, `MAINTENANCE`, `RETIRED` |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

### Connector
| Field | Type | Constraints |
|-------|------|-------------|
| id | VARCHAR(15) | PK, prefix `CON-` + nanoid(12) |
| charger_id | VARCHAR(15) | FK → Charger(id) ON DELETE CASCADE |
| connector_type | VARCHAR(50) | FK → connector_types(code) |
| current_type | VARCHAR(10) | FK → current_types(code) |
| max_power_kw | DECIMAL(6,2) | NOT NULL |
| status | ENUM | `AVAILABLE`, `IN_USE`, `OUT_OF_SERVICE` |
| available_count | INTEGER | DEFAULT 0 |
| total_count | INTEGER | DEFAULT 1 |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

### Sync Job
| Field | Type | Constraints |
|-------|------|-------------|
| id | VARCHAR(15) | PK, prefix `JOB-` + nanoid(12) |
| source_type | VARCHAR(50) | e.g. 'osm', 'ocpi', 'manual' |
| source_external_id | VARCHAR(255) | nullable |
| status | ENUM | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED` |
| records_imported | INTEGER | DEFAULT 0 |
| records_updated | INTEGER | DEFAULT 0 |
| records_failed | INTEGER | DEFAULT 0 |
| error_message | TEXT | nullable |
| started_at | TIMESTAMPTZ | |
| completed_at | TIMESTAMPTZ | |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

## Lookup Tables

### connector_types
| code | description | standard |
|------|-------------|----------|
| CCS | Combined Charging System | IEC 62196-3 |
| CHAdeMO | CHAdeMO DC Fast Charge | IEC 62196-3 |
| TYPE2 | Type 2 (Mennekes) | IEC 62196-2 |
| TYPE1 | Type 1 (SAE J1772) | SAE J1772 |
| GB_T | GB/T | GB/T 20234 |

### current_types
| code | description |
|------|-------------|
| AC | Alternating Current |
| DC | Direct Current |

### data_sources
| Field | Type | Constraints |
|-------|------|-------------|
| id | SERIAL | PK |
| name | VARCHAR(100) | NOT NULL, UNIQUE |
| description | TEXT | |
| is_active | BOOLEAN | DEFAULT TRUE |

## Materialized Views

### mv_stations_geo
| Field | Source | Description |
|-------|--------|-------------|
| station_id | Station.id | |
| station_name | Station.name | |
| partner_id | Station.partner_id | |
| partner_name | Partner.name | |
| location | Station.location | GEOGRAPHY(Point, 4326) |
| status | Station.status | |
| tags | Station.tags | HSTORE |
| max_power_kw | MAX(Charger.max_power_kw) | Highest power at station |
| power_tier | Computed | See classification |
| total_chargers | COUNT(Charger.id) | |
| available_connectors | SUM(CASE WHEN Connector.status='AVAILABLE' THEN 1 ELSE 0 END) | |
| total_connectors | COUNT(Connector.id) | |

### Power Tier Classification
- `ultra_fast`: max_power_kw >= 150
- `fast`: max_power_kw >= 50
- `medium`: max_power_kw >= 22
- `slow`: max_power_kw < 22

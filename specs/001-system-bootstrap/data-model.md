# Data Model: System Bootstrap & Enforcement Kernel

**Feature**: 001-system-bootstrap
**Date**: 2026-06-21

## Overview

This document defines the data model for the system bootstrap phase. Since Sprint 0 is primarily infrastructure and scaffolding, the data model focuses on:

1. Database schema definitions
2. Repository directory structure
3. Configuration data models

## Database Schemas

### platform_db

Owned by: auth-service

**PostgreSQL Role**: `bornemap_auth`

**Permissions**: READ/WRITE (exclusive)

**Schema Ownership**:
```sql
CREATE SCHEMA auth;
GRANT ALL PRIVILEGES ON SCHEMA auth TO bornemap_auth;
GRANT USAGE ON SCHEMA auth TO bornemap_driver; -- limited read access if needed
GRANT USAGE ON SCHEMA auth TO bornemap_admin;  -- limited read access if needed
```

#### Schema: gis

Owned by: driver-service

**PostgreSQL Role**: `bornemap_driver`

**Permissions**: READ/WRITE (exclusive)

**Schema Ownership**:
```sql
CREATE SCHEMA gis;
GRANT ALL PRIVILEGES ON SCHEMA gis TO bornemap_driver;
GRANT USAGE ON SCHEMA gis TO bornemap_admin; -- admin-service needs to query GIS for dashboards
```

#### Schema: inventory

Owned by: admin-service

**PostgreSQL Role**: `bornemap_admin`

**Permissions**: READ/WRITE (exclusive)

**Schema Ownership**:
```sql
CREATE SCHEMA inventory;
GRANT ALL PRIVILEGES ON SCHEMA inventory TO bornemap_admin;
GRANT USAGE ON SCHEMA inventory TO bornemap_driver; -- driver-service needs to query inventory for nearby search
```

#### Schema: users

Purpose: Store user profiles and authentication data

**Table: users**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, NOT NULL | User identifier (Keycloak sub) |
| created_at | TIMESTAMP | NOT NULL | Account creation timestamp |
| updated_at | TIMESTAMP | NOT NULL | Last update timestamp |
| email | VARCHAR(255) | UNIQUE, NOT NULL | User email address |
| name | VARCHAR(255) | NOT NULL | User display name |

**Indexes**:
- `idx_users_email` on (email)

**Identity**: UUID (per constitution)

---

#### Schema: gis

Owned by: driver-service

**PostgreSQL Role**: `bornemap_driver`

**Permissions**: READ/WRITE (exclusive)

**Schema Ownership**:
```sql
CREATE SCHEMA gis;
GRANT ALL PRIVILEGES ON SCHEMA gis TO bornemap_driver;
GRANT USAGE ON SCHEMA gis TO bornemap_admin; -- admin-service needs to query GIS for dashboards
```

#### Schema: inventory

Owned by: admin-service

**PostgreSQL Role**: `bornemap_admin`

**Permissions**: READ/WRITE (exclusive)

**Schema Ownership**:
```sql
CREATE SCHEMA inventory;
GRANT ALL PRIVILEGES ON SCHEMA inventory TO bornemap_admin;
GRANT USAGE ON SCHEMA inventory TO bornemap_driver; -- driver-service needs to query inventory for nearby search
```

#### Reference Tables (Lookup Domain Model)

**Table: connector_types**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Type identifier |
| code | VARCHAR(30) | UNIQUE, NOT NULL | Type code (CCS2, TYPE2, CHAdeMO, etc.) |
| description | TEXT | NULL | Type description |

**Purpose**: Lookup table for connector types

---

**Table: current_types**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Type identifier |
| code | VARCHAR(10) | UNIQUE, NOT NULL | Current type (AC, DC) |
| description | TEXT | NULL | Type description |

**Purpose**: Lookup table for current types (AC/DC)

---

**Table: charger_statuses**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Status identifier |
| code | VARCHAR(30) | UNIQUE, NOT NULL | Status code (AVAILABLE, OFFLINE, FAULTED) |
| description | TEXT | NULL | Status description |

**Purpose**: Lookup table for charger statuses

---

**Table: connector_statuses**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | SERIAL | PRIMARY KEY | Status identifier |
| code | VARCHAR(30) | UNIQUE, NOT NULL | Status code (AVAILABLE, IN_USE, FAULTED) |
| description | TEXT | NULL | Status description |

**Purpose**: Lookup table for connector statuses

---

**Table: partners**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| partner_id | VARCHAR(20) | PRIMARY KEY, NOT NULL | Partner identifier (PRT-xxxx) |
| name | VARCHAR(255) | NOT NULL | Partner name |
| partner_type | VARCHAR(20) | NOT NULL, CHECK (partner_type IN ('INDIVIDUAL', 'COMPANY')) | Partner type |
| support_phone | VARCHAR(50) | NULL | Support phone number |
| support_email | VARCHAR(255) | NULL | Support email |
| is_verified | BOOLEAN | DEFAULT FALSE | Verification status |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_inventory_partners_name` on (name)
- `idx_inventory_partners_type` on (partner_type)

**Identity**: nanoid(12) with PREFIX "PRT"

**Purpose**: Partner/organization information management

---

**Table: stations**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| station_id | VARCHAR(20) | PRIMARY KEY, NOT NULL | Station identifier (STA-xxxx) |
| partner_id | VARCHAR(20) | FOREIGN KEY, NULL | Parent partner |
| osm_id | BIGINT | UNIQUE, NULL | OpenStreetMap ID |
| name | VARCHAR(255) | NOT NULL | Station name |
| address | TEXT | NULL | Physical address |
| location | GEOGRAPHY(Point, 4326) | NOT NULL | GPS coordinates |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_inventory_stations_location` (USING GIST, mandatory spatial index)

**Purpose**: Physical charging station locations

---

**Table: chargers**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| charger_id | VARCHAR(20) | PRIMARY KEY, NOT NULL | Charger identifier (CHR-xxxx) |
| station_id | VARCHAR(20) | FOREIGN KEY, NOT NULL | Parent station |
| name | VARCHAR(100) | NULL | Charger name |
| status_id | INT | FOREIGN KEY, NULL | Charger status |
| max_power_kw | DECIMAL(5,2) | NULL | Maximum power in kW |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_inventory_chargers_station` on (station_id)
- `idx_inventory_chargers_status` on (status_id)

**Identity**: nanoid(12) with PREFIX "CHR"

**Purpose**: Physical charging unit (renamed from EVSE_units)

**Note**: This is the physical charging unit. Connectors are defined in the connectors table.

---

**Table: connectors**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| connector_id | VARCHAR(20) | PRIMARY KEY, NOT NULL | Connector identifier (CON-xxxx) |
| charger_id | VARCHAR(20) | FOREIGN KEY, NOT NULL | Parent charger |
| connector_type_id | INT | FOREIGN KEY, NOT NULL | Connector type |
| current_type_id | INT | FOREIGN KEY, NOT NULL | Current type |
| status_id | INT | FOREIGN KEY, NOT NULL | Connector status |
| voltage | INT | NULL | Voltage in volts |
| amperage | INT | NULL | Amperage in amps |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_inventory_connectors_charger` on (charger_id)
- `idx_inventory_connectors_status` on (status_id)

**Identity**: nanoid(12) with PREFIX "CON"

**Purpose**: Fully normalized connector details

**Note**: Connectors are attached to chargers. Each charger can have multiple connectors with different types.

**Relationships**:
- connector_type_id → connector_types(id)
- current_type_id → current_types(id)
- status_id → connector_statuses(id)
- charger_id → chargers(charger_id)

---

### analytics_db

Owned by: driver-service (write), admin-service (read-only)

**PostgreSQL Roles**:
- `bornemap_analytics_writer` (driver-service)
- `bornemap_analytics_reader` (admin-service)

**Permissions**:
- bornemap_analytics_writer: ALL PRIVILEGES on telemetry_events, analytics_events, system_events
- bornemap_analytics_reader: SELECT only on telemetry_events, analytics_events, system_events

**Schema Ownership**:
```sql
CREATE SCHEMA telemetry;
CREATE SCHEMA analytics;
CREATE SCHEMA system;

-- Writer role (driver-service)
GRANT ALL PRIVILEGES ON SCHEMA telemetry TO bornemap_analytics_writer;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO bornemap_analytics_writer;
GRANT ALL PRIVILEGES ON SCHEMA system TO bornemap_analytics_writer;

-- Reader role (admin-service)
GRANT USAGE ON SCHEMA telemetry TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA telemetry TO bornemap_analytics_reader;

GRANT USAGE ON SCHEMA analytics TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO bornemap_analytics_reader;

GRANT USAGE ON SCHEMA system TO bornemap_analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA system TO bornemap_analytics_reader;
```

**Analytics Write Gate Enforcement**:
- CI gate 03_validate_analytics_gate.sh enforces static analysis
- Database-level roles enforce runtime write permissions
- No service can write to analytics_db except driver-service
- admin-service can only read from analytics_db

#### Schema: telemetry_events

Purpose: Store raw telemetry data from charging events

**Table: telemetry_events**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, NOT NULL | Event identifier |
| created_at | TIMESTAMP | NOT NULL | Event timestamp |
| station_id | STRING(16) | FOREIGN KEY, NOT NULL | Associated charging station |
| charger_id | STRING(16) | FOREIGN KEY, NULL | Associated charger (if applicable) |
| operator_id | STRING(16) | FOREIGN KEY, NOT NULL | Operator identifier |
| start_time | TIMESTAMP | NOT NULL | Charging session start time |
| end_time | TIMESTAMP | NULL | Charging session end time |
| energy_used_kwh | DECIMAL(8,2) | NULL | Energy consumed in kWh |
| status | VARCHAR(50) | NOT NULL | Session status (started, completed, failed) |
| payload | JSONB | NOT NULL | Additional event data |

**Indexes**:
- `idx_telemetry_events_station` on (station_id)
- `idx_telemetry_events_operator` on (operator_id)
- `idx_telemetry_events_created` on (created_at DESC)
- `idx_telemetry_events_start` on (start_time DESC)

**Identity**: UUID (per constitution)

**Write Access**: driver-service only (enforced by analytics gate)

---

#### Schema: analytics_events

Purpose: Aggregated analytics data for dashboards

**Table: analytics_events**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, NOT NULL | Event identifier |
| created_at | TIMESTAMP | NOT NULL | Event timestamp |
| metric_type | VARCHAR(100) | NOT NULL | Type of metric (daily, weekly, monthly) |
| station_id | STRING(16) | FOREIGN KEY, NOT NULL | Station identifier |
| metric_data | JSONB | NOT NULL | Metric data payload |
| window_start | TIMESTAMP | NOT NULL | Time window start |
| window_end | TIMESTAMP | NOT NULL | Time window end |

**Indexes**:
- `idx_analytics_events_station` on (station_id)
- `idx_analytics_events_type` on (metric_type)
- `idx_analytics_events_window` on (window_start DESC, window_end DESC)

**Identity**: UUID (per constitution)

**Write Access**: driver-service only (enforced by analytics gate)

---

#### Schema: system_events

Purpose: System-level events and alerts

**Table: system_events**

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, NOT NULL | Event identifier |
| created_at | TIMESTAMP | NOT NULL | Event timestamp |
| event_type | VARCHAR(100) | NOT NULL | Event type (error, warning, info) |
| severity | VARCHAR(50) | NOT NULL | Severity level (critical, high, medium, low) |
| source_service | VARCHAR(50) | NOT NULL | Service that generated event |
| message | TEXT | NOT NULL | Event message |
| metadata | JSONB | NULL | Additional event context |

**Indexes**:
- `idx_system_events_type` on (event_type)
- `idx_system_events_severity` on (severity)
- `idx_system_events_created` on (created_at DESC)

**Identity**: UUID (per constitution)

**Write Access**: driver-service only (enforced by analytics gate)

---

### keycloak_db

Owned by: Keycloak (no application logic)

**Note**: This database is auto-generated by Keycloak installation. It contains Keycloak internal tables for user authentication, realm configuration, and authorization.

**Key Tables**:
- `AUTHZ_POLICY` - Authorization policies
- `AUTHZ_POLICY_LINK` - Policy links
- `AUTHZ_RESOURCE` - Protected resources
- `AUTHZ_SCOPE` - Resource scopes
- `AUTHZ_PERMISSION` - User permissions
- `REALM` - Realm configuration
- `USER_ATTRIBUTE` - User attributes
- `USER_ROLE_MAPPING` - User role mappings
- `USER_SECRETS` - User credentials

**Note**: Sprint 0 includes creation of a realm export file. The database itself will be initialized by Keycloak setup.

## Repository Structure

### apps/packages/

Purpose: Frontend packages (contracts, UI, transport)

```
apps/packages/
├── ui-kit/                    # UI components only
│   ├── Cargo.toml
│   ├── src/
│   │   ├── components/
│   │   ├── layouts/
│   │   ├── tokens/
│   │   └── accessibility/
│   └── tests/
├── domain-types/              # Contracts only
│   ├── Cargo.toml
│   ├── src/
│   │   ├── dto/
│   │   ├── events/
│   │   └── ids/
│   └── tests/
└── client-core/               # Transport only
    ├── Cargo.toml
    ├── src/
    │   ├── api/
    │   ├── auth/
    │   └── mappers/
    └── tests/
```

**Dependency Chain**: `ui-kit → domain-types → client-core`

**Constraints**:
- No runtime logic in ui-kit
- No networking in ui-kit
- No runtime logic in domain-types
- No runtime logic in client-core

---

### services/

Purpose: Backend microservices

```
services/
├── auth-service/              # Port 3000
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs
│   │   ├── models/
│   │   ├── services/
│   │   ├── api/
│   │   └── db/
│   ├── migrations/
│   │   └── 0001_init.up.sql
│   └── tests/
├── driver-service/            # Port 3001
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs
│   │   ├── models/
│   │   ├── services/
│   │   ├── api/
│   │   ├── db/
│   │   └── telemetry/
│   ├── migrations/
│   │   ├── 0001_init_gis.up.sql
│   │   ├── 0002_init_analytics.up.sql
│   │   └── 0003_create_analytics_indexes.up.sql
│   └── tests/
└── admin-service/             # Port 3002
    ├── Cargo.toml
    ├── src/
    │   ├── main.rs
    │   ├── models/
    │   ├── services/
    │   ├── api/
    │   └── db/
    ├── migrations/
    │   └── 0001_init_inventory.up.sql
    └── tests/
```

**Dependency Chain**: `services → shared-domain → shared-infra`

**Constraints**:
- Each service has isolated migrations
- No service→service imports
- Shared crates must be in shared/ directory

---

### tools/

Purpose: CI enforcement and validation scripts

```
tools/
├── ci_guard.sh                # 9-stage CI enforcement
├── 01_validate_identity.sh    # Validate UUID vs nanoid usage
├── 02_validate_deps.sh        # Validate dependency graph
├── 03_validate_analytics_gate.sh  # Validate analytics write permissions
├── 04_validate_schema.sh      # Validate database schema integrity
├── 05_sqlx_policy_check.sh    # Validate SQLx compile-time policy
└── 06_ci_guard_final.sh       # Final CI gate runner
```

---

### infrastructure/

Purpose: DevOps configuration and deployment scripts

```
infrastructure/
├── docker-compose/
│   └── local.yml              # Local development environment
├── traefik/
│   └── traefik.toml           # Reverse proxy configuration
├── scripts/
│   ├── provision_db.sh        # Database initialization script
│   ├── deploy.sh              # Service deployment script
│   └── migrate.sh             # Schema migration runner
└── README.md                  # Infrastructure documentation
```

---

### docs/

Purpose: Project documentation

```
docs/
├── constitution/
│   └── speckit_enforcement.md # SpecKit enforcement layer
├── sprints/                   # Sprint artifacts
│   ├── sprint_00/
│   │   ├── backlog/
│   │   ├── review/
│   │   ├── system_state.md
│   │   ├── roadmap_status.md
│   │   ├── sprint_state.json
│   │   └── validation_report.md
│   └── ...
└── spec/                      # Feature specifications
```

---

### .specify/

Purpose: SpecKit configuration and enforcement

```
.specify/
├── memory/
│   └── constitution.md        # Project constitution (linked to docs/constitution/)
├── extensions/                # SpecKit extensions
│   ├── git/
│   │   ├── git-config.yml
│   │   └── scripts/
│   │       ├── bash/
│   │       └── powershell/
│   ├── speckit/
│   │   ├── extensions.yml
│   │   └── templates/
│   └── enforcement/
│       └── enforcement.md
└── templates/                 # SpecKit templates
    ├── plan-template.md
    ├── spec-template.md
    └── tasks-template.md
```

## Identity System Summary

### Users (auth-service)

- **Format**: UUID (Keycloak sub)
- **Table**: `platform_db.users.id`
- **Validation**: SHA-256 of email and timestamp

### Entities (driver-service, admin-service)

| Entity Type | Prefix | Length | Table |
|-------------|--------|--------|-------|
| Charging Station | STA | 12 chars | `inventory.stations.station_id` |
| Charger | CHR | 12 chars | `inventory.chargers.charger_id` |
| Connector | CON | 12 chars | `inventory.connectors.connector_id` |
| Partner | PRT | 12 chars | `inventory.partners.partner_id` |
| Event | EVT | 12 chars | `analytics_db.system_events.id` |

### Analytics

- **Telemetry Events**: UUID
- **Analytics Events**: UUID
- **System Events**: UUID

## Validation Rules

1. **UUID Usage**:
   - Users MUST use UUID only (Keycloak sub)
   - No UUID in entity identifiers (STA/CHR/CON/PRT/EVT)
   - No UUID in analytics events (use UUID but check for proper format)

2. **nanoid Usage**:
   - Entities MUST use nanoid(12) with PREFIX
   - No entity using plain UUID or other formats
   - No nanoid in user identifiers

3. **Data Ownership**:
   - platform_db.users → auth-service (READ/WRITE)
   - platform_db.gis → driver-service (READ/WRITE)
   - platform_db.inventory → admin-service (READ/WRITE)
   - analytics_db → driver-service (WRITE), admin-service (READ ONLY)

4. **Cross-Service Writes**:
   - driver-service CAN write to analytics_db
   - admin-service CANNOT write to analytics_db
   - auth-service CANNOT write to analytics_db

5. **Spatial Indexes**:
   - `idx_inventory_stations_location` (USING GIST) - mandatory for spatial queries
   - `idx_inventory_chargers_station` - for charger lookup by station
   - `idx_inventory_connectors_charger` - for connector lookup by charger
   - `idx_inventory_connectors_status` - for status-based filtering
   - `idx_inventory_chargers_status` - for status-based filtering

6. **Normalization**:
   - Connector types and statuses use lookup tables (connector_types, current_types, charger_statuses, connector_statuses)
   - Status IDs reference lookup tables rather than storing raw values
   - Partner information managed separately from station data

5. **Schema Migration**:
   - Each service has isolated migration files
   - Migrations are forward-only (no rollback)
   - Migrations use SQLx for compile-time verification
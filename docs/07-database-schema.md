# BorneMap — Database Schema & Seed Blueprint

## 1. Core Relational Schema

Migration file: `sources/backend/migrations/20260525000000_init.up.sql`

### Extensions

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Enums

| Enum | Values |
|------|--------|
| `user_role` | `admin`, `partner`, `driver` |
| `partner_type` | `business`, `private` |
| `current_type` | `AC`, `DC` |
| `charger_status` | `available`, `occupied`, `faulted`, `offline` |

### Tables

#### `users`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `VARCHAR(64)` | PRIMARY KEY |
| `email` | `VARCHAR(255)` | UNIQUE NOT NULL |
| `password_hash` | `VARCHAR(255)` | NOT NULL |
| `username` | `VARCHAR(100)` | UNIQUE NOT NULL |
| `role` | `user_role` | NOT NULL DEFAULT 'driver' |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `deleted_at` | `TIMESTAMPTZ` | DEFAULT NULL |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT FALSE |

#### `partner_profiles`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `VARCHAR(64)` | PRIMARY KEY |
| `user_id` | `VARCHAR(64)` | NOT NULL → `users(id) ON DELETE CASCADE` |
| `classification` | `partner_type` | NOT NULL DEFAULT 'business' |
| `display_name` | `VARCHAR(255)` | NOT NULL |
| `tax_id` | `VARCHAR(100)` | DEFAULT NULL |
| `logo_url` | `TEXT` | DEFAULT NULL |
| `contact_phone` | `VARCHAR(50)` | DEFAULT NULL |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `deleted_at` | `TIMESTAMPTZ` | DEFAULT NULL (soft delete) |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT FALSE |

#### `station_connector_types`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `VARCHAR(64)` | PRIMARY KEY |
| `name` | `VARCHAR(100)` | UNIQUE NOT NULL |
| `description` | `TEXT` | NOT NULL |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `deleted_at` | `TIMESTAMPTZ` | DEFAULT NULL |

#### `stations`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `VARCHAR(64)` | PRIMARY KEY |
| `owner_id` | `VARCHAR(64)` | NOT NULL → `users(id) ON DELETE RESTRICT` |
| `name` | `VARCHAR(255)` | NOT NULL |
| `address` | `TEXT` | NOT NULL |
| `city` | `VARCHAR(100)` | NOT NULL |
| `coordinates` | `GEOGRAPHY(Point, 4326)` | NOT NULL |
| `is_operational` | `BOOLEAN` | NOT NULL DEFAULT TRUE |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `deleted_at` | `TIMESTAMPTZ` | DEFAULT NULL |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT FALSE |

#### `chargers`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `VARCHAR(64)` | PRIMARY KEY |
| `station_id` | `VARCHAR(64)` | NOT NULL → `stations(id) ON DELETE CASCADE` |
| `connector_type_id` | `VARCHAR(64)` | NOT NULL → `station_connector_types(id) ON DELETE RESTRICT` |
| `power_kw` | `NUMERIC(5, 2)` | NOT NULL |
| `current_type` | `current_type` | NOT NULL DEFAULT 'AC' |
| `status` | `charger_status` | NOT NULL DEFAULT 'available' |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() |

### Indexes

| Index Name | Table | Definition | Type |
|------------|-------|------------|------|
| `idx_stations_spatial_coordinates` | `stations` | `coordinates` | GIST |
| `idx_partner_profiles_user_id` | `partner_profiles` | `user_id` | B-tree |
| `idx_partner_profiles_active` | `partner_profiles` | `deleted_at WHERE deleted_at IS NULL` | Partial B-tree |
| `idx_stations_owner_isolation` | `stations` | `owner_id` | B-tree |
| `idx_chargers_station_id` | `chargers` | `station_id` | B-tree |
| `idx_stations_active_lookup` | `stations` | `deleted_at WHERE deleted_at IS NULL` | Partial B-tree |

### Cascade/Restrict Rules

| Relationship | On Delete | Rationale |
|-------------|-----------|-----------|
| `partner_profiles.user_id` → `users.id` | CASCADE | Partner profile is meaningless without user |
| `stations.owner_id` → `users.id` | RESTRICT | Prevent orphaning stations; must reassign first |
| `chargers.station_id` → `stations.id` | CASCADE | Chargers belong to station; deleted with it |
| `chargers.connector_type_id` → `station_connector_types.id` | RESTRICT | Type in use; must remove chargers first |

## 2. Sandbox Validation Seed Data

Migration file: `sources/backend/migrations/20260525000001_seed_sandbox.up.sql`

### Connector Types (2 entries)

| ID | Name | Description |
|----|------|-------------|
| `CNT-type2acbase` | Type 2 (AC) | Standard AC Charging Socket matching European and local Tunisian baselines |
| `CNT-ccs2dcsuper` | CCS 2 (DC Fast) | High power DC rapid fast charge connector system |

### Test Partner Users (5 entries)

| ID | Email | Username | Role |
|----|-------|----------|------|
| `USR-m1k9p2v4x7q3` | tunis_charge@bornemap.test | tunis_charge | partner |
| `USR-f8n2w7z5k4m1` | sousse_ev@bornemap.test | sousse_ev | partner |
| `USR-t3b9v6x1p8r4` | sfax_power@bornemap.test | sfax_power | partner |
| `USR-j7k4m2n9p1q5` | bizerte_grid@bornemap.test | bizerte_grid | partner |
| `USR-c2v8x4p7n1m3` | ahmed_private@bornemap.test | ahmed_private | partner |

### Test Partner Profiles (5 entries)

| ID | User ID | Classification | Display Name | Tax ID |
|----|---------|---------------|--------------|--------|
| `PRT-z5x3n1v9p4q7` | `USR-m1k9p2v4x7q3` | business | Tunis Charge Operator | MF-8473920-A |
| `PRT-k2m8p4n7v1x3` | `USR-f8n2w7z5k4m1` | business | Sousse EV Network | MF-1294857-B |
| `PRT-q9p3v7m1n4k2` | `USR-t3b9v6x1p8r4` | business | Sfax Power Solutions | MF-9384756-C |
| `PRT-w5n2v8x4p1q7` | `USR-j7k4m2n9p1q5` | business | Bizerte Eco Grid | MF-5738291-D |
| `PRT-r3m7p1v9k4x2` | `USR-c2v8x4p7n1m3` | private | Ahmed Ben Ali (Private) | NULL |

### Spatial Distribution (100 stations, 300 chargers)

- 100 geographically distributed test stations across Tunis, Sousse, Sfax, Bizerte
- Each station has 3 chargers (2x Type 2 AC 22kW, 1x CCS 2 DC 50kW)
- All records marked `is_test = true`
- Coordinates clustered around urban centers with incremental offset distribution

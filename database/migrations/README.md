# Migrations

This directory contains SQL migration files for the BorneMap database.

Migration files use sequential numeric prefix (0001, 0002, ...) for ordering.

All tables live under the `"ev-platform"` schema.

## File Naming Convention

`NNNN_description.sql` where NNNN is a zero-padded sequence number.

## Migration List

- `0001_create_ev_platform_schema.sql` — Creates the `"ev-platform"` schema
- `0002_create_partner_table.sql` — Creates the partner table with CHECK constraints
- `0003_create_station_table.sql` — Creates the station table with spatial column and GIST index
- `0004_create_charger_and_availability_tables.sql` — Creates charger and station_availability tables

## Applying Migrations

```bash
psql -d borne_map -f database/migrations/0001_create_ev_platform_schema.sql
psql -d borne_map -f database/migrations/0002_create_partner_table.sql
psql -d borne_map -f database/migrations/0003_create_station_table.sql
psql -d borne_map -f database/migrations/0004_create_charger_and_availability_tables.sql
```

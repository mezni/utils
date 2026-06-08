# Data Model: Sprint 1 Backend and Database

## Partner

- `id` UUID, primary key, public identifier
- `name` text, required, unique
- `created_at` timestamp, required
- `updated_at` timestamp, required

Relationships:

- One partner has many stations.

## Station

- `id` UUID, primary key, public identifier
- `partner_id` UUID, required, foreign key to `inventory.partner.id`
- `name` text, required
- `latitude` numeric, required
- `longitude` numeric, required
- `address` text, optional
- `city` text, optional
- `governorate` text, optional
- `created_at` timestamp, required
- `updated_at` timestamp, required

Relationships:

- One station belongs to one partner.
- One station has many chargers.
- Deleting a partner removes its stations.
- Deleting a station removes its chargers.

Validation:

- Latitude and longitude must be valid decimal coordinates.
- Nearby lookup uses station coordinates and a radius in kilometers.

## Charger

- `id` UUID, primary key, public identifier
- `station_id` UUID, required, foreign key to `inventory.station.id`
- `label` text, optional
- `connector_type` text, required
- `power_kw` numeric, required
- `status` text, optional
- `created_at` timestamp, required
- `updated_at` timestamp, required

Relationships:

- One charger belongs to one station.

Validation:

- Each station seed must contain 2-4 chargers.
- Charger records must not exist without a station.

## Seed Set

- 3 partners
- 15 stations across Tunisia with real coordinates
- 2-4 chargers per station

## API Behavior Notes

- Public resource identifiers are UUIDs, not sequential integers.
- Nearby results should be returned in ascending distance order.

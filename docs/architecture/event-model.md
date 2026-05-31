# Event Model

## GIS Sync Events

Sourced from PostgreSQL outbox for GIS projection updates.

| Event | Description |
|-------|-------------|
| `station.created` | New station added |
| `station.updated` | Station details changed |
| `station.archived` | Station removed from active map |

**Flow**: Admin Service writes station → outbox event in same transaction →
commit succeeds → GIS Worker processes → GIS schema updated

GIS sync MUST be idempotent per `station_id` + `sync_version`.

## Clickstream Events

Published to RabbitMQ and persisted to the `analytics` schema.

| Event | Description |
|-------|-------------|
| `page.viewed` | User viewed a page |
| `station.opened` | User opened station details |
| `search.performed` | User performed a search |
| `filter.applied` | User applied a map filter |
| `favorite_station.added` | User favorited a station |
| `review.submitted` | User submitted a review |

## Business Audit Events

Used only for audit and reporting consistency. Optional.

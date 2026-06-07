# Domain Model

**Phase**: 1 — Foundation
**Related Tasks**: TASK-15 through TASK-24 (database), TASK-25 through TASK-42 (services)
**Last Updated**: 2026-06-07

---

## Entities (Phase 1 Scope)

### Partner
| Field | Type | Notes |
|---|---|---|
| id | TEXT (PRT-...) | NanoID, prefixed |
| name | TEXT | Display name |
| created_at | TIMESTAMPTZ | Auto-generated |

**Relationships**:
- A Partner has many Stations (1:N)
- A Partner has one User membership (1:1, Phase 2)

### Station
| Field | Type | Notes |
|---|---|---|
| id | TEXT (STN-...) | NanoID, prefixed |
| partner_id | TEXT (PRT-...) | Foreign key to Partner |
| name | TEXT | Display name |
| address | TEXT | Optional street address |
| latitude | NUMERIC(10,7) | WGS84 |
| longitude | NUMERIC(10,7) | WGS84 |
| created_at | TIMESTAMPTZ | Auto-generated |
| updated_at | TIMESTAMPTZ | Auto-updated on change |

**Relationships**:
- A Station belongs to one Partner (N:1)
- A Station has many Chargers (1:N)
- A Station has one StationAvailability (1:1)
- A Station has one StationLocation in gis schema (1:1, derived)

### Charger
| Field | Type | Notes |
|---|---|---|
| id | TEXT (CHG-...) | NanoID, prefixed |
| station_id | TEXT (STN-...) | Foreign key to Station |
| connector_type | TEXT | Enum: type2, ccs, chademo, type1 |
| power_kw | NUMERIC(6,2) | Charging power in kW |
| status | TEXT | Enum: available, in_use, maintenance, offline |
| updated_at | TIMESTAMPTZ | Auto-updated on change |

**Relationships**:
- A Charger belongs to one Station (N:1)

### StationAvailability
| Field | Type | Notes |
|---|---|---|
| id | TEXT | NanoID |
| station_id | TEXT (STN-...) | Foreign key to Station |
| status | TEXT | available, unavailable, partial |
| updated_by | TEXT | User/partner who updated |
| updated_at | TIMESTAMPTZ | Auto-generated |

### StationLocation (gis schema — derived)
| Field | Type | Notes |
|---|---|---|
| station_id | TEXT | PK, references inventory.station |
| geom | GEOMETRY(Point, 4326) | PostGIS point geometry |
| snapped_road_id | BIGINT | FK to gis.roads (nearest road) |
| region_id | BIGINT | FK to gis.boundaries (admin boundary) |
| updated_at | TIMESTAMPTZ | Auto-updated |

---

## Entity Relationship Diagram (Phase 1)

```
┌──────────┐       ┌──────────┐       ┌──────────┐
│ Partner  │       │ Station  │       │ Charger  │
│          │ 1   N │          │ 1   N │          │
│ id       │──────▶│ id       │──────▶│ id       │
│ name     │       │ partner_ │       │ station_ │
│ created  │       │   id     │       │   id     │
└──────────┘       │ name     │       │ connector│
                   │ address  │       │ power_kw │
                   │ lat/lng  │       │ status   │
                   │ created  │       │ updated  │
                   │ updated  │       └──────────┘
                   └────┬─────┘
                        │ 1
                        │
                   ┌────▼─────┐
                   │ Station  │
                   │ Availab- │
                   │ ility    │
                   │ 1        │
                   │ id       │
                   │ station_ │
                   │   id     │
                   │ status   │
                   └──────────┘
```

---

## Domain Rules (Phase 1)

1. A station belongs to exactly one partner
2. A charger belongs to exactly one station
3. A partner can have zero or more stations
4. A station can have zero or more chargers
5. Deleting a partner is only allowed if it has no stations
6. Deleting a station is only allowed if it has no chargers
7. Charger status values are restricted to the canonical set
8. Station coordinates are required (NOT NULL)

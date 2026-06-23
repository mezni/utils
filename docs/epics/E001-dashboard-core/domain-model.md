# E001 DOMAIN MODEL

---

## Entities

- Partners
- Stations
- Chargers

---

## Relationships

Partners → Stations → Chargers

---

## Rules

- Station must belong to partner
- Charger must belong to station
- Deletion cascades downward (HARD delete only)
- Soft deletes do NOT cascade
- Status enum consistent across all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
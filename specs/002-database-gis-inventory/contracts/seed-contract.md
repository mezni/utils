# Seed Data Contract

## Contract

- Files: `db/seeds/dev_partners.sql`, `dev_stations.sql`, `dev_chargers.sql`
- Format: Raw SQL (INSERT statements)
- Dependencies: Partners must be inserted before stations; stations before chargers
- Data: Synthetic Tunisian data only — no real customer data
- Counts: 3 partners, 15 stations, 24 chargers

## Partner Distribution

| Partner | # Stations |
|---------|-----------|
| Partner A (Tunis) | 6 |
| Partner B (Sfax) | 5 |
| Partner C (Sousse) | 4 |

## Station Distribution

| Region | # Stations |
|--------|-----------|
| Tunis (capital) | 5 |
| Sfax | 3 |
| Sousse | 2 |
| Nabeul | 1 |
| Bizerte | 1 |
| Gabès | 1 |
| Kairouan | 1 |
| Monastir | 1 |

## Charger Distribution

- 24 chargers across 15 stations (1-2 per station)
- Connector types: mix of Type2, CCS, Chademo, Type2Combo
- Statuses: mostly Available, some Charging/Offline/Maintenance

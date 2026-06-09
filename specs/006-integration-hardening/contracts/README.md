# Sprint 1.6 Contracts

## Overview

No new interface contracts. This sprint verifies existing contracts across all 4 apps.

## Existing Contracts to Verify

### API Contract (json-server)

| Endpoint | Method | Purpose | Verified In |
|----------|--------|---------|-------------|
| `/api/partners` | GET, POST | List/create partners | Full loop US1 |
| `/api/partners/:id` | GET, PUT, PATCH, DELETE | Read/update/delete partner | Full loop US1 |
| `/api/stations` | GET, POST | List/create stations | Full loop US1 |
| `/api/stations/:id` | GET, PUT, PATCH, DELETE | Read/update/delete station | Full loop US1 |
| `/api/chargers` | GET, POST | List/create chargers | Full loop US1 |
| `/api/chargers/:id` | GET, PUT, PATCH, DELETE | Read/update/delete charger | Full loop US1 |
| `/api/station_availability` | GET, POST | List/create availability records | Full loop US1 |
| `/api?partner_id=` | Filter | Filter stations by partner | Partner scoping US2 |
| `/api/chargers?station_id=` | Filter | Filter chargers by station | Partner scoping US2 |

### UI Contracts

| App | Screens | ErrorState | Form Validation |
|-----|---------|------------|-----------------|
| Dashboard Admin | Overview, Partners, Stations, Chargers | All screens | All forms |
| Dashboard Partner | Overview, My Stations, My Chargers, Availability | All screens | All forms |
| Driver Web | Map, Station Detail | Both screens | N/A |
| Driver Mobile | Map, Station Detail | Both screens | N/A |

### Partner Visibility Contract

- Stations appear on driver apps only when `partner.is_verified && partner.is_live && partner.is_active`
- Verified in full loop: set is_live → appears, deactivate → disappears

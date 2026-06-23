# SYSTEM OVERVIEW

The platform is a modular monolithic EV dashboard system composed of:

- admin-service (Rust backend)
- admin-dashboard (React frontend)
- platform_db (PostgreSQL)
- platform-core (shared utilities)
- platform-db (DB abstraction layer)

---

## Core Domains

- Partners
- Stations
- Chargers

---

## Architecture Style

Clean Architecture with strict layering:
presentation → application → domain → infrastructure

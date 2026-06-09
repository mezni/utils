# Glossary — BorneMap

**Version:** 1.0
**Last updated:** 2026-06-09

---

| Term | Definition |
|---|---|
| **Admin** | User with Keycloak role `admin`. Manages the entire platform globally. |
| **Admin Service** | Rust Actix-web service (port 8081) handling partner, station, charger CRUD and reporting. |
| **ADR** | Architecture Decision Record. Stored in `docs/adr/`. Required for non-trivial architecture changes. |
| **Availability** | Manual station-level status (available, partial, unavailable). Stored append-only in `inventory.station_availability`. |
| **Charger** | An individual EV charging point belonging to a station. Has connector type, power, and status. |
| **Clickstream Service** | Rust Actix-web service (port 8082, MVP-5+) for analytics event ingestion. |
| **Dashboard App** | React + Vite application serving both Admin and Partner roles. Built before driver apps in every MVP. |
| **Driver Mobile App** | React Native + Expo SDK 54 application. Primary driver surface. |
| **Driver Service** | Rust Actix-web service (port 8080) handling public station discovery and authenticated driver features. |
| **Driver Web App** | React + Vite web application. Map-centric with Leaflet + OpenStreetMap. |
| **GIS** | Geographic Information System. PostGIS-powered schema (`gis`) providing spatial data. |
| **GIS Sync** | PostgreSQL trigger that syncs station coordinates from `inventory.station` to `gis.station_locations`. |
| **is_active** | Partner flag. Account operationally enabled. Default `true`. |
| **is_live** | Partner flag. Partner has visible stations. Cannot be `true` without `is_verified`. Default `false`. |
| **is_verified** | Partner flag. Admin-approved identity. Default `false`. |
| **json-server** | Mock REST API used in MVP-1. Serves from `source/mock/db.json` under `/api` prefix. |
| **Keycloak** | Authentication server (MVP-3+). Owns all authentication and JWT issuance. |
| **MVP** | Minimum Viable Product. Each MVP is complete and deployable on its own. |
| **NanoID** | URL-safe unique identifier with entity prefix (e.g., `PRT-...`, `STN-...`). Replaces sequential integers. |
| **OCPP** | Open Charge Point Protocol. Permanently deferred. |
| **Partner** | An organization or individual who owns and operates EV charging stations. Has type `business` or `personal`. |
| **PostGIS** | Spatial extension for PostgreSQL. Provides geometry types and spatial indexing. |
| **Public Driver** | Anonymous user. No login required. Can browse stations on map. |
| **Registered Driver** | Authenticated user with Keycloak role `registered_driver`. Can favorite, review, manage profile. |
| **Station** | A physical EV charging location. Belongs to a partner. Contains one or more chargers. |
| **ST_DWithin** | PostGIS spatial function for distance-based queries. Used by Driver Service nearby endpoint. |
| **Traefik** | Edge router (MVP-6+). Only service exposing public ports. Handles TLS termination. |
| **Trigger** | PostgreSQL function that auto-syncs `inventory.station` coordinates to `gis.station_locations`. |

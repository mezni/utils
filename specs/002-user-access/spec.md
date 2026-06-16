# User Access — MVP-2

## Overview

Enable authenticated access to the BorneMap platform with proper authorization for different user roles (drivers, partners, admins), integrate with Keycloak for identity management, and expose the GIS service API endpoints for stations and charger data. This establishes the foundation for secure data access and role-based features.

## User Stories

- As a **driver**, I want to authenticate with Keycloak using email/password so that I can access the mobile app
- As a **driver**, I want to see my nearby stations and chargers on the map after login so that I can plan my route
- As a **partner**, I want to log in and see my own stations in the dashboard so that I can manage my inventory
- As an **admin**, I want to access the admin panel and manage all stations and partners so that I can control the platform
- As a **driver**, I want the mobile app to fetch real-time station availability from the GIS service so that I know which chargers are available

## Functional Requirements

### Authentication & Authorization
- FR1: Auth Service provides POST `/api/v1/auth/login` accepting email and password, returning a JWT token
- FR2: Auth Service validates credentials against Keycloak via admin API and returns a valid JWT for authenticated users
- FR3: Auth Service provides POST `/api/v1/auth/register` for drivers to create new accounts
- FR4: Auth Service includes `Authorization: Bearer <token>` header validation middleware on all routes
- FR5: Auth Service validates JWT token expiration and refresh tokens for session management
- FR6: Each user has a role field (driver, partner, admin) stored in Keycloak

### API Integration — GIS Service
- FR7: Driver Service provides GET `/api/v1/geo/stations` accepting optional `radius_km` parameter (default: 5km), returning a paginated list of nearby stations based on user location
- FR8: Driver Service provides GET `/api/v1/geo/chargers` accepting optional `radius_km` parameter (default: 5km), returning a paginated list of nearby chargers based on user location
- FR9: GIS Service exposes POST `/api/v1/geo/stations` for partners to create new stations
- FR10: GIS Service exposes PUT `/api/v1/geo/stations/:id` for partners to update station details
- FR11: GIS Service exposes DELETE `/api/v1/geo/stations/:id` for admins to remove stations
- FR12: GIS Service exposes GET `/api/v1/geo/stations/:id` returning full station details
- FR13: GIS Service exposes POST `/api/v1/geo/chargers` for partners to create chargers within stations
- FR14: GIS Service exposes PUT `/api/v1/geo/chargers/:id` for partners to update charger details
- FR15: GIS Service exposes DELETE `/api/v1/geo/chargers/:id` for admins to remove chargers
- FR16: GIS Service exposes GET `/api/v1/geo/chargers/:id` returning full charger details

### Data Models
- FR17: Station data includes: id, name, address, location (lat/lon), partner_id, charger_ids[], availability status
- FR18: Charger data includes: id, station_id, type (type 1, type 2, CCS, CHAdeMO), connector_count, status (available, occupied, unavailable), power_kw

### Mobile App Integration
- FR19: Mobile driver app displays user profile info after successful login
- FR20: Mobile driver app fetches nearby stations and renders markers on the map
- FR21: Mobile driver app displays charger availability (available/occupied/unavailable) at each station

### Dashboard Integration
- FR22: Dashboard login page redirects to Keycloak OAuth2 flow
- FR23: Dashboard displays partner's own stations in a table after login (partners cannot view other partners' stations)
- FR24: Dashboard admin panel allows editing partner stations

## Non-functional Requirements

- NFR1: JWT tokens expire within 15 minutes (refresh token extends session)
- NFR2: Auth Service validates Keycloak token introspection on every request
- NFR3: All GIS Service endpoints support pagination with max 100 items per page
- NFR4: Station and charger data is cached for 30 seconds to reduce database load
- NFR5: API responses include CORS headers for web and mobile apps
- NFR6: Login credentials are hashed and stored securely in Keycloak (not in our DB)
- NFR7: All API endpoints are versioned (`/api/v1/...`) to allow future API changes

## Out of Scope

- OAuth2 client credentials flow (reserved for service-to-service auth)
- Multi-factor authentication (MFA)
- Password reset via email (MVP-3)
- User profile editing (MVP-4)
- Admin panel for managing Keycloak users (MVP-4)
- Partner station sharing between partners
- Partners viewing other partners' stations (excluded from MVP-2 scope)
- Advanced map features (routing, geofencing, POI clustering)
- Real-time WebSocket updates for station status
- GDPR data export or deletion requests
- API rate limiting (will add in MVP-6)

## Success Criteria

| Criterion | Measure |
|-----------|---------|
| POST `/api/v1/auth/login` returns 200 with JWT token for valid credentials | Verified via curl or API test |
| POST `/api/v1/auth/login` returns 401 for invalid credentials | Verified via API test |
| Auth Service successfully authenticates against Keycloak | Verified by checking Keycloak logs |
| Mobile app shows "Logged in as <email>" after login | Visual inspection |
| Mobile app renders station markers within 5 seconds of login | Visual inspection + timing measurement |
| GIS Service `GET /api/v1/geo/stations` returns paginated results | Verified via curl with `?page=1&limit=50` |
| GIS Service `GET /api/v1/geo/chargers` returns charger details | Verified via curl |
| Partner can create a station via POST `/api/v1/geo/stations` and see it in dashboard | Visual inspection |
| Admin can delete a station via DELETE `/api/v1/geo/stations/:id` | Verified via curl |

## Dependencies

- Keycloak running on port 8080 (MVP-3 configures realm and users)
- PostGIS database with `gis`, `inventory`, `users` schemas populated (MVP-1)
- Redis for caching station/charger data (MVP-2)
- `@opencode/skill/expo-maps` for map markers on mobile app (internal skill)

## Assumptions

- Users will be created in Keycloak realm by admins or through self-registration
- Default Keycloak realm is `bornemap` with users in `drivers`, `partners`, `admin` groups
- Drivers use email/password login, partners use Keycloak admin API for station management
- Mobile app uses embedded JWT (user authenticates once, token stored locally)
- Map markers appear only within a 5km radius of the user's location
- Partner ID is derived from Keycloak user email domain (for MVP-2)
- All API responses use JSON format with UTF-8 encoding

## Clarifications

### Session 2026-06-16

- Q: What is the default Keycloak realm name for MVP-2? → A: bornemap
- Q: Should partners be able to see all stations or only their own? → A: Only their own stations (restrict MVP-2 for simplicity)
- Q: What is the minimum location radius for "nearby" station queries? → A: 5km radius (default for MVP-2)

## Questions / Clarifications

(No remaining clarifications needed for MVP-2 scope)

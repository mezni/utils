# MVP-1 Status

Status: not-started
Current focus: prove the end-to-end loop with real data.

## In Scope
- FastAPI backend for stations, partners, and chargers
- Driver web map and station detail screens
- Driver mobile map and station detail screens
- Dashboard CRUD for partners, stations, and chargers
- Dockerfiles and local Docker Compose for development and onboarding

## Out of Scope
- Authentication, Keycloak, JWT, user accounts
- Favorites, reviews, GIS sync, PostGIS, OSM import
- Analytics, reporting, Traefik, TLS, CI/CD
- Production launch hardening

## Done Criteria
- All MVP-1 endpoints return correct data against a real database
- Dashboard supports full CRUD for partners, stations, and chargers
- Driver web and mobile show real markers and station details
- Create in dashboard, then see the data in driver apps
- All three apps fail gracefully when the API is unreachable
- Onboarding guide works from scratch
- API documentation is complete
- Zero Class A bugs

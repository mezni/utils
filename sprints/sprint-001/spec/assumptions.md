# Sprint 001 — Assumptions

- Drivers access the platform via a modern web browser with geolocation capabilities.
- Partners have reliable internet connectivity and basic technical literacy.
- External geospatial data sources follow consistent tagging conventions for charging stations.
- Platform initially deployed for Tunisia with plans for regional expansion.
- Partners are responsible for keeping their station data up to date.
- External data imports are initiated manually by system operators (not automated).
- Station data is read far more frequently than written — read performance prioritized.
- No driver authentication required for browsing and searching stations.
- All identifiers use typed prefix + nanoid(12) format as per constitution.
- PostGIS is the single source of truth for all location data.
- Three-service topology (auth, driver, admin) is frozen for the validation phase.

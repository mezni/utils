# Research: BorneMap Platform Scaffold

- **Decision**: Actix-web 4.4 for Rust HTTP server
  - **Rationale**: Matches constitution Technical Stack Governance (Rust via Actix-web). Battle-tested async web framework with built-in actor model, middleware, and test utilities.
  - **Alternatives considered**: Axum (Tokio-native but less mature ecosystem), Rocket (constitution-locked to Actix-web).

- **Decision**: PostGIS 15-3.3 for spatial database
  - **Rationale**: Constitution mandates PostgreSQL + PostGIS for spatial computations. Performs geospatial queries natively with SRID 4326 compliance.
  - **Alternatives considered**: MongoDB with GeoJSON (would break constitution stack lock).

- **Decision**: React Native via Expo Go for mobile client
  - **Rationale**: Constitution locks mobile to React Native / Expo Go. Enables cross-platform iOS/Android without native build tooling during development.
  - **Alternatives considered**: Bare React Native CLI (more complex setup, violates Expo Go requirement).

- **Decision**: In-memory mock data using parking_lot RwLock
  - **Rationale**: Simplest approach for MVP scaffold — no database dependency required for mock API. Thread-safe concurrent read access via RwLock matches Actix-web's async worker model.
  - **Alternatives considered**: Full PostGIS integration (adds setup complexity for MVP), static JSON files (not thread-friendly for future mutation).

- **Decision**: nanouuid identifiers (`stn-`, `chg-`, `prv-` format)
  - **Rationale**: Constitution mandates `XXX-nanouuid` pattern. Clear entity type prefix enables at-a-glance identification in API responses and logs.

- **Decision**: GitHub Actions for CI/CD
  - **Rationale**: Constitution requires early CI/CD integration. Native GitHub ecosystem integration with matrix testing for Rust + Node.js in a single pipeline.

- **Decision**: Docker Compose for environment parity
  - **Rationale**: Constitution mandates Docker Compose for local/staging parity. Single `docker-compose up` starts PostGIS with health checks.

- **Decision**: axios for mobile API client
  - **Rationale**: Industry standard HTTP client for React Native. Supports interceptors, request/response transforms, and works with Expo without native modules.
  - **Alternatives considered**: fetch API (less feature-rich), React Query (overkill for MVP mock data).

# Research: Keycloak Authentication Setup

**Date**: 2026-06-10 | **Branch**: `013-keycloak-setup` | **Spec**: [spec.md](./spec.md)

## Architecture Decisions

### Keycloak Database Schema

- **Decision**: Use a dedicated `keycloak` schema in the shared PostgreSQL database. Create it via a standard migration (`0005_keycloak_schema.sql`) before Keycloak starts.
- **Rationale**: Avoids table name collisions with application data (which uses the `public` and `inventory` schemas). Migration runs as part of the existing migration pipeline before Keycloak startup.
- **Alternatives considered**: Separate PostgreSQL instance — rejected as unnecessary overhead for dev. Separate database in same instance — works but schema scoping is cleaner.

### Realm Initialization Flow

- **Decision**: First-run realm configuration is done manually via the Keycloak admin console. The realm is then exported to `infra/keycloak/realm-export.json`. Subsequent starts use `--import-realm` to auto-import.
- **Rationale**: The realm has complex configuration (5 clients, 2 IdPs, 3 roles, protocol mappers) that is tedious to script. Manual admin console setup for first run is faster and less error-prone. Once exported, repeatability is guaranteed.
- **Alternatives considered**: Declarative realm JSON (hand-written) — error-prone and hard to maintain. Keycloak Terraform provider — adds infrastructure as code dependency not yet needed.

### PostgreSQL Connection

- **Decision**: Keycloak connects to PostgreSQL at `jdbc:postgresql://postgres:5432/borne_map` with schema `keycloak` using the same `postgres` user/password.
- **Rationale**: Existing `postgres` container and credentials. The `keycloak` schema keeps tables isolated. No additional database user management needed for dev.
- **Note**: In production, a dedicated database user with restricted permissions should be used.

### Identity Provider (Social Login) Configuration

- **Decision**: Google and Facebook IdPs are configured via the admin console during first-run setup. Dev credentials are stored in `infra/env/keycloak.env.example` as placeholders.
- **Rationale**: IdP configuration requires live credentials from Google Cloud Console and Meta Developer Portal. These are organizational secrets managed outside this sprint.
- **Post-login flow**: First broker login flow assigns `registered_driver` role to new users automatically (configurable in admin console).

### JWT Token Structure

- **Decision**: Standard Keycloak JWT with OIDC claims. Custom `partner_id` claim added via a Protocol Mapper (User Attribute → Token Claim). Access token: 15 min, Refresh token: 7 days, SSO session: 7 days.
- **Rationale**: Default Keycloak JWT format matches OIDC specification. The protocol mapper approach avoids custom SPI development.
- **Validation**: Backend services validate tokens by fetching JWKS from `http://keycloak:8180/realms/ev-platform/protocol/openid-connect/certs`.

### Client Configuration Strategy

- **Decision**: Public clients (driver-web, driver-mobile, dashboard) use PKCE with S256. Confidential clients (driver-service, admin-service) use client secret + service account.
- **Rationale**: PKCE is mandatory for public clients per OAuth2 security best practices (mitigates authorization code interception). Service accounts are the standard pattern for backend-to-backend auth.
- **Redirect URIs**: Each client has its specific redirect URIs as defined in the spec, matching the app's expected callback URLs.

### Realm Export Procedure

- **Decision**: Export via `docker exec` using Keycloak's built-in export command. Verify clean import by tearing down with `docker compose down -v` and bringing back up.
- **Rationale**: Keycloak's export command produces a complete realm representation. The `--users realm_file` flag includes user data in the export. Clean re-import verification ensures the export is self-contained.
- **File location**: `infra/keycloak/realm-export.json` — version-controlled in the repo.

### Health Check Strategy

- **Decision**: Docker Compose health check pings the realm endpoint (`/realms/ev-platform`) via `curl`. If the realm doesn't exist yet (first run before admin console setup), the health check will fail until the realm is created.
- **Rationale**: The realm endpoint returning metadata JSON is the most reliable signal that Keycloak is fully initialized. A simple TCP check on port 8180 would pass before Keycloak is ready.
- **Note**: On first run, the health check will remain unhealthy until the admin configures the realm via the admin console. This is expected.

### Environment Variable Management

- **Decision**: Keycloak env vars defined inline in `docker-compose.yml` with `${VAR}` references resolved from `.env` file or shell environment. Example env files at `infra/env/keycloak.env.example`.
- **Rationale**: Existing project convention (see postgres, admin-service, driver-service env vars in docker-compose.yml). `.env` file is gitignored; `.example` files document required variables.
- **New env vars**: `KEYCLOAK_ADMIN`, `KEYCLOAK_ADMIN_PASSWORD`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `FACEBOOK_CLIENT_ID`, `FACEBOOK_CLIENT_SECRET` for keycloak; `KEYCLOAK_URL`, `KEYCLOAK_REALM` for backend services.

## Existing Patterns

From prior sprints:
- Docker Compose at repo root with postgres, admin-service, driver-service, dashboard, driver-web, driver-mobile
- Environment variables inlined in docker-compose.yml, referenced via `${VAR}` syntax
- Database migrations in `database/migrations/0001-0004`, executed by `sqlx::migrate!` at Rust service startup
- Root `.gitignore` excludes `.env` files
- API services expose health checks at `/api/health`

## Environment Variables Summary

| Variable | Default | Used By |
|----------|---------|---------|
| `KEYCLOAK_ADMIN` | `admin` | Keycloak initial admin user |
| `KEYCLOAK_ADMIN_PASSWORD` | `admin` | Keycloak initial admin password |
| `KEYCLOAK_URL` | `http://keycloak:8180` | Backend services (JWT validation) |
| `KEYCLOAK_REALM` | `ev-platform` | Backend services (JWT validation) |
| `GOOGLE_CLIENT_ID` | — | Google IdP (dev credentials) |
| `GOOGLE_CLIENT_SECRET` | — | Google IdP (dev credentials) |
| `FACEBOOK_CLIENT_ID` | — | Facebook IdP (dev credentials) |
| `FACEBOOK_CLIENT_SECRET` | — | Facebook IdP (dev credentials) |

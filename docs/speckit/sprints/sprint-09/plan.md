# Sprint 09 Plan — auth-service OIDC Broker

## Implementation Order

1. **Sprint docs** — spec, plan, tasks
2. **Cargo.toml** — no new deps needed (reqwest, uuid, serde suffice)
3. **oidc/mod.rs + client.rs** — OIDC adapter: authorize_url, exchange_code, refresh_token, logout_url
4. **session/mod.rs + manager.rs** — In-memory session store (RwLock<HashMap>), CSRF state tracking
5. **auth/mod.rs + login.rs + register.rs + callback.rs** — Auth flow handlers
6. **api/auth_routes.rs** — Auth route handlers wrapping flow handlers
7. **state.rs** — Add oidc_client, session_manager to AppState
8. **config.rs** — Add KEYCLOAK_CLIENT_ID, KEYCLOAK_CLIENT_SECRET, KEYCLOAK_REDIRECT_URI
9. **router.rs** — Add /auth/* routes
10. **lib.rs** — Add new modules
11. **main.rs** — Initialize OIDC client and session manager
12. **docker-compose.yml** — Add OIDC env vars
13. **cargo check** — Verify build
14. **Commit + PR**

## Key Decisions
- OIDC flow implemented manually with reqwest (no oauth2 crate dependency)
- Sessions stored in-memory (Arc<RwLock<HashMap>>) — no Redis in Sprint 09
- CSRF state tracked in-memory with expiry
- Admin-dashboard client (confidential) used for all OIDC flows
- JWT from token exchange decoded with existing JwtValidator infrastructure

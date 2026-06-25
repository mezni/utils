# Sprint 07 — Task Breakdown

## Phase 0: Documentation & Branch

- [ ] `0.1` Write spec.md, plan.md, tasks.md to `docs/speckit/sprints/sprint-07/`
- [ ] `0.2` Create branch `sprint/07-keycloak-users-foundation`

## Phase 1: Database — Keycloak DB Init

- [ ] `1.1` Create `migrations/keycloak_db/001_create_keycloak_db.sql`
  - `CREATE DATABASE keycloak_db WITH OWNER bornemap;`
- [ ] `1.2` Update `docker-compose.yml` postgres volumes to mount `migrations/keycloak_db/`

## Phase 2: Database — Users Schema

- [ ] `2.1` Create `migrations/platform_db/users/001_create_users_schema.sql`
  - `CREATE SCHEMA IF NOT EXISTS users;`
- [ ] `2.2` Create `migrations/platform_db/users/002_create_user_profiles.sql`
  - `CREATE TABLE users.user_profiles (...)`
  - UUID PK, email UNIQUE, deleted_at, timestamps
- [ ] `2.3` Update `migrations/init.sh` to process `users/` in the loop

## Phase 3: Keycloak Realm Config

- [ ] `3.1` Create `infra/keycloak/realm/bornemap-realm.json`
  - Realm: `bornemap`
  - Roles: `driver`, `partner`, `admin`, `super_admin`
  - Clients: `mobile-driver` (public, PKCE), `web-driver` (public, PKCE), `admin-dashboard` (confidential)
  - Default redirect URIs
- [ ] `3.2` Create `infra/keycloak/docker-compose.keycloak.yml`
  - Image: `quay.io/keycloak/keycloak:26.0`
  - Mode: `start-dev`
  - Port: `8080`
  - DB: keycloak_db on postgres
  - Realm import: `--import-realm`

## Phase 4: Keycloak Deployment

- [ ] `4.1` Update `docker-compose.yml` — add `keycloak` service
  - depends_on: postgres (healthy)
  - Mount realm import
  - Environment variables for DB connection
- [ ] `4.2` Start services and verify Keycloak boots
- [ ] `4.3` Verify `/health` endpoint responds

## Phase 5: Validation

- [ ] `5.1` Verify OIDC discovery endpoint: `GET /realms/bornemap/.well-known/openid-configuration`
- [ ] `5.2` Verify JWKS endpoint: `GET /realms/bornemap/protocol/openid-connect/certs`
- [ ] `5.3` Issue access token via client credentials grant
- [ ] `5.4` Verify JWT contains expected claims (`sub`, `email`, `realm_access.roles`)
- [ ] `5.5` Verify `users` schema exists in platform_db
- [ ] `5.6` Verify `user_profiles` table columns and constraints
- [ ] `5.7` Verify `keycloak_db` exists

## Phase 6: Documentation

- [ ] `6.1` Create `infra/keycloak/README.md` — operations guide
- [ ] `6.2` Create `infra/keycloak/validation.md` — validation procedures

## Phase 7: Delivery Artifacts

- [ ] `7.1` Generate SYSTEM_STATE.md
- [ ] `7.2` Generate sprint_state.json
- [ ] `7.3` Generate validation_report.md
- [ ] `7.4` Generate sprint_review.md
- [ ] `7.5` Generate follow_up.md

## Phase 8: Commit & PR

- [ ] `8.1` Commit all changes
- [ ] `8.2` Create PR

---

## Legend

- `[ ]` = pending
- `[x]` = completed
- `[~]` = in progress

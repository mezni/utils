# ADR-0003 — Keycloak as sole identity provider

- **Status**: Accepted
- **Date**: 2026-05-22
- **Deciders**: BorneMap core team
- **Tags**: security, identity, deployment

## Context

BorneMap has four user populations (anonymous public, driver, operator,
admin) and needs federation with at least Google. Building bespoke auth
is out of scope for an on-premises MVP. The Constitution (Principle V)
requires:

- A single identity provider.
- JWT validated at the gateway AND at each service.
- PKCE flow for interactive clients.
- The IdP never publicly exposed.
- Secrets via environment variables only.

## Decision

We will use **Keycloak** as the **sole identity provider** for BorneMap.

- Keycloak is deployed as a container on the internal Docker network.
- It is **not** routed publicly at NGINX. Only `auth-service` proxies
  the user-facing flows (`/auth/login`, `/auth/token`, `/auth/refresh`).
- Realm configuration (clients, roles, identity providers) is committed
  to the repository as a JSON export under `infra/keycloak/` and
  imported on first boot.
- Google OAuth federation is configured as an identity broker inside
  Keycloak (not by individual services).
- JWTs are RS256-signed; Keycloak public keys are fetched and cached by
  every service for independent validation.

## Alternatives considered

- **Auth0 / Cognito / Okta** — Rejected. SaaS dependency conflicts with
  the on-premises deployment posture stated in the Constitution.
- **Roll our own JWT issuer** — Rejected. Cost and risk far exceed
  Keycloak's footprint; no differentiating value.
- **Ory stack (Kratos + Hydra)** — Rejected for MVP. Higher operational
  surface for a solo developer; reconsider only via ADR if Keycloak
  becomes a bottleneck.

## Consequences

- **Positive**
  - Battle-tested OAuth/OIDC and PKCE support out of the box.
  - First-class identity brokering (Google now, others later) without
    code changes in BorneMap services.
  - Admin UI ships with Keycloak; no custom admin to build for user
    management beyond the role-mapping layer.
- **Negative**
  - Keycloak is a substantial dependency to operate (JVM, dedicated
    database in production, upgrade discipline).
  - Realm export must be kept in sync with running configuration; drift
    becomes a silent risk.
- **Follow-ups**
  - Phase 2 ships a JWT-validation middleware in every service with a
    dev-mode issuer so endpoints can be built without Keycloak running.
  - Phase 6 swaps the dev issuer for Keycloak and adds PKCE in the SPA.
  - A CI check (added when realm export lands) MUST diff the live realm
    against the committed JSON on tagged releases.

## Compliance check

- NGINX config review: no public route to Keycloak admin or direct
  account endpoints.
- Service startup logs MUST show the Keycloak JWKS URL in use; in
  non-dev environments this URL MUST be the Keycloak service.
- Penetration test in Phase 11 verifies no unauthenticated path leaks
  protected resources.

# Services

## Keycloak (Identity Provider)

- Manages login, token issuance, session management
- Handles roles and authentication only
- Does NOT handle business data (favorites, reviews, stations, partner data)

## Driver Service

- Rust backend service
- Handles driver-facing API endpoints
- Station discovery, search, favorites, reviews, profile
- Validates JWT tokens from Keycloak

## Admin Service

- Rust backend service
- Handles partner + admin API endpoints
- Station/charger CRUD, availability updates, moderation, reports
- Enforces partner scope at the API layer
- Validates JWT tokens from Keycloak

## Clickstream Service

- Rust backend service
- Ingests analytics events from frontends
- Validates event structure
- Publishes events to RabbitMQ

## GIS Sync Worker

- Rust background worker
- Polls outbox table for station changes
- Updates GIS spatial data asynchronously
- Failures do not block station updates

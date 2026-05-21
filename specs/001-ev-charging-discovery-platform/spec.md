# EV Charging Discovery Platform MVP Specification

## Feature Title and Summary

**Title:** EV Charging Discovery Platform MVP

**Summary:**
The EV Charging Discovery Platform MVP is designed to provide a high-performance, on-premises, internet-facing solution for seamless electric vehicle charger discovery for drivers. It offers multi-tenant control dashboards for station owners (Partners) and Super Admins, focusing on spatially optimized charger queries, asynchronous telemetry tracking, and secure identity management.

---

## Actors and User Personas

- **Drivers:**
  - Anonymous or authenticated users using a React Native mobile application to discover nearby EV charging stations.

- **Partners (Station Owners):**
  - Organizations or individuals who manage charging stations and use a stateless React SPA admin dashboard for station control and manual status overrides.

- **Super Admins:**
  - Platform-wide administrators responsible for tenant management, operator onboarding, and configuring system-wide settings.

- **System Components:**
  - Backend API implemented in Rust with Actix-web and SQLx handling geospatial queries.
  - Operational Database on PostgreSQL with PostGIS for spatial data.
  - Analytical Database using MongoDB for flexible clickstream event storage.
  - RabbitMQ message broker for asynchronous telemetry ingestion.
  - Keycloak identity and access management for multi-tenant authentication.
  - Nginx reverse proxy managing traffic, TLS termination, and rate limiting.

---

## Core Functional Workflows

### 1. Real-Time Spatial Discovery (Driver Core)

When a driver (anonymous or authenticated) opens the map in the React Native app, the client issues an optimized HTTP request to the `/api/v1/public/stations` endpoint with parameters specifying latitude, longitude, and a search radius.

The request passes through Nginx, which handles TLS termination and applies a dedicated public rate limiting zone for discovery requests to protect the backend from overloading.

The request is then forwarded to the Actix-web backend, which obtains a connection from a read-only SQLx pool connected to the PostgreSQL database with PostGIS extensions.

Using the PostGIS geography types and spatial indexes (GIST), a highly optimized query employing the `ST_DWithin` function is executed to retrieve all charging stations within the radius of the specified point.

The backend returns a JSON response to the client containing a vector frame with charger locations and relevant metadata. The spatial query executes with sub-millisecond latency even under high concurrent load.

### 2. Asynchronous Telemetry Ingestion (Clickstream Pipeline)

The React Native app buffers user interaction events such as map taps, screen transitions, and search keywords locally, batching 10 or more events or flushing on significant UI changes.

Batched payloads are dispatched to the `/api/v1/public/telemetry` endpoint exposed through Nginx, which applies a separate high-burst rate limiter to ensure responsiveness.

The Actix-web endpoint accepts the batch, parses the array of events, and immediately publishes them to a RabbitMQ exchange named `telemetry.events`.

The mobile client receives an HTTP 202 Accepted status immediately to avoid blocking.

A dedicated Rust worker asynchronously consumes messages from the RabbitMQ queue, converts event frames into BSON documents, and writes them in bulk to MongoDB for analytical storage, acknowledging messages upon successful insertion.

### 3. Identity, Onboarding & Backend-for-Frontend (BFF) Security

Administrative users interact through a React SPA dashboard. To mitigate Cross-Site Scripting (XSS) risks, the SPA does not directly handle Keycloak access tokens.

Instead, the Rust backend serves as a BFF layer, intercepting OAuth authorization flows, exchanging tokens server-side, and mapping credentials into encrypted, HttpOnly, SameSite=Strict cookies that the browser stores securely.

Super Admins may issue invitations to new operators. These invitations generate UUID tokens stored in a secure local ledger.

Verification links containing tokens are sent via an external SMTP relay.

Upon operator registration, the backend programmatically provisions user identities in Keycloak via its Admin REST API, linking them to their tenant partner context through local database references.

The React Native client retrieves dynamic client customization settings (e.g., logos, color schemes, search radius defaults) from a public configuration endpoint, enabling instant over-the-air UI updates without app store redeployments.

---

## Data Model Overview

The platform employs a dual-database data storage architecture to balance operational efficiency and flexible analytics.

### Operational Tier (PostgreSQL + PostGIS)

- Stores core transactional and spatial entities including Stations, Connectors, Partners, Registered Drivers, Saved Favorites, Station Reviews, Invitations, and App Configurations.
- Uses rigid, well-defined relational schemas with geospatial geometries encoded using SRID 4326.
- Employs PostGIS GIST indexes to accelerate highly performant spatial queries essential for real-time discovery.

### Analytical Tier (MongoDB)

- Captures unstructured and semi-structured behavioral event logs such as clickstream data, screen transitions, tap coordinates, and search keywords.
- Uses flexible, evolving BSON document schemas to accommodate new dynamic tracking attributes without rigid database migrations.
- Enables decoupling of volatile, high-frequency telemetry ingestion from operational processes, supporting scalable analytics and reporting.

This segregation ensures that intensive analytical queries and event ingestion workloads do not impede core transactional workflows, preserving system responsiveness for end users.

---

## Security and Perimeter Isolation Architecture

The entire platform infrastructure resides within a secure, on-premises containerized network environment.

- **Container Isolation:** PostgreSQL, MongoDB, Keycloak, RabbitMQ, and application backend components operate in private containers without exposed host ports.

- **Nginx Perimeter Proxy:** The sole exposed entry point is an Nginx reverse proxy handling TLS termination, static asset caching, cross-origin resource sharing (CORS) policies, and route-specific rate limiting.

- **Network Controls:** Direct access to internal services is blocked at the network level, forcing all external requests to route through the proxy.

- **Rate Limiting:** Distinct rate limiter configurations apply to discovery endpoints, telemetry ingestion routes, and admin portals to prevent abuse and ensure availability.

- **Backend-for-Frontend Security:** Administrative credentials are never exposed directly to frontends; instead, credentials flow through the backend-provided secure HttpOnly cookies bound to user sessions.

This strong perimeter isolation and layered defense strategy ensures tight security posture suitable for internet-facing, multi-tenant deployments while protecting sensitive backend resources.

---

## Non-Functional Requirements

- **Performance:**
  - The system must support sub-millisecond response times for spatial queries under typical peak loads.
  - Telemetry ingestion paths must guarantee minimal latency with batch acknowledgments within seconds.

- **Scalability:**
  - The architecture should accommodate scaling horizontally across services, particularly for telemetry processing and database read replicas.
  - Rate limiting should dynamically adapt to prevent overload during traffic spikes.

- **Availability:**
  - Core services should maintain high availability with failover strategies for databases and message brokers.
  - Planned maintenance windows must minimize user disruptions.

- **Observability:**
  - Comprehensive structured logging, metrics, and tracing must be implemented across components.
  - Alerting mechanisms should notify operators of critical failures or performance degradations.

---

## Success Criteria

- **Search Responsiveness:** 95% of spatial discovery queries return results within 500 milliseconds under typical load.
- **Telemetry Ingestion:** Over 99% of telemetry event batches are processed and persisted within 5 seconds of receipt.
- **Security Compliance:** No administrative credentials are exposed in browser-accessible scripts or storage.
- **System Availability:** Core platform services maintain 99.9% uptime excluding scheduled maintenance.
- **Scalability:** The platform supports at least 10,000 concurrent users with stable performance.
- **Configurability:** Client themes and parameters update dynamically without requiring app store releases or client restarts.

---

## Assumptions and Constraints

- Real-time charger hardware state tracking and billing session management are out of scope for the MVP.
- Administrative charger status changes rely on manual input; no live hardware protocols like OCPP are integrated.
- Deployment assumes a secure, dedicated on-premises container infrastructure isolated from public network access.
- External SMTP services are available for invitation email delivery under a secure internet gateway.
- Client applications are cross-platform React Native (mobile) and React SPA (admin) using secure, token-based authentication mediated by the backend.
- Database storage volumes and compute resources are provisioned to handle expected peak telemetry and operational loads.
- Rate limiting and perimeter protections are sufficient to prevent abuse but may require tuning during production.

---


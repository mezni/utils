# Implementation Plan

## Goal
Deliver BorneMap in MVP cycles that each prove a complete user loop without overbuilding infrastructure.

## MVP-1
### Goal
A partner creates stations and chargers via a dashboard, and a driver finds nearby stations on a map.

### Stack
- Backend: Python + FastAPI
- Database: PostgreSQL
- Driver Web: React + Vite + Tailwind + Leaflet
- Driver Mobile: React Native + Expo SDK 54 + react-native-maps
- Dashboard: React + Vite + Tailwind

### In Scope
- `/api` station, partner, and charger CRUD
- nearby station lookup with simple distance logic
- driver web map and station detail
- driver mobile map and station detail
- dashboard overview, partners, stations, chargers
- Dockerfiles and local Docker Compose

### Out of Scope
- authentication, Keycloak, JWT, accounts
- favorites, reviews, GIS sync, PostGIS, OSM import
- analytics, reporting, Traefik, TLS, CI/CD
- production launch hardening

### Done Criteria
- all endpoints work against a real database
- dashboard CRUD works for partners, stations, chargers
- driver web shows real markers and station detail
- driver mobile shows real markers on iOS and Android
- create in dashboard, then see data in driver apps
- apps handle API unreachable gracefully
- onboarding guide works from scratch
- API documentation is complete
- zero Class A bugs

## MVP-2
### Goal
Replace the Python service with Rust, add PostGIS, and introduce CI/CD.

### Notes
- this MVP should preserve MVP-1 behavior
- database and infrastructure changes must not break earlier flows

## MVP-3
### Goal
Add authentication and user management with Keycloak.

## MVP-4
### Goal
Add GIS synchronization via PostgreSQL triggers.

## MVP-5
### Goal
Add analytics and reporting.

## MVP-6
### Goal
Production hardening, Traefik, and launch readiness.

# Contracts: Keycloak Authentication

## Overview

This directory defines the interface contracts between backend services and Keycloak. Backend services (`driver-service`, `admin-service`) act as confidential OAuth2 clients that validate incoming JWTs and optionally obtain service account tokens.

## Contract Files

- [jwt-validation.md](./jwt-validation.md) — JWT structure, validation rules, and JWKS endpoint contract
- [service-auth.md](./service-auth.md) — Service account authentication flow for inter-service communication
- [api-endpoints.md](./api-endpoints.md) — Keycloak REST API endpoints used by services and admin procedures

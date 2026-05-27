# BorneMap Architecture

## Overview

BorneMap is a geospatial EV charging discovery platform for the Tunisian market.

## System Components

- **api-service**: Rust/Actix-web HTTP gateway — business router and public API
- **auth-service**: Identity provider, credential processing, RBAC (stub for MVP)
- **core**: Plain domain entities and shared error structures
- **infra**: Shared database connection pools and PostGIS clients
- **mobile-driver**: React Native / Expo Go driver mobile app
- **web-admin**: React admin portal (future)

## Data Flow

Mobile client → api-service → (mock data / future: PostGIS via infra)

## Stack

- Backend: Rust + Actix-web
- Database: PostgreSQL + PostGIS
- Cache: Redis (future)
- Message Broker: RabbitMQ (future)
- Mobile: React Native / Expo Go
- Admin: React (Vite / Next.js)

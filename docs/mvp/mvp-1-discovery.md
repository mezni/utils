BorneMap MVP-1 — EXECUTION PLAN (REWRITTEN)
Version: 2.0
Name: Discovery Core (Map System)
0. 🎯 MVP-1 OBJECTIVE (LOCKED)

Deliver a working geospatial discovery product:

Users can:
Open map (web + mobile)
See EV charging stations (real DB data)
Move map → fetch nearby stations (PostGIS)
Tap station → view details
1. 🧱 EXECUTION PRINCIPLE

Infrastructure → Data → Backend → API → Frontend → UX

No exceptions.

2. 🏗️ PHASE 0 — INFRASTRUCTURE + DATABASE (FIRST STEP)
2.1 Create infrastructure
infra/
├── docker-compose.yml
└── migrations/
2.2 Start databases
REQUIRED DATABASES:
platform_db (PostgreSQL + PostGIS)
analytics_db (append-only)
2.3 Enable PostGIS
CREATE EXTENSION postgis;
2.4 Create schemas

Inside platform_db:

inventory
gis (read-only conceptually)
users (empty for MVP-1)
2.5 Seed data (CRITICAL)

Insert initial stations:

10–20 stations
Tunisia coordinates
mixed statuses:
active
maintenance
PHASE 0 DONE IF:
DB runs via Docker
PostGIS enabled
stations exist in DB
query returns real rows
3. 🧠 PHASE 1 — BACKEND (driver-service)
3.1 Create service
source/services/driver-service/
3.2 Responsibilities
expose station APIs
run PostGIS queries
no business logic in handlers
3.3 REQUIRED ENDPOINTS
GET /api/v1/stations
GET /api/v1/stations/nearby?lat&lng&radius
GET /api/v1/stations/{id}
3.4 Nearby logic (PostGIS)
distance calculation required
sorted ASC by distance
filtered by active only
PHASE 1 DONE IF:
API returns real DB data
nearby search works correctly
no mock data remains
4. 🔌 PHASE 2 — API CLIENT LAYER
4.1 Create:
source/front/packages/@bm/api-client
4.2 Functions:
getStations()
getNearbyStations()
getStationById()
RULES:
NO fetch() in apps
NO direct HTTP calls
strict typing via @bm/types
PHASE 2 DONE IF:
frontend uses ONLY api-client
endpoints match backend 1:1
5. 📱 PHASE 3 — FRONTEND FOUNDATION
Apps:
mobile-driver
web-driver
SETUP:
React Query (server state)
Zustand (UI state only)
MapContainer abstraction created
design tokens integrated
PHASE 3 DONE IF:
apps run cleanly
no map logic yet implemented
state separation enforced
6. 🗺️ PHASE 4 — MAP SYSTEM (CORE ENGINE)
6.1 Map abstraction
MapContainer.ts
MapContainer.native.ts
MapContainer.web.ts
RULES:
no direct map library usage outside this layer
no duplication across platforms
no UI components touching map engine
PHASE 4 DONE IF:
map renders both platforms
user location works
basic interactions functional
7. 📍 PHASE 5 — NEARBY SEARCH (CORE FEATURE)
Trigger:
map move
GPS update
initial load
Flow:
map center → API → PostGIS → stations → markers update
RULES:
debounce 300–500ms
no full map rerender
incremental marker updates only
PHASE 5 DONE IF:
stations update live on movement
performance is smooth
no flickering
8. 🧾 PHASE 6 — STATION DETAILS
UI:
mobile → bottom sheet
web → side panel
DATA:
GET /api/v1/stations/{id}
MUST SHOW:
name
status
chargers
connector types
distance
PHASE 6 DONE IF:
correct station loads
UI is responsive
no lag on open
9. 🎨 PHASE 7 — UX POLISH (PRO MAX RULE)
REQUIRED:
skeleton loading (not spinner)
smooth transitions
haptic feedback (mobile)
empty states designed
EMPTY STATES:
no stations nearby
GPS unavailable
network failure
10. 🧪 PHASE 8 — VALIDATION + FREEZE
FINAL CHECKLIST:
Functional:
map loads
stations render
nearby search works
station details open
Technical:
no fetch outside api-client
no map logic outside MapContainer
no extra services introduced
UX:
no jank
smooth interactions
consistent behavior across platforms
🚫 HARD CONSTRAINTS
NO auth system
NO admin/dashboard work
NO analytics UI expansion
NO new services
NO MVP-2 leakage
🧠 CORE PRINCIPLE

MVP-1 is a vertical slice of reality: data → map → interaction. PostGIS Infrastructure
Version: MVP-1 Ready
1. 🐳 infra/docker-compose.yml
version: "3.9"

services:
  platform_db:
    image: postgis/postgis:16-3.4
    container_name: bornemap-platform-db
    restart: unless-stopped
    environment:
      POSTGRES_USER: bornemap
      POSTGRES_PASSWORD: bornemap_dev
      POSTGRES_DB: platform_db
    ports:
      - "5432:5432"
    volumes:
      - platform_db_data:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d
    command: >
      postgres -c shared_preload_libraries=postgis

  analytics_db:
    image: postgres:16
    container_name: bornemap-analytics-db
    restart: unless-stopped
    environment:
      POSTGRES_USER: analytics
      POSTGRES_PASSWORD: analytics_dev
      POSTGRES_DB: analytics_db
    ports:
      - "5433:5432"
    volumes:
      - analytics_db_data:/var/lib/postgresql/data

volumes:
  platform_db_data:
  analytics_db_data:
2. 📦 Migration System
Folder structure
infra/migrations/
├── 001_extensions.sql
├── 002_schema_inventory.sql
├── 003_seed_stations.sql
3. ⚙️ Migration 1 — PostGIS Extension
001_extensions.sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
4. 🧱 Migration 2 — Inventory Schema
002_schema_inventory.sql
CREATE SCHEMA IF NOT EXISTS inventory;

-- Stations table
CREATE TABLE inventory.station (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',

    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    location GEOGRAPHY(POINT, 4326) NOT NULL,

    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for PostGIS queries
CREATE INDEX idx_station_location
ON inventory.station
USING GIST (location);
5. 🌍 Migration 3 — Seed Data (Tunisia)
003_seed_stations.sql
INSERT INTO inventory.station (id, name, status, latitude, longitude, location)
VALUES

('STA-001', 'Tunis Centre Charging Hub', 'active', 36.8065, 10.1815,
 ST_SetSRID(ST_MakePoint(10.1815, 36.8065), 4326)),

('STA-002', 'Sfax EV Station', 'active', 34.7406, 10.7603,
 ST_SetSRID(ST_MakePoint(10.7603, 34.7406), 4326)),

('STA-003', 'Sousse Fast Charge', 'maintenance', 35.8256, 10.6084,
 ST_SetSRID(ST_MakePoint(10.6084, 35.8256), 4326)),

('STA-004', 'Bizerte Coastal Charge', 'active', 37.2746, 9.8739,
 ST_SetSRID(ST_MakePoint(9.8739, 37.2746), 4326)),

('STA-005', 'Gabes EV Point', 'active', 33.8815, 10.0982,
 ST_SetSRID(ST_MakePoint(10.0982, 33.8815), 4326));
6. 🧠 IMPORTANT POSTGIS RULE

We store:

latitude/longitude (debug + API convenience)
location GEOGRAPHY (for real queries)
7. 🧭 REQUIRED QUERY (NEARBY)

This is what your Rust service MUST use:

SELECT
  id,
  name,
  status,
  latitude,
  longitude,
  ST_Distance(
    location,
    ST_SetSRID(ST_MakePoint($1, $2), 4326)
  ) AS distance
FROM inventory.station
WHERE status = 'active'
ORDER BY location <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
LIMIT 50;
8. 🚀 HOW TO RUN
cd infra
docker compose up -d
9. 🧪 VALIDATION CHECKLIST
DB must:
start without errors
expose port 5432
contain inventory.station
return seed stations
support PostGIS distance queries
10. 🚫 NON-NEGOTIABLE RULES
NO ORM hiding PostGIS logic
NO storing only lat/lng without GEOGRAPHY
NO runtime schema creation
NO app writes to GIS schema (future rule)
NO analytics in this DB
🧠 CORE PRINCIPLE

Database is not storage — it is the geospatial engine of MVP-1. BorneMap — driver-service (MVP-1)
Stack:
Actix-web
SQLx (Postgres)
PostGIS
Tokio
Serde
📁 1. PROJECT STRUCTURE
source/services/driver-service/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── db.rs
│   ├── error.rs
│   ├── routes/
│   │   ├── mod.rs
│   │   └── stations.rs
│   ├── handlers/
│   │   └── stations.rs
│   ├── services/
│   │   └── station_service.rs
│   ├── repositories/
│   │   └── station_repo.rs
│   └── models/
│       └── station.rs
📦 2. CARGO.TOML
[package]
name = "driver-service"
version = "0.1.0"
edition = "2021"

[dependencies]
actix-web = "4"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sqlx = { version = "0.7", features = [
    "runtime-tokio-rustls",
    "postgres",
    "macros"
] }
dotenv = "0.15"
thiserror = "1"
uuid = { version = "1", features = ["serde", "v4"] }
⚙️ 3. CONFIG
src/config.rs
use std::env;

pub struct Config {
    pub database_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
        }
    }
}
🗄️ 4. DB CONNECTION
src/db.rs
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

pub type DbPool = Pool<Postgres>;

pub async fn create_pool(database_url: &str) -> DbPool {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .expect("Failed to connect to DB")
}
🚨 5. ERROR HANDLING
src/error.rs
use actix_web::{HttpResponse, ResponseError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error")]
    DbError(#[from] sqlx::Error),

    #[error("Not found")]
    NotFound,
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::NotFound => HttpResponse::NotFound().json("Not found"),
            AppError::DbError(_) => HttpResponse::InternalServerError().json("DB error"),
        }
    }
}
📍 6. MODEL
src/models/station.rs
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub status: String,
    pub latitude: f64,
    pub longitude: f64,
}
🧱 7. REPOSITORY (POSTGIS LAYER)
src/repositories/station_repo.rs
use sqlx::PgPool;
use crate::models::station::Station;

pub struct StationRepository;

impl StationRepository {
    pub async fn get_all(pool: &PgPool) -> Result<Vec<Station>, sqlx::Error> {
        sqlx::query_as!(
            Station,
            r#"
            SELECT id, name, status, latitude, longitude
            FROM inventory.station
            "#
        )
        .fetch_all(pool)
        .await
    }

    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Station, sqlx::Error> {
        sqlx::query_as!(
            Station,
            r#"
            SELECT id, name, status, latitude, longitude
            FROM inventory.station
            WHERE id = $1
            "#,
            id
        )
        .fetch_one(pool)
        .await
    }

    // 🌍 POSTGIS CORE QUERY
    pub async fn get_nearby(
        pool: &PgPool,
        lng: f64,
        lat: f64,
        radius: f64
    ) -> Result<Vec<Station>, sqlx::Error> {
        sqlx::query_as!(
            Station,
            r#"
            SELECT
                id,
                name,
                status,
                latitude,
                longitude
            FROM inventory.station
            WHERE status = 'active'
            AND ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint($1, $2), 4326),
                $3
            )
            ORDER BY location <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
            "#,
            lng,
            lat,
            radius
        )
        .fetch_all(pool)
        .await
    }
}
🧠 8. SERVICE LAYER
src/services/station_service.rs
use sqlx::PgPool;
use crate::repositories::station_repo::StationRepository;
use crate::models::station::Station;

pub struct StationService;

impl StationService {
    pub async fn get_all(pool: &PgPool) -> Result<Vec<Station>, sqlx::Error> {
        StationRepository::get_all(pool).await
    }

    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Station, sqlx::Error> {
        StationRepository::get_by_id(pool, id).await
    }

    pub async fn get_nearby(
        pool: &PgPool,
        lng: f64,
        lat: f64,
        radius: f64
    ) -> Result<Vec<Station>, sqlx::Error> {
        StationRepository::get_nearby(pool, lng, lat, radius).await
    }
}
🌐 9. HANDLERS (ACTIX LAYER)
src/handlers/stations.rs
use actix_web::{get, web, HttpResponse};
use sqlx::PgPool;

use crate::services::station_service::StationService;

#[get("/stations")]
pub async fn get_stations(pool: web::Data<PgPool>) -> HttpResponse {
    match StationService::get_all(pool.get_ref()).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[get("/stations/{id}")]
pub async fn get_station_by_id(
    pool: web::Data<PgPool>,
    id: web::Path<String>,
) -> HttpResponse {
    match StationService::get_by_id(pool.get_ref(), &id).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/stations/nearby")]
pub async fn get_nearby(
    pool: web::Data<PgPool>,
    query: web::Query<NearbyQuery>,
) -> HttpResponse {
    match StationService::get_nearby(
        pool.get_ref(),
        query.lng,
        query.lat,
        query.radius,
    )
    .await
    {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[derive(serde::Deserialize)]
pub struct NearbyQuery {
    pub lng: f64,
    pub lat: f64,
    pub radius: f64,
}
🧭 10. ROUTES
src/routes/mod.rs
use actix_web::web;
use crate::handlers::stations;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .service(stations::get_stations)
            .service(stations::get_station_by_id)
            .service(stations::get_nearby),
    );
}
🚀 11. MAIN
src/main.rs
use actix_web::{App, HttpServer, web};
use dotenv::dotenv;

mod config;
mod db;
mod error;
mod models;
mod repositories;
mod services;
mod handlers;
mod routes;

use config::Config;
use db::create_pool;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    let config = Config::from_env();
    let pool = create_pool(&config.database_url).await;

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .configure(routes::config)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
🧠 ARCHITECTURE GUARANTEES

This service enforces:

handler → service → repository layering
no SQL in handlers
PostGIS isolated in repository layer
no cross-service coupling
MVP-1 scope only
🚫 NON-NEGOTIABLE RULES
no ORM abstraction over PostGIS logic
no business logic in handlers
no external service calls
no auth logic (MVP-1)
no analytics writes
🧠 CORE PRINCIPLE

The driver-service is not an API layer. It is the geospatial computation engine of MVP-1. BorneMap Frontend — Map Integration (MVP-1)
Scope
Load map
Fetch nearby stations
Render markers
Select station → open detail
Update markers on map move
📁 1. FRONTEND STRUCTURE (relevant parts)
source/front/apps/web-driver/
source/front/apps/mobile-driver/

source/front/packages/@bm/api-client/
source/front/packages/@bm/types/
📦 2. API CLIENT (REQUIRED CONTRACT)
@bm/api-client/src/stations.ts
import { Station } from "@bm/types";

const BASE_URL = "http://localhost:8080/api/v1";

export async function getNearbyStations(params: {
  lat: number;
  lng: number;
  radius: number;
}): Promise<Station[]> {
  const res = await fetch(
    `${BASE_URL}/stations/nearby?lat=${params.lat}&lng=${params.lng}&radius=${params.radius}`
  );

  if (!res.ok) throw new Error("Failed to fetch nearby stations");

  return res.json();
}

export async function getStationById(id: string): Promise<Station> {
  const res = await fetch(`${BASE_URL}/stations/${id}`);
  if (!res.ok) throw new Error("Failed to fetch station");

  return res.json();
}

export async function getStations(): Promise<Station[]> {
  const res = await fetch(`${BASE_URL}/stations`);
  if (!res.ok) throw new Error("Failed to fetch stations");

  return res.json();
}
🧠 3. TYPES (@bm/types)
export interface Station {
  id: string;
  name: string;
  status: string;
  latitude: number;
  longitude: number;
}
⚛️ 4. REACT QUERY HOOKS
useNearbyStations.ts
import { useQuery } from "@tanstack/react-query";
import { getNearbyStations } from "@bm/api-client";

export function useNearbyStations(lat: number, lng: number, radius: number) {
  return useQuery({
    queryKey: ["stations", "nearby", lat, lng, radius],
    queryFn: () => getNearbyStations({ lat, lng, radius }),
    enabled: !!lat && !!lng,
    staleTime: 30_000,
  });
}
useStation.ts
import { useQuery } from "@tanstack/react-query";
import { getStationById } from "@bm/api-client";

export function useStation(id?: string) {
  return useQuery({
    queryKey: ["station", id],
    queryFn: () => getStationById(id!),
    enabled: !!id,
  });
}
🧭 5. UI STATE (ZUSTAND)
import { create } from "zustand";

interface MapState {
  selectedStationId: string | null;
  setSelectedStationId: (id: string | null) => void;

  mapCenter: { lat: number; lng: number };
  setMapCenter: (c: { lat: number; lng: number }) => void;

  radius: number;
  setRadius: (r: number) => void;
}

export const useMapStore = create<MapState>((set) => ({
  selectedStationId: null,
  setSelectedStationId: (id) => set({ selectedStationId: id }),

  mapCenter: { lat: 36.8065, lng: 10.1815 },
  setMapCenter: (c) => set({ mapCenter: c }),

  radius: 5000,
  setRadius: (r) => set({ radius: r }),
}));
🗺️ 6. MAP CONTAINER (WEB EXAMPLE)
MapContainer.web.tsx

(using Leaflet)

import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import { useNearbyStations } from "../hooks/useNearbyStations";
import { useMapStore } from "../store/mapStore";

export function MapView() {
  const { mapCenter, radius, setSelectedStationId } = useMapStore();

  const { data: stations = [] } = useNearbyStations(
    mapCenter.lat,
    mapCenter.lng,
    radius
  );

  return (
    <MapContainer
      center={[mapCenter.lat, mapCenter.lng]}
      zoom={13}
      style={{ height: "100vh", width: "100%" }}
    >
      <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

      {stations.map((s) => (
        <Marker
          key={s.id}
          position={[s.latitude, s.longitude]}
          eventHandlers={{
            click: () => setSelectedStationId(s.id),
          }}
        >
          <Popup>
            <strong>{s.name}</strong>
            <div>{s.status}</div>
          </Popup>
        </Marker>
      ))}
    </MapContainer>
  );
}
📱 7. MOBILE MAP (react-native-maps)
import MapView, { Marker } from "react-native-maps";
import { useNearbyStations } from "../hooks/useNearbyStations";
import { useMapStore } from "../store/mapStore";

export function MapScreen() {
  const { mapCenter, radius, setSelectedStationId } = useMapStore();

  const { data: stations = [] } = useNearbyStations(
    mapCenter.lat,
    mapCenter.lng,
    radius
  );

  return (
    <MapView
      style={{ flex: 1 }}
      region={{
        latitude: mapCenter.lat,
        longitude: mapCenter.lng,
        latitudeDelta: 0.05,
        longitudeDelta: 0.05,
      }}
    >
      {stations.map((s) => (
        <Marker
          key={s.id}
          coordinate={{
            latitude: s.latitude,
            longitude: s.longitude,
          }}
          onPress={() => setSelectedStationId(s.id)}
        />
      ))}
    </MapView>
  );
}
🔁 8. MAP MOVEMENT → NEARBY UPDATE FLOW
user moves map
→ update mapCenter
→ React Query triggers refetch
→ driver-service /stations/nearby
→ PostGIS computes results
→ markers update
⚡ 9. PERFORMANCE RULES (IMPORTANT)
MUST:
debounce map movement (300–500ms)
React Query caching enabled
no full map rerender
markers must be memoized
FORBIDDEN:
fetch() in components
recalculating markers on every render
uncontrolled map state loops
🧠 10. UX BEHAVIOR CONTRACT
markers update smoothly (no flicker)
selection persists across refresh
map never blocks UI thread
loading = skeleton markers (not spinner)
🚫 NON-NEGOTIABLE RULES
API calls ONLY via @bm/api-client
React Query = server state ONLY
Zustand = UI state ONLY
Map logic ONLY inside MapContainer
No cross-app duplication logic
🧠 CORE PRINCIPLE

Map is not a component. It is a reactive geospatial system.
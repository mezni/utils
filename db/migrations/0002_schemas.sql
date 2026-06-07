-- Migration 0002: Create Schemas
-- Purpose: Create inventory and gis schemas
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Inventory schema for business entities (partners, stations, chargers, availability)
CREATE SCHEMA IF NOT EXISTS inventory;

-- GIS schema for spatial data (OSM tables, station locations, spatial indexes)
CREATE SCHEMA IF NOT EXISTS gis;

-- Migration 0003: Inventory Tables
-- Purpose: Create partner, station, charger, and station_availability tables
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Partner table: Organization that owns charging stations
CREATE TABLE IF NOT EXISTS inventory.partner (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Station table: Physical charging station location
CREATE TABLE IF NOT EXISTS inventory.station (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES inventory.partner(id),
    name TEXT NOT NULL,
    address TEXT,
    latitude NUMERIC(10,7) NOT NULL,
    longitude NUMERIC(10,7) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Charger table: Individual charging unit at a station
CREATE TABLE IF NOT EXISTS inventory.charger (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES inventory.station(id),
    connector_type TEXT NOT NULL CHECK (connector_type IN ('Type2','Type2Combo','Chademo','CCS','Schuko','Wall')),
    power_kw NUMERIC(6,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'Available' CHECK (status IN ('Available','Charging','Offline','Maintenance','Reserved','Unknown')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Station availability table: Operational status history
CREATE TABLE IF NOT EXISTS inventory.station_availability (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES inventory.station(id),
    status TEXT NOT NULL CHECK (status IN ('Available','Unavailable','Partial')),
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

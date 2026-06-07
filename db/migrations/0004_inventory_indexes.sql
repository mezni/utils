-- Migration 0004: Inventory Indexes
-- Purpose: Create indexes for query performance on inventory tables
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Composite index on station coordinates for spatial queries
CREATE INDEX IF NOT EXISTS inventory.idx_station_coords ON inventory.station(latitude, longitude);

-- Index on station partner_id for filtering by partner
CREATE INDEX IF NOT EXISTS inventory.idx_station_partner_id ON inventory.station(partner_id);

-- Index on charger station_id for filtering by station
CREATE INDEX IF NOT EXISTS inventory.idx_charger_station_id ON inventory.charger(station_id);

-- Index on station_availability station_id for filtering by station
CREATE INDEX IF NOT EXISTS inventory.idx_availability_station_id ON inventory.station_availability(station_id);

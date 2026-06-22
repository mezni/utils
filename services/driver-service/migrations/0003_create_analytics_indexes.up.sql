-- Migrations for driver-service: analytics indexes
-- Purpose: Add performance indexes for analytics queries

-- Additional indexes for analytics performance
CREATE INDEX IF NOT EXISTS idx_analytics_events_raw_event_id ON telemetry.analytics_events(event_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type_created ON telemetry.raw_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_events_event_type ON telemetry.analytics_events(event_type);
CREATE INDEX IF NOT EXISTS idx_analytics_events_created_at ON telemetry.analytics_events(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_events_raw_event_id ON telemetry.system_events(event_id);
CREATE INDEX IF NOT EXISTS idx_system_events_created_at ON telemetry.system_events(processed_at DESC);

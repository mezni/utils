-- Migrations for driver-service: analytics schema
-- Purpose: Store telemetry, analytics events, and system events (raw_events)

-- Create analytics schemas
CREATE SCHEMA IF NOT EXISTS telemetry;
CREATE SCHEMA IF NOT EXISTS analytics_events;
CREATE SCHEMA IF NOT EXISTS system_events;

-- Raw events table (append-only event log as primary model)
CREATE TABLE IF NOT EXISTS telemetry.raw_events (
    event_id VARCHAR(15) PRIMARY KEY CHECK (event_id ~ '^EVT[a-zA-Z0-9]{11}$'),
    event_type VARCHAR(100) NOT NULL,
    source_service VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Analytics events (derived from raw events)
CREATE TABLE IF NOT EXISTS telemetry.analytics_events (
    event_id VARCHAR(15) PRIMARY KEY REFERENCES telemetry.raw_events(event_id),
    event_type VARCHAR(100) NOT NULL,
    aggregated_data JSONB,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- System events
CREATE TABLE IF NOT EXISTS telemetry.system_events (
    event_id VARCHAR(15) PRIMARY KEY REFERENCES telemetry.raw_events(event_id),
    event_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    event_data JSONB NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_raw_events_type ON telemetry.raw_events(event_type);
CREATE INDEX idx_raw_events_created_at ON telemetry.raw_events(created_at DESC);
CREATE INDEX idx_analytics_events_type ON telemetry.analytics_events(event_type);
CREATE INDEX idx_analytics_events_created_at ON telemetry.analytics_events(processed_at DESC);
CREATE INDEX idx_system_events_severity ON telemetry.system_events(severity);
CREATE INDEX idx_system_events_type ON telemetry.system_events(event_type);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA telemetry TO bornemap_analytics_writer;
GRANT ALL PRIVILEGES ON TABLE telemetry.raw_events TO bornemap_analytics_writer;
GRANT ALL PRIVILEGES ON TABLE telemetry.analytics_events TO bornemap_analytics_writer;
GRANT ALL PRIVILEGES ON TABLE telemetry.system_events TO bornemap_analytics_writer;
GRANT USAGE ON SCHEMA telemetry TO bornemap_analytics_reader, bornemap_driver;
GRANT SELECT ON TABLE telemetry.raw_events TO bornemap_analytics_reader, bornemap_driver;
GRANT SELECT ON TABLE telemetry.analytics_events TO bornemap_analytics_reader;
GRANT SELECT ON TABLE telemetry.system_events TO bornemap_analytics_reader, bornemap_driver;

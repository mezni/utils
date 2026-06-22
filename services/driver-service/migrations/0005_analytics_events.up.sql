-- Create analytics_events table for telemetry ingestion
-- Idempotency enforcement using UUID v7
-- Schema version governance (unknown/deprecated versions rejected)

CREATE TABLE IF NOT EXISTS analytics_events (
    id BIGSERIAL PRIMARY KEY,
    schema_version VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    idempotency_key UUID NOT NULL,
    location_latitude DECIMAL(10, 8),
    location_longitude DECIMAL(11, 8),
    location_country VARCHAR(100),
    location_city VARCHAR(100),
    location_source VARCHAR(50) NOT NULL,
    session_start TIMESTAMP WITH TIME ZONE NOT NULL,
    session_duration INTEGER NOT NULL,
    role VARCHAR(50) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    event_source VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create unique index on idempotency_key for duplicate detection
CREATE UNIQUE INDEX IF NOT EXISTS idx_analytics_events_idempotency_key
ON analytics_events (idempotency_key);

-- Create index on user_id for filtering
CREATE INDEX IF NOT EXISTS idx_analytics_events_user_id
ON analytics_events (user_id);

-- Create index on timestamp for time range queries
CREATE INDEX IF NOT EXISTS idx_analytics_events_timestamp
ON analytics_events (timestamp DESC);

-- Create index on event_type for filtering
CREATE INDEX IF NOT EXISTS idx_analytics_events_event_type
ON analytics_events (event_type);

-- Create index on schema_version for version filtering
CREATE INDEX IF NOT EXISTS idx_analytics_events_schema_version
ON analytics_events (schema_version);

-- Create index on location_source for provenance queries
CREATE INDEX IF NOT EXISTS idx_analytics_events_location_source
ON analytics_events (location_source);

-- Create index on status for status filtering
CREATE INDEX IF NOT EXISTS idx_analytics_events_status
ON analytics_events (status);

-- Add comment for documentation
COMMENT ON TABLE analytics_events IS 'Telemetry events from auth-service, driver-service, and inventory-service';
COMMENT ON COLUMN analytics_events.idempotency_key IS 'UUID v7 for idempotency enforcement';
COMMENT ON COLUMN analytics_events.location_source IS 'Provenance: EVENT_LOCATION, SESSION_LOCATION, LAST_KNOWN_LOCATION, DEFAULT_LOCATION';
COMMENT ON COLUMN analytics_events.event_type IS 'Fixed enum value: AUTH_LOGIN, AUTH_LOGOUT, TOKEN_REFRESH, LOCATION_UPDATE, SESSION_START, SESSION_END, DRIVER_STATUS, INVENTORY_UPDATE, PRICE_CHANGE, STOCK_ALERT, ERROR_UNHANDLED';

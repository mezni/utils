-- Create analytics_events_dead_letter table for malformed events
-- Includes provenance and error details for debugging

CREATE TABLE IF NOT EXISTS analytics_events_dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_event_id UUID NOT NULL,
    original_event JSONB NOT NULL,
    error_type VARCHAR(255) NOT NULL,
    error_message TEXT NOT NULL,
    error_stack_trace TEXT,
    event_schema_version VARCHAR(50),
    location_source VARCHAR(50),
    captured_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    original_request_id VARCHAR(255),
    retry_attempts INTEGER NOT NULL DEFAULT 0,
    event_type VARCHAR(50),
    user_id UUID,
    timestamp TIMESTAMP WITH TIME ZONE
);

-- Create index on captured_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_dead_letter_captured_at
ON analytics_events_dead_letter (captured_at DESC);

-- Create index on error_type for error analysis
CREATE INDEX IF NOT EXISTS idx_dead_letter_error_type
ON analytics_events_dead_letter (error_type);

-- Create index on original_request_id for traceability
CREATE INDEX IF NOT EXISTS idx_dead_letter_original_request_id
ON analytics_events_dead_letter (original_request_id);

-- Add comment for documentation
COMMENT ON TABLE analytics_events_dead_letter IS 'Dead-letter store for malformed telemetry events (not a queue)';
COMMENT ON COLUMN analytics_events_dead_letter.location_source IS 'Provenance: EVENT_LOCATION, SESSION_LOCATION, LAST_KNOWN_LOCATION, DEFAULT_LOCATION';
COMMENT ON COLUMN analytics_events_dead_letter.error_type IS 'Type of validation error';

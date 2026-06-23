-- Materialized view metadata table
-- Tracks last refresh times for materialized views

-- Create metadata table
CREATE TABLE IF NOT EXISTS materialized_view_meta (
    view_name TEXT PRIMARY KEY,
    last_refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rows_count BIGINT NOT NULL DEFAULT 0,
    refresh_duration_ms BIGINT NOT NULL DEFAULT 0
);

-- Insert initial metadata for materialized views
INSERT INTO materialized_view_meta (view_name, rows_count, refresh_duration_ms)
VALUES
    ('station_usage', 0, 0),
    ('user_activity', 0, 0),
    ('search_trends', 0, 0)
ON CONFLICT (view_name) DO NOTHING;

-- Function to refresh station_usage materialized view
CREATE OR REPLACE FUNCTION refresh_station_usage()
RETURNS void AS $$
BEGIN
    -- Record start time
    DECLARE
        start_time TIMESTAMP;
        duration_ms BIGINT;
    BEGIN
        start_time := CURRENT_TIMESTAMP;

        -- TRUNCATE existing data
        TRUNCATE TABLE station_usage;

        -- Refresh from analytics_events
        REFRESH MATERIALIZED VIEW CONCURRENTLY station_usage;

        -- Record refresh metadata
        duration_ms := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)) * 1000;
        INSERT INTO materialized_view_meta (view_name, last_refreshed_at, rows_count, refresh_duration_ms)
        VALUES ('station_usage', CURRENT_TIMESTAMP, (SELECT COUNT(*) FROM station_usage), duration_ms)
        ON CONFLICT (view_name) DO UPDATE SET
            last_refreshed_at = CURRENT_TIMESTAMP,
            rows_count = (SELECT COUNT(*) FROM station_usage),
            refresh_duration_ms = duration_ms;

        RAISE NOTICE '✅ station_usage materialized view refreshed successfully (rows: %, duration: %ms)', (SELECT COUNT(*) FROM station_usage), duration_ms;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh user_activity materialized view
CREATE OR REPLACE FUNCTION refresh_user_activity()
RETURNS void AS $$
BEGIN
    DECLARE
        start_time TIMESTAMP;
        duration_ms BIGINT;
    BEGIN
        start_time := CURRENT_TIMESTAMP;

        -- TRUNCATE existing data
        TRUNCATE TABLE user_activity;

        -- Refresh from analytics_events
        REFRESH MATERIALIZED VIEW CONCURRENTLY user_activity;

        -- Record refresh metadata
        duration_ms := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)) * 1000;
        INSERT INTO materialized_view_meta (view_name, last_refreshed_at, rows_count, refresh_duration_ms)
        VALUES ('user_activity', CURRENT_TIMESTAMP, (SELECT COUNT(*) FROM user_activity), duration_ms)
        ON CONFLICT (view_name) DO UPDATE SET
            last_refreshed_at = CURRENT_TIMESTAMP,
            rows_count = (SELECT COUNT(*) FROM user_activity),
            refresh_duration_ms = duration_ms;

        RAISE NOTICE '✅ user_activity materialized view refreshed successfully (rows: %, duration: %ms)', (SELECT COUNT(*) FROM user_activity), duration_ms;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh search_trends materialized view
CREATE OR REPLACE FUNCTION refresh_search_trends()
RETURNS void AS $$
BEGIN
    DECLARE
        start_time TIMESTAMP;
        duration_ms BIGINT;
    BEGIN
        start_time := CURRENT_TIMESTAMP;

        -- TRUNCATE existing data
        TRUNCATE TABLE search_trends;

        -- Refresh from analytics_events
        REFRESH MATERIALIZED VIEW CONCURRENTLY search_trends;

        -- Record refresh metadata
        duration_ms := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)) * 1000;
        INSERT INTO materialized_view_meta (view_name, last_refreshed_at, rows_count, refresh_duration_ms)
        VALUES ('search_trends', CURRENT_TIMESTAMP, (SELECT COUNT(*) FROM search_trends), duration_ms)
        ON CONFLICT (view_name) DO UPDATE SET
            last_refreshed_at = CURRENT_TIMESTAMP,
            rows_count = (SELECT COUNT(*) FROM search_trends),
            refresh_duration_ms = duration_ms;

        RAISE NOTICE '✅ search_trends materialized view refreshed successfully (rows: %, duration: %ms)', (SELECT COUNT(*) FROM search_trends), duration_ms;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to get all view metadata
CREATE OR REPLACE FUNCTION get_view_metadata()
RETURNS TABLE (
    view_name TEXT,
    last_refreshed_at TIMESTAMP,
    rows_count BIGINT,
    refresh_duration_ms BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        view_name,
        last_refreshed_at,
        rows_count,
        refresh_duration_ms
    FROM materialized_view_meta
    ORDER BY last_refreshed_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Verification query
SELECT
    'MATERIALIZED VIEW METADATA' AS test_query,
    view_name AS view,
    last_refreshed_at AS last_refreshed_at,
    rows_count AS rows_count,
    refresh_duration_ms AS refresh_duration_ms
FROM materialized_view_meta
ORDER BY last_refreshed_at DESC;

-- Expected results:
-- view_name: station_usage, user_activity, search_trends
-- rows_count: 0 initially, increases as events are ingested
-- refresh_duration_ms: 0 initially, increases after refresh
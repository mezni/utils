CREATE TABLE IF NOT EXISTS raw_events (
    id          BIGSERIAL PRIMARY KEY,
    batch_id    VARCHAR(21) NOT NULL,
    event_name  VARCHAR(50) NOT NULL,
    user_id     VARCHAR(255),
    session_id  VARCHAR(255) NOT NULL,
    payload     JSONB,
    client_ts   TIMESTAMPTZ NOT NULL,
    server_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip_address  VARCHAR(45)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_name ON raw_events(event_name);
CREATE INDEX IF NOT EXISTS idx_raw_events_server_ts   ON raw_events(server_ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_session_id  ON raw_events(session_id);

pub struct Config {
    pub database_url: String,
    pub bind_addr: String,
    pub rate_limit_burst: u32,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL_ANALYTICS")
                .unwrap_or_else(|_| "postgres://borne:borne@localhost:5432/analytics".into()),
            bind_addr: std::env::var("CLICKSTREAM_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8082".into()),
            rate_limit_burst: std::env::var("RATE_LIMIT_BURST_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
        }
    }
}

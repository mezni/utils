pub struct Config {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: std::env::var("PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3003),
            database_url: std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
            log_level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into()),
        }
    }
}

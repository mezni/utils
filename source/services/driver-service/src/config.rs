use std::env;

pub struct AppConfig {
    pub database_url: String,
    pub host: String,
    pub port: u16,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| {
                    "postgres://bornemap:bornemap@localhost:5432/bornemap_platform".to_string()
                }),
            host: env::var("DRIVER_SERVICE_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("DRIVER_SERVICE_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3001),
        }
    }
}

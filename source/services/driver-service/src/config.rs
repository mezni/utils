use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: String,
    pub database_url: String,
    pub db_pool_min: u32,
    pub db_pool_max: u32,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen_addr: env::var("LISTEN_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:3001".to_string()),
            database_url: env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
            db_pool_min: env::var("DB_POOL_MIN")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
            db_pool_max: env::var("DB_POOL_MAX")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        }
    }
}

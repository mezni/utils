use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
            host: env::var("ADMIN_SERVICE_HOST")
                .unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("ADMIN_SERVICE_PORT")
                .unwrap_or_else(|_| "3002".into())
                .parse()
                .expect("ADMIN_SERVICE_PORT must be a valid port number"),
        }
    }
}

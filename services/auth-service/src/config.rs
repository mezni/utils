use std::env;

pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_url: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());

        let port = env::var("PORT")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .expect("Invalid PORT");

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL required");

        Self {
            host,
            port,
            database_url,
        }
    }
}

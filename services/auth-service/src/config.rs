use std::env;

pub struct AppConfig {
    pub host: String,
    pub port: u16,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());

        let port = env::var("PORT")
            .unwrap_or_else(|_| "8081".into())
            .parse()
            .expect("Invalid PORT");

        Self { host, port }
    }
}

use actix_web::HttpRequest;
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
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("PORT")
                .unwrap_or_else(|_| "8081".to_string())
                .parse()
                .expect("PORT must be a valid u16"),
        }
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub fn x_partner_id(req: &HttpRequest) -> String {
    req.headers()
        .get("X-Partner-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("admin")
        .to_string()
}

use std::env;

pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub jwt_secret: String,
    pub jwt_expiration_seconds: i64,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
        let port = env::var("PORT")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .expect("Invalid PORT");
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
        let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET required");
        let jwt_expiration_seconds = env::var("JWT_EXPIRATION_SECONDS")
            .unwrap_or_else(|_| "86400".into())
            .parse()
            .expect("Invalid JWT_EXPIRATION_SECONDS");

        Self {
            host,
            port,
            database_url,
            jwt_secret,
            jwt_expiration_seconds,
        }
    }
}

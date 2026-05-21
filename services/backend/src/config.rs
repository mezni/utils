use std::env;

#[derive(Debug, Clone)]
pub struct Settings {
    pub database_url: String,
    pub mongo_url: String,
    pub rabbitmq_url: String,
    pub keycloak_url: String,
    pub bind_address: String,
    pub jwt_secret: String,
}

impl Settings {
    pub fn from_env() -> Result<Self, env::VarError> {
        dotenvy::dotenv().ok();

        Ok(Self {
            database_url: env::var("DATABASE_URL")?,
            mongo_url: env::var("MONGO_URL")?,
            rabbitmq_url: env::var("RABBITMQ_URL")?,
            keycloak_url: env::var("KEYCLOAK_URL").unwrap_or_else(|_| "http://keycloak:8080".into()),
            bind_address: env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8000".into()),
            jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "dev-secret-change-me".into()),
        })
    }
}

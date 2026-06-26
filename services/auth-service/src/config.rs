use std::env;

use bornemap_core::AppError;

pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub jwt_secret: String,
    pub jwt_access_ttl_seconds: i64,
    pub jwt_refresh_ttl_seconds: i64,
    pub jwt_issuer: String,
    pub jwt_audience: String,
}

#[derive(Clone)]
pub struct AuthServiceConfig {
    pub refresh_ttl_seconds: i64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, AppError> {
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
        let port = env::var("PORT")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid PORT".into()))?;
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| AppError::ConfigurationError("DATABASE_URL required".into()))?;
        let jwt_secret = env::var("JWT_SECRET")
            .map_err(|_| AppError::ConfigurationError("JWT_SECRET required".into()))?;

        let access_ttl_minutes: i64 = env::var("JWT_ACCESS_TTL_MINUTES")
            .unwrap_or_else(|_| "15".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid JWT_ACCESS_TTL_MINUTES".into()))?;

        let refresh_ttl_days: i64 = env::var("JWT_REFRESH_TTL_DAYS")
            .unwrap_or_else(|_| "7".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid JWT_REFRESH_TTL_DAYS".into()))?;

        let jwt_issuer = env::var("JWT_ISSUER").unwrap_or_else(|_| "bornemap".into());
        let jwt_audience = env::var("JWT_AUDIENCE").unwrap_or_else(|_| "bornemap-api".into());

        Ok(Self {
            host,
            port,
            database_url,
            jwt_secret,
            jwt_access_ttl_seconds: access_ttl_minutes * 60,
            jwt_refresh_ttl_seconds: refresh_ttl_days * 86400,
            jwt_issuer,
            jwt_audience,
        })
    }
}

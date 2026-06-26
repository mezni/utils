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
    // Redis configuration
    pub redis_url: String,
    pub rate_limit_requests: u32,
    pub rate_limit_window_seconds: u64,
    // OAuth configuration
    pub oauth_state_ttl_seconds: i64,
    // Google OAuth configuration
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    pub google_redirect_uri: Option<String>,
    pub google_auth_url: Option<String>,
    pub google_token_url: Option<String>,
    pub google_userinfo_url: Option<String>,
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

        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
        let rate_limit_requests: u32 = env::var("RATE_LIMIT_REQUESTS")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_REQUESTS".into()))?;
        let rate_limit_window_seconds: u64 = env::var("RATE_LIMIT_WINDOW_SECONDS")
            .unwrap_or_else(|_| "60".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid RATE_LIMIT_WINDOW_SECONDS".into()))?;
        let oauth_state_ttl_seconds: i64 = env::var("OAUTH_STATE_TTL")
            .unwrap_or_else(|_| "300".into())
            .parse()
            .map_err(|_| AppError::ConfigurationError("Invalid OAUTH_STATE_TTL".into()))?;

        let google_client_id = env::var("GOOGLE_CLIENT_ID").ok();
        let google_client_secret = env::var("GOOGLE_CLIENT_SECRET").ok();
        let google_redirect_uri = env::var("GOOGLE_REDIRECT_URI").ok();
        let google_auth_url = env::var("GOOGLE_AUTH_URL").unwrap_or_else(|_| "https://accounts.google.com/o/oauth2/v2/auth".into());
        let google_token_url = env::var("GOOGLE_TOKEN_URL").unwrap_or_else(|_| "https://oauth2.googleapis.com/token".into());
        let google_userinfo_url = env::var("GOOGLE_USERINFO_URL").unwrap_or_else(|_| "https://openidconnect.googleapis.com/v1/userinfo".into());

        Ok(Self {
            host,
            port,
            database_url,
            jwt_secret,
            jwt_access_ttl_seconds: access_ttl_minutes * 60,
            jwt_refresh_ttl_seconds: refresh_ttl_days * 86400,
            jwt_issuer,
            jwt_audience,
            redis_url,
            rate_limit_requests,
            rate_limit_window_seconds,
            oauth_state_ttl_seconds,
            google_client_id,
            google_client_secret,
            google_redirect_uri,
            google_auth_url,
            google_token_url,
            google_userinfo_url,
        })
    }
}

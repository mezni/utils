use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub server_port: u16,
    pub database_url: String,
    pub keycloak_url: String,
    pub keycloak_realm: String,
    pub keycloak_admin_username: String,
    pub keycloak_admin_password: String,
    pub jwks_uri: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub clock_skew_secs: i64,
    pub auth_service_client_id: String,
    pub auth_service_client_secret: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let keycloak_url = env::var("APP_KEYCLOAK_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let keycloak_realm = env::var("APP_KEYCLOAK_REALM")
            .unwrap_or_else(|_| "bornemap".to_string());

        Self {
            server_port: env::var("APP_SERVER_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000),
            database_url: env::var("APP_DATABASE_URL")
                .unwrap_or_else(|_| "postgres://bornemap_admin:bornemap_password@localhost:5432/platform_db".to_string()),
            keycloak_url: keycloak_url.clone(),
            keycloak_realm: keycloak_realm.clone(),
            keycloak_admin_username: env::var("APP_KEYCLOAK_ADMIN_USERNAME")
                .unwrap_or_else(|_| "admin".to_string()),
            keycloak_admin_password: env::var("APP_KEYCLOAK_ADMIN_PASSWORD")
                .unwrap_or_else(|_| "admin123".to_string()),
            jwks_uri: format!("{}/realms/{}/protocol/openid-connect/certs", keycloak_url, keycloak_realm),
            jwt_issuer: format!("{}/realms/{}", keycloak_url, keycloak_realm),
            jwt_audience: env::var("APP_JWT_AUDIENCE")
                .unwrap_or_else(|_| "auth-service-sa".to_string()),
            clock_skew_secs: env::var("APP_CLOCK_SKEW_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            auth_service_client_id: env::var("APP_AUTH_SERVICE_CLIENT_ID")
                .unwrap_or_else(|_| "auth-service-sa".to_string()),
            auth_service_client_secret: env::var("APP_AUTH_SERVICE_CLIENT_SECRET")
                .unwrap_or_else(|_| "".to_string()),
        }
    }
}

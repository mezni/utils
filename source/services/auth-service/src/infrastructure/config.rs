use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub service_port: u16,
    pub database_url: String,
    pub oidc: OidcConfig,
}

#[derive(Debug, Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    pub jwks_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        let service_port = env::var("SERVICE_PORT")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .expect("SERVICE_PORT must be a valid port number");

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        let keycloak_realm = env::var("KEYCLOAK_REALM").unwrap_or_else(|_| "bornemap".into());

        let keycloak_issuer = env::var("KEYCLOAK_ISSUER")
            .unwrap_or_else(|_| format!("http://localhost:8080/realms/{}", keycloak_realm));

        let client_id = env::var("KEYCLOAK_CLIENT_ID").unwrap_or_else(|_| "admin-dashboard".into());

        let client_secret =
            env::var("KEYCLOAK_CLIENT_SECRET").unwrap_or_else(|_| "pXFERGbIuDhSDGBL555DVlt1hRgIzt93".into());

        let redirect_uri = env::var("KEYCLOAK_REDIRECT_URI")
            .unwrap_or_else(|_| "http://localhost:5174/auth/callback".into());

        let jwks_url = env::var("KEYCLOAK_JWKS_URL")
            .unwrap_or_else(|_| format!("{}/protocol/openid-connect/certs", keycloak_issuer));

        Self {
            service_port,
            database_url,
            oidc: OidcConfig {
                issuer: keycloak_issuer,
                client_id,
                client_secret,
                redirect_uri,
                jwks_url,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_port() {
        temp_env::with_var("SERVICE_PORT", None, || {
            temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
                let config = Config::from_env();
                assert_eq!(config.service_port, 3000);
            });
        });
    }

    #[test]
    fn test_oidc_defaults() {
        temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
            temp_env::with_var("KEYCLOAK_REALM", Some("bornemap"), || {
                let config = Config::from_env();
                assert_eq!(config.oidc.issuer, "http://localhost:8080/realms/bornemap");
                assert_eq!(config.oidc.client_id, "admin-dashboard");
                assert_eq!(config.oidc.redirect_uri, "http://localhost:5174/auth/callback");
                assert!(config.oidc.jwks_url.contains("certs"));
            });
        });
    }

    #[test]
    fn test_custom_oidc_config() {
        temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
            temp_env::with_var("KEYCLOAK_CLIENT_ID", Some("web-driver"), || {
                temp_env::with_var("KEYCLOAK_REDIRECT_URI", Some("http://localhost:5173/cb"), || {
                    let config = Config::from_env();
                    assert_eq!(config.oidc.client_id, "web-driver");
                    assert_eq!(config.oidc.redirect_uri, "http://localhost:5173/cb");
                });
            });
        });
    }
}

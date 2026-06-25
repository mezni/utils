use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub service_port: u16,
    pub database_url: String,
    pub keycloak_realm: String,
    pub keycloak_issuer: String,
    pub keycloak_jwks_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        let service_port = env::var("SERVICE_PORT")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .expect("SERVICE_PORT must be a valid port number");

        let database_url = env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set");

        let keycloak_realm = env::var("KEYCLOAK_REALM")
            .unwrap_or_else(|_| "bornemap".into());

        let keycloak_base = env::var("KEYCLOAK_ISSUER")
            .unwrap_or_else(|_| format!("http://localhost:8080/realms/{}", keycloak_realm));

        let keycloak_jwks_url = env::var("KEYCLOAK_JWKS_URL")
            .unwrap_or_else(|_| format!("{}/protocol/openid-connect/certs", keycloak_base));

        Self {
            service_port,
            database_url,
            keycloak_realm,
            keycloak_issuer: keycloak_base,
            keycloak_jwks_url,
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
    fn test_config_custom_port() {
        temp_env::with_var("SERVICE_PORT", Some("4000"), || {
            temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
                let config = Config::from_env();
                assert_eq!(config.service_port, 4000);
            });
        });
    }

    #[test]
    fn test_config_default_issuer() {
        temp_env::with_var("KEYCLOAK_REALM", Some("test-realm"), || {
            temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
                let config = Config::from_env();
                assert_eq!(config.keycloak_issuer, "http://localhost:8080/realms/test-realm");
            });
        });
    }

    #[test]
    fn test_config_custom_issuer() {
        temp_env::with_var("KEYCLOAK_ISSUER", Some("https://auth.example.com/realms/myrealm"), || {
            temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
                let config = Config::from_env();
                assert_eq!(config.keycloak_issuer, "https://auth.example.com/realms/myrealm");
            });
        });
    }

    #[test]
    fn test_config_jwks_url() {
        temp_env::with_var("KEYCLOAK_ISSUER", Some("https://auth.example.com/realms/myrealm"), || {
            temp_env::with_var("DATABASE_URL", Some("postgres://localhost/test"), || {
                let config = Config::from_env();
                assert_eq!(config.keycloak_jwks_url, "https://auth.example.com/realms/myrealm/protocol/openid-connect/certs");
            });
        });
    }
}

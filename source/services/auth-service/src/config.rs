/// Configuration for the Auth Service.
///
/// This module loads configuration from environment variables.
pub struct Config {
    pub database_url: String,
    pub keycloak_url: String,
    pub port: u16,
}

impl Config {
    /// Get the database URL from environment variables.
    pub fn database_url() -> String {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL environment variable must be set")
    }

    /// Get the Keycloak URL from environment variables.
    pub fn keycloak_url() -> String {
        std::env::var("KEYCLOAK_URL")
            .expect("KEYCLOAK_URL environment variable must be set")
    }

    /// Get the server port from environment variables.
    pub fn port() -> u16 {
        std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3000)
    }

    /// Create a new Config instance.
    pub fn new() -> Self {
        Self {
            database_url: Self::database_url(),
            keycloak_url: Self::keycloak_url(),
            port: Self::port(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_database_url() {
        std::env::set_var("DATABASE_URL", "postgres://test");
        assert_eq!(Config::database_url(), "postgres://test");
    }

    #[test]
    fn test_config_keycloak_url() {
        std::env::set_var("KEYCLOAK_URL", "http://localhost:8080");
        assert_eq!(Config::keycloak_url(), "http://localhost:8080");
    }

    #[test]
    fn test_config_port_default() {
        std::env::remove_var("PORT");
        assert_eq!(Config::port(), 3000);
    }

    #[test]
    fn test_config_port_custom() {
        std::env::set_var("PORT", "8080");
        assert_eq!(Config::port(), 8080);
    }
}

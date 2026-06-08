// Configuration module
use serde::Deserialize;

/// PostgreSQL database connection URL
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresUrl {
    pub url: String,
}

impl PostgresUrl {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub fn validate(&self) -> Result<(), String> {
        // Check that URL is not empty
        if self.url.is_empty() {
            return Err("POSTGRES_URL environment variable is not set".to_string());
        }

        // Check that URL starts with postgresql:// or postgres://
        if !self.url.starts_with("postgresql://") && !self.url.starts_with("postgres://") {
            return Err(
                "POSTGRES_URL must start with 'postgresql://' or 'postgres://'".to_string()
            );
        }

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.url
    }
}

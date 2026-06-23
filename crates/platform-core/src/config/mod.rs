//! Configuration management for platform-core
//! Provides configuration structs for different parts of the application

use config::Config;
use serde::Deserialize;

/// Application configuration
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
}

/// Server configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
}

/// Database configuration
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub pool_size: u32,
    pub max_connections: u32,
}

/// Load configuration from environment variables and config files
pub fn load_config() -> Result<AppConfig, config::ConfigError> {
    let config = Config::builder()
        .add_source(config::Environment::with_prefix("BORNEMAP"))
        .add_source(config::File::with_name("config/default"))
        .build()?;

    config.try_deserialize()
}

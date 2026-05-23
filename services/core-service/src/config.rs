use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://bornemap:bornemap_dev@localhost:5432/bornemap".to_string()),
            max_connections: 20,
            min_connections: 10,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 3600,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct JwtConfig {
    pub secret: String,
    pub issuer: String,
    pub audience: String,
    pub expiration_hours: i64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: env::var("JWT_SECRET")
                .unwrap_or_else(|_| "default-secret-change-in-production".to_string()),
            issuer: "bornemap".to_string(),
            audience: "bornemap-api".to_string(),
            expiration_hours: 24,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: env::var("CORE_SERVICE_HOST")
                .unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("CORE_SERVICE_PORT")
                .unwrap_or_else(|_| "3001".to_string())
                .parse()
                .unwrap_or(3001),
            workers: env::var("CORE_SERVICE_WORKERS")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .unwrap_or(4),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RabbitMQConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub vhost: String,
    pub exchange: String,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            host: env::var("RABBITMQ_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("RABBITMQ_PORT")
                .unwrap_or_else(|_| "5672".to_string())
                .parse()
                .unwrap_or(5672),
            username: env::var("RABBITMQ_USER")
                .unwrap_or_else(|_| "guest".to_string()),
            password: env::var("RABBITMQ_PASSWORD")
                .unwrap_or_else(|_| "guest".to_string()),
            vhost: env::var("RABBITMQ_VHOST")
                .unwrap_or_else(|_| "/".to_string()),
            exchange: "bornemap.events".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: env::var("LOG_LEVEL")
                .unwrap_or_else(|_| "info".to_string()),
            format: "pretty".to_string(),
            json: env::var("LOG_JSON")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub jwt: JwtConfig,
    pub server: ServerConfig,
    pub rabbitmq: RabbitMQConfig,
    pub logging: LoggingConfig,
}

impl AppConfig {
    /// Load configuration from files and environment variables
    pub fn load() -> Result<Self, ConfigError> {
        let mut config = Config::builder()
            // Start with default configuration
            .add_source(File::with_name("config/default"))
            // Override with environment-specific configuration
            .add_source(File::with_name("config/local").required(false))
            // Override with environment variables
            .add_source(Environment::with_prefix("CORE").separator("__"))
            .build()?;

        config.try_deserialize()
    }

    /// Load configuration for a specific environment
    pub fn load_for_env(env: &str) -> Result<Self, ConfigError> {
        let mut config = Config::builder()
            // Start with default configuration
            .add_source(File::with_name("config/default"))
            // Override with environment-specific configuration
            .add_source(File::with_name(&format!("config/{}", env)).required(false))
            // Override with local configuration (for development)
            .add_source(File::with_name("config/local").required(false))
            // Override with environment variables
            .add_source(Environment::with_prefix("CORE").separator("__"))
            .build()?;

        config.try_deserialize()
    }

    /// Get the database URL
    pub fn database_url(&self) -> &str {
        &self.database.url
    }

    /// Get the JWT secret
    pub fn jwt_secret(&self) -> &str {
        &self.jwt.secret
    }

    /// Get the server address
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    /// Get the RabbitMQ connection string
    pub fn rabbitmq_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.rabbitmq.username,
            self.rabbitmq.password,
            self.rabbitmq.host,
            self.rabbitmq.port,
            self.rabbitmq.vhost
        )
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Check if JWT secret is not default
        if self.jwt.secret == "default-secret-change-in-production" {
            return Err("JWT secret must be changed from default value".to_string());
        }

        // Check if database URL is provided
        if self.database.url.is_empty() {
            return Err("Database URL is required".to_string());
        }

        // Check if server port is valid
        if self.server.port == 0 {
            return Err("Server port must be greater than 0".to_string());
        }

        // Check if logging level is valid
        let valid_levels = vec!["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.as_str()) {
            return Err(format!("Invalid logging level: {}", self.logging.level));
        }

        Ok(())
    }
}

/// Initialize configuration
pub fn init_config() -> Result<AppConfig, ConfigError> {
    // Determine the environment
    let env = env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string());
    
    // Load configuration
    let config = AppConfig::load_for_env(&env)?;
    
    // Validate configuration
    if let Err(err) = config.validate() {
        eprintln!("Configuration validation failed: {}", err);
        std::process::exit(1);
    }
    
    Ok(config)
}
#[allow(dead_code)]
pub struct Settings {
    pub server_host: String,
    pub server_port: u16,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
}

impl Settings {
    pub fn from_env() -> Self {
        Self {
            server_host: std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            server_port: std::env::var("SERVER_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            db_host: std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()),
            db_port: std::env::var("DB_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5432),
            db_user: std::env::var("DB_USER").unwrap_or_else(|_| "postgres".into()),
            db_password: std::env::var("DB_PASSWORD").unwrap_or_else(|_| "postgres".into()),
            db_name: std::env::var("DB_NAME").unwrap_or_else(|_| "platform_db".into()),
        }
    }
}

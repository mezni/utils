use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub name: String,
}

impl DatabaseConfig {
    pub fn url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.name
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database: DatabaseConfig,
    pub jwt_secret: Option<String>,
    pub max_db_connections: Option<u32>,
}

fn parse_database_url(url: &str) -> Option<(String, String, String, u16, String)> {
    let rest = url.strip_prefix("postgres://")?;
    let (user_pass, host_db) = rest.split_once('@')?;
    let (user, pass) = user_pass.split_once(':')?;
    let (host_port, db_name) = host_db.split_once('/')?;
    let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
        (h.to_string(), p.parse().ok()?)
    } else {
        (host_port.to_string(), 5432)
    };
    Some((user.to_string(), pass.to_string(), host, port, db_name.to_string()))
}

pub fn load_config(service_prefix: &str) -> Result<AppConfig, config::ConfigError> {
    dotenvy::dotenv().ok();

    let host_key = format!("{}_HOST", service_prefix.to_uppercase());
    let port_key = format!("{}_PORT", service_prefix.to_uppercase());

    let mut builder = config::Config::builder()
        .set_default("host", "127.0.0.1")?
        .set_default("port", "3000")?
        .set_default("database.host", "localhost")?
        .set_default("database.port", "5432")?
        .set_default("database.user", "bornemap")?
        .set_default("database.password", "bornemap_dev")?
        .set_default("database.name", "bornemap")?
        .set_default("max_db_connections", 10)?
        .add_source(config::Environment::default().prefix(service_prefix));

    if let Ok(v) = std::env::var(&host_key) {
        builder = builder.set_override("host", v)?;
    }
    if let Ok(v) = std::env::var(&port_key) {
        builder = builder.set_override("port", v)?;
    }

    if let Ok(db_url) = std::env::var("DATABASE_URL") {
        if let Some((user, pass, host, port, name)) = parse_database_url(&db_url) {
            builder = builder
                .set_override("database.user", user)?
                .set_override("database.password", pass)?
                .set_override("database.host", host)?
                .set_override("database.port", port as u64)?
                .set_override("database.name", name)?;
        }
    }

    let c: AppConfig = builder.build()?.try_deserialize()?;
    Ok(c)
}

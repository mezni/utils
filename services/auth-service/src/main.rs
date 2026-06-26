mod config;
mod http;

use actix_web::{App, HttpServer};
use config::AppConfig;
use tracing_subscriber::EnvFilter;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let config = AppConfig::from_env();

    println!("auth-service running on {}:{}", config.host, config.port);

    HttpServer::new(|| App::new().configure(http::configure))
        .bind((config.host, config.port))?
        .run()
        .await
}

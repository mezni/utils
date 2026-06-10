mod config;
mod db;
mod error;
mod models;
mod routes;

use actix_web::{App, HttpServer, web};
use config::Config;
use ev_db::{init_pool_with_config, PoolConfig};
use log::info;
use std::time::Duration;

pub struct AppState {
    pub pool: sqlx::PgPool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let config = Config::from_env();
    info!("Starting Driver Service on {}", config.bind_address());

    let pool = init_pool_with_config(PoolConfig {
        connection_string: config.database_url.clone(),
        max_connections: 10,
        connection_timeout: Duration::from_secs(30),
    })
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    let bind = config.bind_address();
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                pool: pool.clone(),
            }))
            .configure(routes::configure)
    })
    .bind(&bind)?
    .run()
    .await
}

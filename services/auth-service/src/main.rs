mod config;
mod db;
mod routes;

use actix_web::web::Data;
use actix_web::{App, HttpServer, middleware};
use config::Config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cfg = Config::from_env();

    std::env::set_var("RUST_LOG", &cfg.log_level);
    env_logger::init();

    let pool = db::init_pool(&cfg.database_url).await;

    log::info!("Starting auth-service on {}:{}", cfg.host, cfg.port);

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(pool.clone()))
            .wrap(middleware::Logger::default())
            .service(routes::health::health)
            .service(routes::ready::ready)
    })
    .bind(format!("{}:{}", cfg.host, cfg.port))?
    .run()
    .await
}

use actix_web::{middleware::Logger, HttpServer};
use sqlx::postgres::PgPoolOptions;

mod config;
mod db;
mod handlers;
mod middleware;
mod models;
mod routes;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("everest_backend=info".parse().unwrap()),
        )
        .init();

    let cfg = config::Settings::from_env().expect("Failed to load configuration");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.database_url)
        .await
        .expect("Failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    tracing::info!("Database connected and migrations applied");

    let mongo_client = db::mongo::connect(&cfg.mongo_url)
        .await
        .expect("Failed to connect to MongoDB");

    let rabbit_conn = db::rabbit::connect(&cfg.rabbitmq_url)
        .await
        .expect("Failed to connect to RabbitMQ");

    tracing::info!("Starting Everest backend on {}", cfg.bind_address);

    HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(middleware::security::security_headers())
            .configure(routes::public::configure)
            .configure(routes::admin::configure)
            .app_data(actix_web::web::Data::new(pool.clone()))
            .app_data(actix_web::web::Data::new(mongo_client.clone()))
            .app_data(actix_web::web::Data::new(rabbit_conn.clone()))
    })
    .bind(&cfg.bind_address)?
    .run()
    .await
}

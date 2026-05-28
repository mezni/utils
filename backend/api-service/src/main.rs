use actix_web::{web, App, HttpServer, middleware::Logger};
use actix_cors::Cors;
use sqlx::PgPool;

mod domains;

pub struct AppState {
    pub db: PgPool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://borne:borne@localhost:5432/borne_map".to_string());

    let pool = infra::join_database_pool(&database_url)
        .await
        .expect("Failed to connect to database");

    let state = web::Data::new(AppState { db: pool });

    let host = "0.0.0.0";
    let port = 8080;
    log::info!("api-service online on http://{}:{}", host, port);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(state.clone())
            .wrap(Logger::default())
            .route("/health", web::get().to(domains::locate::routes::health))
            .service(
                web::scope("/api/v1")
                    .configure(domains::locate::init_routes),
            )
    })
    .bind((host, port))?
    .run()
    .await
}

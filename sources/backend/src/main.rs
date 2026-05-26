use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

mod auth;
mod domain;
mod utils;

pub struct AppState {
    pub db: sqlx::PgPool,
}

#[get("/api/v1/health")]
async fn health(state: web::Data<AppState>) -> impl Responder {
    let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.db)
        .await
        .is_ok();

    if db_ok {
        HttpResponse::Ok().json(serde_json::json!({
            "status": "ok",
            "service": "bornemap-backend",
            "database": "connected"
        }))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "error",
            "service": "bornemap-backend",
            "database": "disconnected"
        }))
    }
}

fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(health)
        .route("/api/v1/auth/register", web::post().to(auth::handlers::register))
        .route("/api/v1/auth/login", web::post().to(auth::handlers::login))
        .route("/api/v1/users", web::post().to(domain::users::handlers::create_user))
        .route("/api/v1/users", web::get().to(domain::users::handlers::list_users))
        .route("/api/v1/users/{id}", web::get().to(domain::users::handlers::get_user))
        .route("/api/v1/users/{id}", web::patch().to(domain::users::handlers::update_user))
        .route("/api/v1/users/{id}", web::delete().to(domain::users::handlers::delete_user))
        .route("/api/v1/partners", web::post().to(domain::partners::handlers::create_partner))
        .route("/api/v1/partners", web::get().to(domain::partners::handlers::list_partners))
        .route("/api/v1/partners/{id}", web::get().to(domain::partners::handlers::get_partner))
        .route("/api/v1/partners/{id}", web::patch().to(domain::partners::handlers::update_partner))
        .route("/api/v1/partners/{id}", web::delete().to(domain::partners::handlers::delete_partner))
        .route("/api/v1/stations/nearby", web::get().to(domain::infrastructure::nearby_stations))
        .route("/api/v1/stations", web::post().to(domain::stations::handlers::create_station))
        .route("/api/v1/stations", web::get().to(domain::stations::handlers::list_stations))
        .route("/api/v1/stations/{id}", web::get().to(domain::stations::handlers::get_station))
        .route("/api/v1/stations/{id}", web::patch().to(domain::stations::handlers::update_station))
        .route("/api/v1/stations/{id}", web::delete().to(domain::stations::handlers::delete_station))
        .route("/api/v1/stations/{station_id}/chargers", web::post().to(domain::chargers::handlers::create_charger))
        .route("/api/v1/stations/{station_id}/chargers", web::get().to(domain::chargers::handlers::list_chargers_for_station))
        .route("/api/v1/stations/{station_id}/chargers/{id}", web::get().to(domain::chargers::handlers::get_charger))
        .route("/api/v1/stations/{station_id}/chargers/{id}", web::patch().to(domain::chargers::handlers::update_charger))
        .route("/api/v1/stations/{station_id}/chargers/{id}", web::delete().to(domain::chargers::handlers::delete_charger))
        .route("/api/v1/connector-types", web::post().to(domain::connector_types::handlers::create_connector_type))
        .route("/api/v1/connector-types", web::get().to(domain::connector_types::handlers::list_connector_types))
        .route("/api/v1/connector-types/{id}", web::get().to(domain::connector_types::handlers::get_connector_type))
        .route("/api/v1/connector-types/{id}", web::patch().to(domain::connector_types::handlers::update_connector_type))
        .route("/api/v1/connector-types/{id}", web::delete().to(domain::connector_types::handlers::delete_connector_type));
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    tracing::info!("Starting BorneMap backend on :8080");

    let app_state = web::Data::new(AppState { db });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes)
    })
        .bind("0.0.0.0:8080")?
        .run()
        .await
}

mod application;
mod db;
mod presentation;

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{web, App, HttpResponse, HttpServer, middleware::Logger};

use application::charger_service::ChargerService;
use application::dashboard_service::DashboardService;
use application::partner_service::PartnerService;
use application::station_service::StationService;

use bornemap_platform_db::repositories::charger_repo::PgChargerRepository;
use bornemap_platform_db::repositories::dashboard_repo::PgDashboardRepository;
use bornemap_platform_db::repositories::partner_repo::PgPartnerRepository;
use bornemap_platform_db::repositories::station_repo::PgStationRepository;

use presentation::{chargers, dashboard, partners, stations};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let pool = db::init_pool().await;

    let partner_repo = Arc::new(PgPartnerRepository::new(pool.clone()));
    let station_repo = Arc::new(PgStationRepository::new(pool.clone()));
    let charger_repo = Arc::new(PgChargerRepository::new(pool.clone()));
    let dashboard_repo = Arc::new(PgDashboardRepository::new(pool.clone()));

    let partner_svc = Arc::new(PartnerService::new(partner_repo.clone()));
    let station_svc = Arc::new(StationService::new(station_repo.clone(), partner_repo.clone()));
    let charger_svc = Arc::new(ChargerService::new(charger_repo.clone(), station_repo.clone()));
    let dashboard_svc = Arc::new(DashboardService::new(dashboard_repo.clone()));

    println!("admin-service starting on 0.0.0.0:8080");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .wrap(Logger::default())
            .app_data(web::Data::new(partner_svc.clone()))
            .app_data(web::Data::new(station_svc.clone()))
            .app_data(web::Data::new(charger_svc.clone()))
            .app_data(web::Data::new(dashboard_svc.clone()))
            // Dashboard
            .route("/api/v1/dashboard/kpis", web::get().to(dashboard::get_kpis))
            // Partners
            .route("/api/v1/partners", web::get().to(partners::list))
            .route("/api/v1/partners", web::post().to(partners::create))
            .route("/api/v1/partners", web::delete().to(partners::hard_delete))
            .route("/api/v1/partners/{id}", web::get().to(partners::get))
            .route("/api/v1/partners/{id}", web::put().to(partners::soft_delete))
            .route("/api/v1/partners/{id}", web::patch().to(partners::update))
            // Stations
            .route("/api/v1/stations", web::get().to(stations::list))
            .route("/api/v1/stations", web::post().to(stations::create))
            .route("/api/v1/stations", web::delete().to(stations::hard_delete))
            .route("/api/v1/stations/{id}", web::get().to(stations::get))
            .route("/api/v1/stations/{id}", web::put().to(stations::soft_delete))
            .route("/api/v1/stations/{id}", web::patch().to(stations::update))
            // Chargers
            .route("/api/v1/chargers", web::get().to(chargers::list))
            .route("/api/v1/chargers", web::post().to(chargers::create))
            .route("/api/v1/chargers", web::delete().to(chargers::hard_delete))
            .route("/api/v1/chargers/{id}", web::get().to(chargers::get))
            .route("/api/v1/chargers/{id}", web::put().to(chargers::update))
            .route("/api/v1/chargers/{id}", web::patch().to(chargers::patch))
            // Health
            .route("/api/v1/health", web::get().to(|| async {
                HttpResponse::Ok().json(serde_json::json!({"status": "ok", "service": "admin-service"}))
            }))
            .default_service(web::route().to(|| async {
                HttpResponse::NotFound().json(serde_json::json!({
                    "success": false, "data": null,
                    "error": {"code": "NOT_FOUND", "message": "route not found"}
                }))
            }))
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

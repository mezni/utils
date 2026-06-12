use actix_web::web;

mod health;
mod stations;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/stations", web::get().to(stations::list_stations))
            .route("/stations/nearby", web::get().to(stations::nearby_stations))
            .route("/stations/{id}", web::get().to(stations::get_station)),
    )
    .route("/health", web::get().to(health::health_check));
}

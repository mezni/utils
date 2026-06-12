use actix_web::web;

mod health;
mod stations;
mod events;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/stations", web::post().to(stations::create_station))
            .route("/stations/{id}", web::put().to(stations::update_station))
            .route("/stations/{id}", web::delete().to(stations::delete_station))
            .route("/events", web::post().to(events::ingest_event))
            .route("/events/batch", web::post().to(events::ingest_batch)),
    )
    .route("/health", web::get().to(health::health_check));
}

use actix_web::web;

use crate::handlers::{nearby_handler, station_handler};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("", web::get().to(station_handler::list_stations))
        .route("/nearby", web::get().to(nearby_handler::find_nearby))
        .route("/{id}", web::get().to(station_handler::get_station_detail));
}

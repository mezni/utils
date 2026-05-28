use actix_web::web;

pub mod model;
pub mod routes;

pub fn init_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(routes::get_nearby_stations);
}

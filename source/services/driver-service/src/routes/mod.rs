pub mod health;
pub mod nearby;
pub mod detail;
pub mod search;
pub mod markers;
pub mod reviews;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health::health)
        .service(nearby::nearby)
        .service(search::search)
        .service(detail::detail)
        .service(markers::markers)
        .service(reviews::reviews);
}

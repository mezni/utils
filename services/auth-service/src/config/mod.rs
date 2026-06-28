pub mod routes {
    use actix_web::web;

    use crate::presentation::http::health;

    pub fn configure(cfg: &mut web::ServiceConfig) {
        cfg.service(health::health_check);
    }
}

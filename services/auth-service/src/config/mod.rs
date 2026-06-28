pub mod routes {
    use actix_web::web;

    use crate::presentation::http::auth;
    use crate::presentation::http::health;

    pub fn configure(cfg: &mut web::ServiceConfig) {
        cfg.service(health::health_check)
            .service(
                web::scope("/auth")
                    .route("/register", web::post().to(auth::register))
                    .route("/login", web::post().to(auth::login)),
            );
    }
}

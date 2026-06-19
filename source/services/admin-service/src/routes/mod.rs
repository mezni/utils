pub mod partner;
pub mod station;
pub mod charger;

pub fn config(cfg: &mut web::ServiceConfig) {
    // Partner routes
    cfg.service(web::resource("/admin/partner")
        .route(web::post().to(partner::create_partner))
        .route(web::get().to(partner::get_partner)));

    // Station routes
    cfg.service(web::resource("/admin/station")
        .route(web::post().to(station::create_station)));

    // Charger routes
    cfg.service(web::resource("/admin/charger")
        .route(web::post().to(charger::create_charger)));
}

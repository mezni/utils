pub mod availability;
pub mod chargers;
pub mod health;
pub mod partners;
pub mod stations;

use actix_web::web;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(health::health)
        .service(partners::create_partner)
        .service(partners::list_partners)
        .service(partners::get_partner)
        .service(partners::update_partner)
        .service(partners::delete_partner)
        .service(stations::create_station)
        .service(stations::list_stations)
        .service(stations::get_station)
        .service(stations::update_station)
        .service(stations::delete_station)
        .service(chargers::create_charger)
        .service(chargers::list_chargers)
        .service(chargers::get_charger)
        .service(chargers::update_charger)
        .service(chargers::delete_charger)
        .service(availability::create_availability);
}

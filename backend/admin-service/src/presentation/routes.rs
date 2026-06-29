use actix_web::web;

use crate::domain::repositories::connector_repo::ConnectorRepository;
use crate::domain::repositories::partner_repo::PartnerRepository;
use crate::domain::repositories::station_repo::StationRepository;
use crate::infrastructure::repositories::connector_repo::PostgresConnectorRepository;
use crate::infrastructure::repositories::partner_repo::PostgresPartnerRepository;
use crate::infrastructure::repositories::station_repo::PostgresStationRepository;
use crate::presentation::handlers::connectors;
use crate::presentation::handlers::partners;
use crate::presentation::handlers::stations;

pub fn configure_routes<P, S, C>(cfg: &mut web::ServiceConfig)
where
    P: PartnerRepository + Clone + 'static,
    S: StationRepository + Clone + 'static,
    C: ConnectorRepository + Clone + 'static,
{
    cfg.service(
        web::scope("/api/v1")
            .service(
                web::scope("/partners")
                    .route("", web::post().to(partners::create_partner::<P>))
                    .route("", web::get().to(partners::list_partners::<P>)),
            )
            .service(
                web::scope("/stations")
                    .route("", web::post().to(stations::create_station::<S, P>))
                    .route("", web::get().to(stations::list_stations::<S>))
                    .route("/{id}", web::put().to(stations::update_station::<S>))
                    .route("/{id}", web::delete().to(stations::delete_station::<S>)),
            )
            .service(
                web::scope("/connectors")
                    .route("", web::post().to(connectors::create_connector::<C, S>))
                    .route("", web::get().to(connectors::list_connectors::<C>))
                    .route("/{id}", web::delete().to(connectors::delete_connector::<C>)),
            ),
    );
}

pub fn configure_routes_typed(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/partners", web::post().to(partners::create_partner::<PostgresPartnerRepository>))
            .route("/partners", web::get().to(partners::list_partners::<PostgresPartnerRepository>))
            .route("/stations", web::post().to(stations::create_station::<PostgresStationRepository, PostgresPartnerRepository>))
            .route("/stations", web::get().to(stations::list_stations::<PostgresStationRepository>))
            .route("/stations/{id}", web::put().to(stations::update_station::<PostgresStationRepository>))
            .route("/stations/{id}", web::delete().to(stations::delete_station::<PostgresStationRepository>))
            .route("/connectors", web::post().to(connectors::create_connector::<PostgresConnectorRepository, PostgresStationRepository>))
            .route("/connectors", web::get().to(connectors::list_connectors::<PostgresConnectorRepository>))
            .route("/connectors/{id}", web::delete().to(connectors::delete_connector::<PostgresConnectorRepository>)),
    );
}

use actix_web::{get, web, HttpResponse, Responder};
use chrono::Utc;
use domain::{Charger, StationHub};
use crate::AppState;

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, web, App};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use crate::AppState;

    #[actix_web::test]
    async fn test_nearby_stations_returns_non_empty() {
        let state = web::Data::new(AppState {
            stations: Arc::new(RwLock::new(generate_mock_data())),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(web::scope("/api/v1").service(get_nearby_stations))
        ).await;
        let req = test::TestRequest::get().uri("/api/v1/stations/nearby").to_request();
        let resp: Vec<StationHub> = test::call_and_read_body_json(&app, req).await;
        assert!(!resp.is_empty(), "Response must contain at least one station");
    }

    #[actix_web::test]
    async fn test_station_id_matches_nanouuid_pattern() {
        let state = web::Data::new(AppState {
            stations: Arc::new(RwLock::new(generate_mock_data())),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(web::scope("/api/v1").service(get_nearby_stations))
        ).await;
        let req = test::TestRequest::get().uri("/api/v1/stations/nearby").to_request();
        let resp: Vec<StationHub> = test::call_and_read_body_json(&app, req).await;
        for station in &resp {
            assert!(
                station.id.starts_with("stn-"),
                "Station ID must start with stn-: {}",
                station.id
            );
            assert_eq!(station.id.len(), 12, "Station ID must be 12 chars: {}", station.id);
        }
    }

    #[actix_web::test]
    async fn test_chargers_have_valid_fields() {
        let state = web::Data::new(AppState {
            stations: Arc::new(RwLock::new(generate_mock_data())),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(web::scope("/api/v1").service(get_nearby_stations))
        ).await;
        let req = test::TestRequest::get().uri("/api/v1/stations/nearby").to_request();
        let resp: Vec<StationHub> = test::call_and_read_body_json(&app, req).await;
        for station in &resp {
            assert!(!station.chargers.is_empty(), "Station {} must have at least one charger", station.id);
            for charger in &station.chargers {
                assert!(!charger.id.is_empty(), "Charger ID must not be empty");
                assert!(!charger.plug_type.is_empty(), "Charger plug_type must not be empty");
                assert!(charger.power_output > 0, "Charger power_output must be positive");
                assert!(charger.status == "Available" || charger.status == "Occupied",
                    "Charger status must be Available or Occupied, got: {}", charger.status);
            }
        }
    }
}

pub fn generate_mock_data() -> Vec<StationHub> {
    vec![
        StationHub {
            id: "stn-e3b0c442".to_string(),
            name: "LES BERGES DU LAC 2 HUB".to_string(),
            provider_id: "prv-k9x2m47a".to_string(),
            provider_name: "TotalEnergies Tunisia".to_string(),
            latitude: 36.8324,
            longitude: 10.2321,
            status: "Available".to_string(),
            chargers: vec![
                Charger {
                    id: "chg-7b2a19f4".to_string(),
                    plug_type: "CCS2".to_string(),
                    power_output: 120,
                    status: "Available".to_string(),
                },
            ],
            updated_at: Utc::now(),
        },
        StationHub {
            id: "stn-f4a1d553".to_string(),
            name: "TUNIS MARINE PLAZA".to_string(),
            provider_id: "prv-m1n8b52c".to_string(),
            provider_name: "Ola Energy".to_string(),
            latitude: 36.8010,
            longitude: 10.1912,
            status: "Occupied".to_string(),
            chargers: vec![
                Charger {
                    id: "chg-3a1b2c3d".to_string(),
                    plug_type: "CCS2".to_string(),
                    power_output: 120,
                    status: "Occupied".to_string(),
                },
            ],
            updated_at: Utc::now(),
        },
    ]
}

#[get("/stations/nearby")]
pub async fn get_nearby_stations(state: web::Data<AppState>) -> impl Responder {
    let data = state.stations.read();
    HttpResponse::Ok().json(&*data)
}

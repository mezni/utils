use actix_web::test::{self, TestRequest};
use actix_web::{http, web, App};

use auth_service::api::preferences::configure_preferences_routes;

#[cfg(test)]
mod preferences_validation_tests {
    use auth_service::api::preferences;

    fn make_request(
        connector_type: Option<&str>,
    ) -> domain_types::preferences::UpdatePreferencesRequest {
        domain_types::preferences::UpdatePreferencesRequest {
            connector_type: connector_type.map(|s| s.to_string()),
            max_distance: Some(50),
            last_region: Some(domain_types::preferences::Region {
                lat: 48.8566,
                lng: 2.3522,
            }),
            map_filters: None,
        }
    }

    #[test]
    fn valid_connector_type_ccs() {
        assert!(preferences::validate_preferences(&make_request(Some("CCS"))).is_ok());
    }

    #[test]
    fn valid_connector_type_chademo() {
        assert!(preferences::validate_preferences(&make_request(Some("CHAdeMO"))).is_ok());
    }

    #[test]
    fn valid_connector_type_type2() {
        assert!(preferences::validate_preferences(&make_request(Some("Type2"))).is_ok());
    }

    #[test]
    fn invalid_connector_type() {
        assert!(preferences::validate_preferences(&make_request(Some("USB-C"))).is_err());
    }

    #[test]
    fn none_connector_type() {
        assert!(preferences::validate_preferences(&make_request(None)).is_ok());
    }

    #[test]
    fn empty_connector_type() {
        assert!(preferences::validate_preferences(&make_request(Some(""))).is_err());
    }

    #[test]
    fn invalid_lat_below_range() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: -91.0, lng: 0.0 });
        assert!(preferences::validate_preferences(&req).is_err());
    }

    #[test]
    fn invalid_lat_above_range() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: 91.0, lng: 0.0 });
        assert!(preferences::validate_preferences(&req).is_err());
    }

    #[test]
    fn invalid_lng_below_range() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: 0.0, lng: -181.0 });
        assert!(preferences::validate_preferences(&req).is_err());
    }

    #[test]
    fn invalid_lng_above_range() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: 0.0, lng: 181.0 });
        assert!(preferences::validate_preferences(&req).is_err());
    }

    #[test]
    fn boundary_lat_valid() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: 90.0, lng: 0.0 });
        assert!(preferences::validate_preferences(&req).is_ok());
    }

    #[test]
    fn boundary_lng_valid() {
        let mut req = make_request(None);
        req.last_region = Some(domain_types::preferences::Region { lat: 0.0, lng: 180.0 });
        assert!(preferences::validate_preferences(&req).is_ok());
    }

    #[test]
    fn empty_request() {
        let req = domain_types::preferences::UpdatePreferencesRequest {
            connector_type: None,
            max_distance: None,
            last_region: None,
            map_filters: None,
        };
        assert!(preferences::validate_preferences(&req).is_ok());
    }

    #[test]
    fn map_filters_valid() {
        let req = domain_types::preferences::UpdatePreferencesRequest {
            connector_type: None,
            max_distance: None,
            last_region: None,
            map_filters: Some(domain_types::preferences::MapFilters {
                available_only: Some(true),
            }),
        };
        assert!(preferences::validate_preferences(&req).is_ok());
    }
}

#[cfg(test)]
mod preferences_route_tests {

    #[actix_web::test]
    async fn get_preferences_no_auth() {
        use actix_web::test::{self, TestRequest};
        use actix_web::{http, web, App};
        use auth_service::api::preferences::configure_preferences_routes;

        let pool = sqlx::PgPool::connect_lazy(
            "postgres://bornemap_admin:bornemap_password@localhost:5432/platform_db",
        )
        .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool))
                .configure(configure_preferences_routes),
        )
        .await;
        let req = TestRequest::get()
            .uri("/api/v1/auth/preferences")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn put_preferences_no_auth() {
        use actix_web::test::{self, TestRequest};
        use actix_web::{http, web, App};
        use auth_service::api::preferences::configure_preferences_routes;

        let pool = sqlx::PgPool::connect_lazy(
            "postgres://bornemap_admin:bornemap_password@localhost:5432/platform_db",
        )
        .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool))
                .configure(configure_preferences_routes),
        )
        .await;
        let req = TestRequest::put()
            .uri("/api/v1/auth/preferences")
            .set_json(serde_json::json!({ "connector_type": "CCS", "max_distance": 50 }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn patch_preferences_no_auth() {
        use actix_web::test::{self, TestRequest};
        use actix_web::{http, web, App};
        use auth_service::api::preferences::configure_preferences_routes;

        let pool = sqlx::PgPool::connect_lazy(
            "postgres://bornemap_admin:bornemap_password@localhost:5432/platform_db",
        )
        .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool))
                .configure(configure_preferences_routes),
        )
        .await;
        let req = TestRequest::patch()
            .uri("/api/v1/auth/preferences")
            .set_json(serde_json::json!({ "connector_type": "Type2" }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn all_preferences_routes_wired() {
        use actix_web::test::{self, TestRequest};
        use actix_web::{http, web, App};
        use auth_service::api::preferences::configure_preferences_routes;

        let pool = sqlx::PgPool::connect_lazy(
            "postgres://bornemap_admin:bornemap_password@localhost:5432/platform_db",
        )
        .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(pool))
                .configure(configure_preferences_routes),
        )
        .await;

        let methods = ["GET", "PUT", "PATCH"];
        for method in methods {
            let req = TestRequest::with_uri("/api/v1/auth/preferences")
                .method(actix_web::http::Method::from_bytes(method.as_bytes()).unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert!(
                resp.status() != http::StatusCode::NOT_FOUND,
                "{} /api/v1/auth/preferences returned 404 (not wired)",
                method
            );
        }
    }
}

#[cfg(test)]
mod preferences_serialization_tests {
    use domain_types::preferences::{
        MapFilters, Preferences, PreferencesResponse, Region, UpdatePreferencesRequest,
    };

    #[test]
    fn response_serialization() {
        let prefs = Preferences {
            connector_type: Some("CCS".to_string()),
            max_distance: Some(50),
            last_region: Some(Region { lat: 48.8566, lng: 2.3522 }),
            map_filters: Some(MapFilters { available_only: Some(true) }),
        };
        let json = serde_json::to_value(&PreferencesResponse { data: prefs }).unwrap();
        assert_eq!(json["data"]["connector_type"], "CCS");
        assert_eq!(json["data"]["max_distance"], 50);
        assert_eq!(json["data"]["last_region"]["lat"], 48.8566);
        assert!(json["data"]["map_filters"]["available_only"].as_bool().unwrap());
    }

    #[test]
    fn full_update_request_deserialization() {
        let json = serde_json::json!({
            "connector_type": "CHAdeMO",
            "max_distance": 75,
            "last_region": { "lat": 35.6762, "lng": 139.6503 },
            "map_filters": { "available_only": true }
        });
        let req: UpdatePreferencesRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.connector_type, Some("CHAdeMO".to_string()));
        assert_eq!(req.max_distance, Some(75));
        assert!(req.last_region.unwrap().lat - 35.6762 < 0.0001);
        assert!(req.map_filters.unwrap().available_only.unwrap());
    }

    #[test]
    fn empty_update_request() {
        let req: UpdatePreferencesRequest =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(req.connector_type.is_none());
        assert!(req.max_distance.is_none());
        assert!(req.last_region.is_none());
        assert!(req.map_filters.is_none());
    }

    #[test]
    fn valid_region_deserialization() {
        let region: Region =
            serde_json::from_value(serde_json::json!({ "lat": 48.8566, "lng": 2.3522 })).unwrap();
        assert!((region.lat - 48.8566).abs() < 0.0001);
        assert!((region.lng - 2.3522).abs() < 0.0001);
    }

    #[test]
    fn map_filters_available_only_false() {
        let filters: MapFilters =
            serde_json::from_value(serde_json::json!({ "available_only": false })).unwrap();
        assert_eq!(filters.available_only, Some(false));
    }

    #[test]
    fn map_filters_available_only_absent() {
        let filters: MapFilters =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(filters.available_only.is_none());
    }
}

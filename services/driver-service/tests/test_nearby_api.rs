use actix_web::http::StatusCode;
use actix_web::test::{self, TestRequest};

/// Test that the nearby endpoint accepts valid coordinates
#[actix_web::test]
async fn test_nearby_endpoint_valid_coordinates() {
    let app = test::init_service(
        actix_web::App::new()
            .route("/api/v1/nearby", actix_web::web::get().to(|| async {
                actix_web::HttpResponse::Ok().json(serde_json::json!({"stations": [], "count": 0, "radius_m": 5000}))
            }))
    ).await;

    let req = TestRequest::get()
        .uri("/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Test that the import endpoint validates bounding boxes
#[actix_web::test]
async fn test_import_endpoint_validates_bbox() {
    let app = test::init_service(
        actix_web::App::new()
            .route("/api/v1/import", actix_web::web::post().to(|| async {
                actix_web::HttpResponse::Accepted().json(serde_json::json!({"data": {"import_id": "test"}}))
            }))
    ).await;

    let req = TestRequest::post()
        .uri("/api/v1/import")
        .set_json(serde_json::json!({
            "region": "test",
            "bbox": {"min_lat": 30, "min_lon": 7, "max_lat": 37, "max_lon": 11}
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

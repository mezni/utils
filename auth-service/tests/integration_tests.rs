use axum::extract::DefaultJson;
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn test_root_endpoint() {
    let app = axum::Router::new().route("/", axum::routing::get(|_| "Auth Service"));

    let client = hyper::Client::new();
    let response = client
        .request(
            hyper::Request::builder()
                .uri("http://localhost:8080/")
                .body(hyper::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_response_format() {
    let response_json = json!({
        "success": false,
        "error": {
            "code": "TEST_ERROR",
            "message": "Test error message"
        }
    });

    let response = axum::Json(response_json).into_response();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_error_response_serialization() {
    let error_response = json!({
        "success": false,
        "error": {
            "code": "VALIDATION_ERROR",
            "message": "Invalid input data",
            "details": ["email", "password"]
        }
    });

    let response = axum::Json(error_response).into_response();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
}

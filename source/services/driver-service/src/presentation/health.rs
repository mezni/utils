use axum::Json;
use serde_json::{json, Value};

pub async fn health_handler() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "driver-service",
        "version": "1.0.0"
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn test_health_response() {
        let response = health_handler().await.into_response();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_health_body() {
        let json = health_handler().await;
        assert_eq!(json.0["status"], "ok");
        assert_eq!(json.0["service"], "driver-service");
        assert_eq!(json.0["version"], "1.0.0");
    }
}

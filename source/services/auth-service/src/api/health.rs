use axum::{extract::State, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::state::AppState;

pub async fn health_handler() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "auth-service",
        "version": "1.0.0"
    }))
}

pub async fn live_handler() -> Json<Value> {
    Json(json!({ "alive": true }))
}

pub async fn ready_handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    let db_ok = sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .is_ok();

    Json(json!({
        "ready": db_ok
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
        assert_eq!(json.0["service"], "auth-service");
        assert_eq!(json.0["version"], "1.0.0");
    }

    #[tokio::test]
    async fn test_live_response() {
        let json = live_handler().await;
        assert_eq!(json.0["alive"], true);
    }
}

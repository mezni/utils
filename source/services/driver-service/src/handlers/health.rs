use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

pub async fn health_check(State(state): State<AppState>) -> Json<Value> {
    let healthy = sqlx::query("SELECT 1")
        .execute(&state.db)
        .await
        .is_ok();

    if healthy {
        Json(json!({
            "status": "ok",
            "database": "connected"
        }))
    } else {
        Json(json!({
            "status": "error",
            "database": "disconnected"
        }))
    }
}

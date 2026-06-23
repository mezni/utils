//! Health check endpoints

use actix_web::{web, HttpResponse, Result};

/// Health check endpoint
pub async fn health_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "admin-service",
        "version": "1.0.0",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Ready check endpoint
pub async fn ready_check() -> Result<HttpResponse> {
    // Add database connection check here
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "ready",
        "service": "admin-service"
    })))
}

/// Liveness check endpoint
pub async fn live_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "live",
        "service": "admin-service"
    })))
}
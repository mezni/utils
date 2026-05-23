use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use crate::utils::database::Database;
use crate::services::{CompanyService, StationService, ChargerService};

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub database: String,
    pub services: ServiceHealth,
    pub details: HealthDetails,
}

#[derive(Debug, Serialize)]
pub struct ServiceHealth {
    pub company_service: String,
    pub station_service: String,
    pub charger_service: String,
}

#[derive(Debug, Serialize)]
pub struct HealthDetails {
    pub database: DatabaseHealth,
    pub services: ServiceHealthDetails,
}

#[derive(Debug, Serialize)]
pub struct DatabaseHealth {
    pub status: String,
    pub response_time_ms: u64,
    pub pool_size: u32,
    pub idle_connections: u32,
    pub active_connections: u32,
}

#[derive(Debug, Serialize)]
pub struct ServiceHealthDetails {
    pub company_service: ServiceHealthDetail,
    pub station_service: ServiceHealthDetail,
    pub charger_service: ServiceHealthDetail,
}

#[derive(Debug, Serialize)]
pub struct ServiceHealthDetail {
    pub status: String,
    pub response_time_ms: u64,
    pub error: Option<String>,
}

// Application startup time (set in main.rs)
static mut APP_START_TIME: Option<Instant> = None;

pub fn set_app_start_time() {
    unsafe {
        APP_START_TIME = Some(Instant::now());
    }
}

fn get_app_uptime() -> u64 {
    unsafe {
        APP_START_TIME
            .map(|start| start.elapsed().as_secs())
            .unwrap_or(0)
    }
}

/// Health check endpoint
pub async fn health_check(
    db: web::Data<Database>,
    company_service: web::Data<std::sync::Arc<CompanyService>>,
    station_service: web::Data<std::sync::Arc<StationService>>,
    charger_service: web::Data<std::sync::Arc<ChargerService>>,
) -> impl Responder {
    let start = Instant::now();
    
    // Check database health
    let database_status = match db.health_check().await {
        Ok(_) => "healthy",
        Err(_) => "unhealthy",
    };
    
    let database_response_time = start.elapsed().as_millis() as u64;
    let pool_stats = db.pool_stats().await;
    
    // Check company service health
    let company_service_start = Instant::now();
    let company_service_status = match company_service.get_company_count().await {
        Ok(_) => "healthy",
        Err(e) => "unhealthy",
    };
    let company_service_response_time = company_service_start.elapsed().as_millis() as u64;
    let company_service_error = if company_service_status == "unhealthy" {
        Some("Failed to access company service".to_string())
    } else {
        None
    };
    
    // Check station service health
    let station_service_start = Instant::now();
    let station_service_status = match station_service.get_station_count().await {
        Ok(_) => "healthy",
        Err(e) => "unhealthy",
    };
    let station_service_response_time = station_service_start.elapsed().as_millis() as u64;
    let station_service_error = if station_service_status == "unhealthy" {
        Some("Failed to access station service".to_string())
    } else {
        None
    };
    
    // Check charger service health
    let charger_service_start = Instant::now();
    let charger_service_status = match charger_service.get_charger_count().await {
        Ok(_) => "healthy",
        Err(e) => "unhealthy",
    };
    let charger_service_response_time = charger_service_start.elapsed().as_millis() as u64;
    let charger_service_error = if charger_service_status == "unhealthy" {
        Some("Failed to access charger service".to_string())
    } else {
        None
    };
    
    // Determine overall health
    let all_services_healthy = database_status == "healthy"
        && company_service_status == "healthy"
        && station_service_status == "healthy"
        && charger_service_status == "healthy";
    
    let overall_status = if all_services_healthy {
        "healthy"
    } else {
        "unhealthy"
    };
    
    let uptime_seconds = get_app_uptime();
    
    let health_response = HealthResponse {
        status: overall_status.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds,
        database: database_status.to_string(),
        services: ServiceHealth {
            company_service: company_service_status.to_string(),
            station_service: station_service_status.to_string(),
            charger_service: charger_service_status.to_string(),
        },
        details: HealthDetails {
            database: DatabaseHealth {
                status: database_status.to_string(),
                response_time_ms: database_response_time,
                pool_size: pool_stats.size,
                idle_connections: pool_stats.num_idle,
                active_connections: pool_stats.num_active,
            },
            services: ServiceHealthDetails {
                company_service: ServiceHealthDetail {
                    status: company_service_status.to_string(),
                    response_time_ms: company_service_response_time,
                    error: company_service_error,
                },
                station_service: ServiceHealthDetail {
                    status: station_service_status.to_string(),
                    response_time_ms: station_service_response_time,
                    error: station_service_error,
                },
                charger_service: ServiceHealthDetail {
                    status: charger_service_status.to_string(),
                    response_time_ms: charger_service_response_time,
                    error: charger_service_error,
                },
            },
        },
    };
    
    let status_code = if overall_status == "healthy" { 200 } else { 503 };
    
    HttpResponse::build(status_code)
        .content_type("application/json")
        .json(health_response)
}

/// Metrics endpoint for Prometheus
pub async fn metrics(
    db: web::Data<Database>,
    company_service: web::Data<std::sync::Arc<CompanyService>>,
    station_service: web::Data<std::sync::Arc<StationService>>,
    charger_service: web::Data<std::sync::Arc<ChargerService>>,
) -> impl Responder {
    let pool_stats = db.pool_stats().await;
    let uptime_seconds = get_app_uptime();
    
    // Get service counts for metrics
    let company_count = company_service.get_company_count().await.unwrap_or(0);
    let station_count = station_service.get_station_count().await.unwrap_or(0);
    let charger_count = charger_service.get_charger_count().await.unwrap_or(0);
    let available_charger_count = charger_service.get_available_charger_count().await.unwrap_or(0);
    
    let metrics = format!(
        r#"# HELP core_service_app_uptime_seconds Application uptime in seconds
# TYPE core_service_app_uptime_seconds counter
core_service_app_uptime_seconds {}

# HELP core_service_version_info Information about the core service version
# TYPE core_service_version_info gauge
core_service_version_info{{version="{}"}} 1

# HELP core_service_database_connections_total Total number of database connections
# TYPE core_service_database_connections_total gauge
core_service_database_connections_total {}

# HELP core_service_database_connections_idle Number of idle database connections
# TYPE core_service_database_connections_idle gauge
core_service_database_connections_idle {}

# HELP core_service_database_connections_active Number of active database connections
# TYPE core_service_database_connections_active gauge
core_service_database_connections_active {}

# HELP core_service_companies_total Total number of companies in the system
# TYPE core_service_companies_total gauge
core_service_companies_total {}

# HELP core_service_stations_total Total number of stations in the system
# TYPE core_service_stations_total gauge
core_service_stations_total {}

# HELP core_service_chargers_total Total number of chargers in the system
# TYPE core_service_chargers_total gauge
core_service_chargers_total {}

# HELP core_service_chargers_available_total Total number of available chargers in the system
# TYPE core_service_chargers_available_total gauge
core_service_chargers_available_total {}

# HELP core_service_health_check_requests_total Total number of health check requests
# TYPE core_service_health_check_requests_total counter
core_service_health_check_requests_total {{status="healthy"}} {}
core_service_health_check_requests_total {{status="unhealthy"}} {}

# HELP core_service_company_service_health_status Health status of company service (1=healthy, 0=unhealthy)
# TYPE core_service_company_service_health_status gauge
core_service_company_service_health_status {}

# HELP core_service_station_service_health_status Health status of station service (1=healthy, 0=unhealthy)
# TYPE core_service_station_service_health_status gauge
core_service_station_service_health_status {}

# HELP core_service_charger_service_health_status Health status of charger service (1=healthy, 0=unhealthy)
# TYPE core_service_charger_service_health_status gauge
core_service_charger_service_health_status {}
"#,
        uptime_seconds,
        env!("CARGO_PKG_VERSION"),
        pool_stats.size,
        pool_stats.num_idle,
        pool_stats.num_active,
        company_count,
        station_count,
        charger_count,
        available_charger_count,
        if company_count >= 0 { 1 } else { 0 }, // Healthy if we got a count
        if station_count >= 0 { 1 } else { 0 }, // Healthy if we got a count
        if charger_count >= 0 { 1 } else { 0 }  // Healthy if we got a count
    );
    
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4")
        .body(metrics)
}
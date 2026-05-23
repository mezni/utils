use tracing::{info, warn, error, debug, trace, span, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use std::env;

/// Initialize logging for the application
pub fn init_logging() {
    // Get log level from environment or default to info
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    
    // Get log format from environment or default to pretty
    let log_format = env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    
    // Get whether to use JSON format from environment or default to false
    let json_format = env::var("LOG_JSON").unwrap_or_else(|_| "false".to_string()) == "true";
    
    // Create the tracing subscriber
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level));
    
    if json_format {
        // JSON format for production
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json())
            .init();
    } else if log_format == "compact" {
        // Compact format
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().compact())
            .init();
    } else {
        // Pretty format for development
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().pretty())
            .init();
    }
    
    info!("Logging initialized with level: {}", log_level);
}

/// Create a span for request tracing
pub fn request_span(method: &str, path: &str) -> tracing::Span {
    span!(
        Level::INFO,
        "request",
        method = method,
        path = path,
    )
}

/// Log database query information
pub fn log_query(query: &str, params: &[&dyn sqlx::Encode<sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Sync]) {
    trace!(
        query = query,
        param_count = params.len(),
        "Database query executed"
    );
}

/// Log database query error
pub fn log_query_error(query: &str, error: &sqlx::Error) {
    error!(
        query = query,
        error = %error,
        "Database query failed"
    );
}

/// Log authentication event
pub fn log_auth_event(event: &str, user_id: Option<&str>, success: bool) {
    if success {
        info!(
            event = event,
            user_id = user_id.unwrap_or("unknown"),
            "Authentication succeeded"
        );
    } else {
        warn!(
            event = event,
            user_id = user_id.unwrap_or("unknown"),
            "Authentication failed"
        );
    }
}

/// Log API request
pub fn log_api_request(method: &str, path: &str, status: u16, duration_ms: u64) {
    info!(
        method = method,
        path = path,
        status = status,
        duration_ms = duration_ms,
        "API request processed"
    );
}

/// Log business event
pub fn log_business_event(event_type: &str, aggregate_type: &str, aggregate_id: &str, user_id: &str) {
    info!(
        event_type = event_type,
        aggregate_type = aggregate_type,
        aggregate_id = aggregate_id,
        user_id = user_id,
        "Business event occurred"
    );
}

/// Log error with context
pub fn log_error(context: &str, error: &dyn std::error::Error) {
    error!(
        context = context,
        error = %error,
        "Error occurred"
    );
}

/// Log warning with context
pub fn log_warning(context: &str, message: &str) {
    warn!(
        context = context,
        message = message,
        "Warning occurred"
    );
}

/// Log performance metric
pub fn log_metric(metric_name: &str, value: f64, tags: &[(&str, &str)]) {
    let tags_str = tags
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect::<Vec<_>>()
        .join(",");
    
    info!(
        metric_name = metric_name,
        value = value,
        tags = tags_str,
        "Performance metric recorded"
    );
}

/// Log database connection pool status
pub fn log_pool_status(size: u32, idle: u32, active: u32) {
    info!(
        pool_size = size,
        idle_connections = idle,
        active_connections = active,
        "Database connection pool status"
    );
}

/// Log health check result
pub fn log_health_check(service: &str, healthy: bool, response_time_ms: u64) {
    if healthy {
        info!(
            service = service,
            response_time_ms = response_time_ms,
            "Health check passed"
        );
    } else {
        error!(
            service = service,
            response_time_ms = response_time_ms,
            "Health check failed"
        );
    }
}

/// Log event publishing
pub fn log_event_publishing(event_type: &str, aggregate_id: &str, success: bool) {
    if success {
        info!(
            event_type = event_type,
            aggregate_id = aggregate_id,
            "Event published successfully"
        );
    } else {
        error!(
            event_type = event_type,
            aggregate_id = aggregate_id,
            "Event publishing failed"
        );
    }
}

/// Log outbox processing
pub fn log_outbox_processing(event_id: &str, status: &str) {
    info!(
        event_id = event_id,
        status = status,
        "Outbox event processed"
    );
}

/// Log configuration validation
pub fn log_config_validation(valid: bool, errors: &[String]) {
    if valid {
        info!("Configuration validation passed");
    } else {
        error!(
            errors = errors.join(", "),
            "Configuration validation failed"
        );
    }
}
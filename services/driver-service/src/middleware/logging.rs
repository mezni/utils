//! Structured logging for telemetry operations

use tracing::{debug, error, info, warn};

pub struct TelemetryLogger {
    service_name: String,
}

impl TelemetryLogger {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }

    pub fn log_ingestion(&self, event_id: &str, user_id: &str, status: &str) {
        info!(
            service = %self.service_name,
            event_id = %event_id,
            user_id = %user_id,
            status = %status,
            "Telemetry event ingestion"
        );
    }

    pub fn log_validation(&self, event_id: &str, error: &str) {
        debug!(
            service = %self.service_name,
            event_id = %event_id,
            error = %error,
            "Event validation"
        );
    }

    pub fn log_enrichment(&self, event_id: &str, enrichment_type: &str) {
        debug!(
            service = %self.service_name,
            event_id = %event_id,
            enrichment_type = %enrichment_type,
            "Event enrichment"
        );
    }

    pub fn log_duplicate(&self, event_id: &str, idempotency_key: &str) {
        warn!(
            service = %self.service_name,
            event_id = %event_id,
            idempotency_key = %idempotency_key,
            "Duplicate event detected"
        );
    }

    pub fn log_error(&self, event_id: &str, error: &str) {
        error!(
            service = %self.service_name,
            event_id = %event_id,
            error = %error,
            "Telemetry operation failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logger_creation() {
        let logger = TelemetryLogger::new("test-service".to_string());
        assert_eq!(logger.service_name, "test-service");
    }

    #[test]
    fn test_logger_methods() {
        let logger = TelemetryLogger::new("test-service".to_string());
        logger.log_ingestion("test-id", "test-user", "success");
        logger.log_validation("test-id", "test-error");
        logger.log_enrichment("test-id", "location");
        logger.log_duplicate("test-id", "test-key");
        logger.log_error("test-id", "test-error");
        // These just log, no assertion needed
    }
}

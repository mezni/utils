use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::role::Role;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_type: String,
    pub source_service: String,
    pub event_data: Value,
    pub idempotency_key: String,
}

impl AuditEvent {
    pub fn new(
        event_type: impl Into<String>,
        source_service: impl Into<String>,
        event_data: Value,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            source_service: source_service.into(),
            event_data,
            idempotency_key: idempotency_key.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEventData {
    pub user_uuid: Uuid,
    pub role: Role,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub correlation_id: String,
    pub reason: Option<String>,
}

impl SecurityEventData {
    pub fn new(
        user_uuid: Uuid,
        role: Role,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            user_uuid,
            role,
            ip_address: None,
            user_agent: None,
            correlation_id: correlation_id.into(),
            reason: None,
        }
    }

    pub fn with_ip(mut self, ip: impl Into<String>) -> Self {
        self.ip_address = Some(ip.into());
        self
    }

    pub fn with_user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

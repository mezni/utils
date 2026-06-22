//! Database module for admin-service
//! Exports analytics query functionality

pub mod queries;
pub use queries::{get_analytics_events, get_event_count, AnalyticsQueryParams};

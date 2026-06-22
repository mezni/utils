//! Database module for driver-service
//! Exports all database-related modules

pub mod analytics;
pub mod spatial;

// Re-export commonly used types
pub use analytics::{AnalyticsQuery, AnalyticsQueryResponse};

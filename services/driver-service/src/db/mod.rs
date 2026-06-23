//! Database module for driver-service
//! Exports all database-related modules

pub mod analytics;
pub mod spatial;
pub mod pool;

// Re-export commonly used types
pub use analytics::{AnalyticsQuery, AnalyticsQueryResponse};
pub use pool::{PlatformDb, AnalyticsDb};

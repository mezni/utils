//! Database module for admin-service
//! Exports analytics query functionality and database pool

pub mod analytics_db;
pub mod queries;

pub use analytics_db::{create_pool, DatabaseConfig, PoolStats};
pub use queries::*;

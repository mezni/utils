#![deny(warnings)]
#![deny(missing_docs)]

//! ev-db — PostgreSQL pool and pagination utilities.
//!
//! Provides a shared PostgreSQL connection pool initializer and a generic
//! paginated response struct used by all Rust services.

mod pagination;
mod pool;

pub use pagination::*;
pub use pool::*;

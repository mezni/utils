#![deny(warnings)]
#![deny(missing_docs)]

//! ev-core — shared enums and NanoID generation.
//!
//! Provides canonical enum types used across all Rust services, matching the
//! MVP-1 data model conventions. Also provides NanoID generation with
//! configurable prefix and length for creating unique identifiers.

mod enums;
mod id;

pub use enums::*;
pub use id::*;

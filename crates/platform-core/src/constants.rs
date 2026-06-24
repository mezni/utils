//! Shared application constants

/// Default page size for pagination
pub const DEFAULT_PAGE_SIZE: u32 = 50;

/// Maximum page size to prevent large queries
pub const MAX_PAGE_SIZE: u32 = 100;

/// Minimum page size
pub const MIN_PAGE_SIZE: u32 = 1;

/// Emitted entity types for ID generation
pub const PARTNER_PREFIX: &str = "PRT-";
pub const STATION_PREFIX: &str = "STA-";
pub const CHARGER_PREFIX: &str = "CHR-";

/// ID length after prefix
pub const ID_LENGTH: usize = 12;

/// Charger power rating bounds in kW
pub const MIN_POWER_RATING: i32 = 1;
pub const MAX_POWER_RATING: i32 = 1000;

/// Partner name validation limits
pub const MAX_PARTNER_NAME_LENGTH: usize = 200;

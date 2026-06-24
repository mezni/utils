//! Pagination validation

use bornemap_platform_core::constants::{MAX_PAGE_SIZE, MIN_PAGE_SIZE};

/// Validate and clamp page number
pub fn validate_page(page: u32) -> u32 {
    page.clamp(MIN_PAGE_SIZE, u32::MAX)
}

/// Validate and clamp limit
pub fn validate_limit(limit: u32) -> u32 {
    limit.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE)
}

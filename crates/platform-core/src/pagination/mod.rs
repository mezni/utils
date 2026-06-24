//! Pagination data structure

use bornemap_platform_core::constants::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE};

/// Pagination response structure
#[derive(Debug, Clone, Serialize)]
pub struct Pagination {
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    pub pages: u32,
}

impl Pagination {
    pub fn new(page: u32, limit: u32, total: u64) -> Self {
        let pages = (total as u32 / limit).saturating_add(1);
        Self {
            page,
            limit,
            total,
            pages,
        }
    }
}

/// Validate and clamp page number
pub fn validate_page(page: u32) -> u32 {
    page.clamp(MIN_PAGE_SIZE, u32::MAX)
}

/// Validate and clamp limit
pub fn validate_limit(limit: u32) -> u32 {
    limit.clamp(MIN_PAGE_SIZE, MAX_PAGE_SIZE)
}

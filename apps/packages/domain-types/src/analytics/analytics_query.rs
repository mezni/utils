//! Analytics query DTOs for admin-service
//! Request parameters and validation for analytics endpoints

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Analytics query parameters with filtering and pagination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsQuery {
    /// Filter by partner_id (for partner isolation)
    pub partner_id: Option<String>,
    /// Filter by station_id
    pub station_id: Option<String>,
    /// Filter by user_uuid
    pub user_uuid: Option<String>,
    /// Filter by date range
    pub date_range: Option<DateRange>,
    /// Filter by event type
    pub event_type: Option<String>,
    /// Page number (1-based, default 1)
    pub page: usize,
    /// Items per page (default 100, max 1000)
    pub per_page: usize,
    /// Sort by field
    pub sort_by: Option<String>,
    /// Sort order ("asc" or "desc")
    pub sort_order: Option<String>,
}

/// Date range for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateRange {
    /// Start date (ISO 8601 format)
    pub start: String,
    /// End date (ISO 8601 format)
    pub end: String,
}

/// KPI query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KPIQuery {
    /// KPI name to query
    pub kpi_name: String,
    /// Filter by partner_id
    pub partner_id: Option<String>,
    /// Filter by date range
    pub date_range: Option<DateRange>,
}

/// Pagination metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationMetadata {
    /// Current page
    pub page: usize,
    /// Items per page
    pub per_page: usize,
    /// Total items
    pub total_items: u64,
    /// Total pages
    pub total_pages: usize,
    /// Previous page (None if first page)
    pub previous_page: Option<usize>,
    /// Next page (None if last page)
    pub next_page: Option<usize>,
}

impl AnalyticsQuery {
    /// Create default analytics query
    pub fn default() -> Self {
        Self {
            partner_id: None,
            station_id: None,
            user_uuid: None,
            date_range: None,
            event_type: None,
            page: 1,
            per_page: 100,
            sort_by: None,
            sort_order: None,
        }
    }

    /// Validate query parameters
    pub fn validate(&self) -> Result<(), String> {
        // Validate page number
        if self.page < 1 {
            return Err("Page number must be >= 1".to_string());
        }

        // Validate per_page
        if self.per_page < 1 || self.per_page > 1000 {
            return Err("Per-page must be between 1 and 1000".to_string());
        }

        // Validate sort order
        if let Some(order) = &self.sort_order {
            if order.to_lowercase() != "asc" && order.to_lowercase() != "desc" {
                return Err("Sort order must be 'asc' or 'desc'".to_string());
            }
        }

        // Validate date range if provided
        if let Some(range) = &self.date_range {
            if let Err(e) = range.validate() {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Get partner_id filter (prioritizes partner_id over partner)
    pub fn effective_partner_id(&self) -> Option<&String> {
        self.partner_id.as_ref()
    }

    /// Get effective sort order (defaults to "desc" if not specified)
    pub fn effective_sort_order(&self) -> String {
        self.sort_order
            .as_deref()
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "desc".to_string())
    }
}

impl DateRange {
    /// Create a new date range
    pub fn new(start: String, end: String) -> Result<Self, String> {
        Self::validate_date(&start)?;
        Self::validate_date(&end)?;

        // Validate that start <= end
        let start_date = DateTime::parse_from_rfc3339(&start)
            .map_err(|_| "Invalid start date format".to_string())?;
        let end_date = DateTime::parse_from_rfc3339(&end)
            .map_err(|_| "Invalid end date format".to_string())?;

        if start_date > end_date {
            return Err("Start date must be before or equal to end date".to_string());
        }

        Ok(Self { start, end })
    }

    /// Validate date format
    fn validate_date(date_str: &str) -> Result<(), String> {
        if date_str.is_empty() {
            return Err("Date cannot be empty".to_string());
        }

        if !date_str.ends_with("Z") {
            // Try parsing without Z suffix
            if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
                return Ok(());
            }
        } else {
            if DateTime::parse_from_rfc3339(date_str).is_ok() {
                return Ok(());
            }
        }

        Err("Date must be in ISO 8601 format (e.g., 2026-06-22T15:30:00Z)".to_string())
    }

    /// Validate date range
    pub fn validate(&self) -> Result<(), String> {
        Self::validate_date(&self.start)?;
        Self::validate_date(&self.end)?;

        let start_date = DateTime::parse_from_rfc3339(&self.start)
            .map_err(|_| "Invalid start date format".to_string())?;
        let end_date = DateTime::parse_from_rfc3339(&self.end)
            .map_err(|_| "Invalid end date format".to_string())?;

        if start_date > end_date {
            return Err("Start date must be before or equal to end date".to_string());
        }

        Ok(())
    }

    /// Check if date range is valid
    pub fn is_valid(&self) -> bool {
        self.validate().is_ok()
    }
}

impl PaginationMetadata {
    /// Create pagination metadata from total items
    pub fn new(page: usize, per_page: usize, total_items: u64) -> Self {
        let total_pages = (total_items as f64 / per_page as f64).ceil() as usize;

        Self {
            page,
            per_page,
            total_items,
            total_pages,
            previous_page: if page > 1 { Some(page - 1) } else { None },
            next_page: if page < total_pages { Some(page + 1) } else { None },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_query() {
        let query = AnalyticsQuery::default();
        assert_eq!(query.page, 1);
        assert_eq!(query.per_page, 100);
    }

    #[test]
    fn test_query_validation_page() {
        let mut query = AnalyticsQuery::default();
        query.page = 0;

        assert!(query.validate().is_err());
    }

    #[test]
    fn test_query_validation_per_page() {
        let mut query = AnalyticsQuery::default();
        query.per_page = 1001;

        assert!(query.validate().is_err());
    }

    #[test]
    fn test_date_range_creation() {
        let range = DateRange::new(
            "2026-06-22T00:00:00Z".to_string(),
            "2026-06-23T00:00:00Z".to_string(),
        );

        assert!(range.is_ok());
    }

    #[test]
    fn test_date_range_invalid_order() {
        let range = DateRange::new(
            "2026-06-23T00:00:00Z".to_string(),
            "2026-06-22T00:00:00Z".to_string(),
        );

        assert!(range.is_err());
    }

    #[test]
    fn test_pagination_metadata() {
        let meta = PaginationMetadata::new(1, 100, 500);

        assert_eq!(meta.page, 1);
        assert_eq!(meta.per_page, 100);
        assert_eq!(meta.total_items, 500);
        assert_eq!(meta.total_pages, 5);
        assert!(meta.previous_page.is_none());
        assert!(meta.next_page.is_some());
    }
}
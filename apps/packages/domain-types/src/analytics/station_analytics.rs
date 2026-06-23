//! Station analytics DTOs for admin-service
//! Station-specific analytics response and data models

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Station analytics response with station-specific metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationAnalytics {
    /// Station entity ID (PREFIX-nanoid(12))
    pub station_id: String,
    /// Total number of views
    pub station_views: u64,
    /// Total number of favorites
    pub favorites: u64,
    /// Number of search hits
    pub search_hits: u64,
    /// Average session time in seconds
    pub avg_session_time_seconds: f64,
    /// Number of unique users who visited
    pub unique_users: u64,
    /// Last viewed timestamp
    pub last_viewed_at: Option<String>,
    /// First viewed timestamp
    pub first_viewed_at: Option<String>,
    /// Partner ID (for partner isolation)
    pub partner_id: Option<String>,
}

/// Database row for station analytics (for conversion)
#[derive(Debug, Clone)]
pub struct StationUsageRow {
    pub station_id: String,
    pub station_views: u64,
    pub total_favorites: u64,
    pub favorite_count: u64,
    pub unique_users: u64,
    pub avg_session_gap_seconds: f64,
    pub last_viewed_at: Option<DateTime<Utc>>,
    pub first_viewed_at: Option<DateTime<Utc>>,
    pub partner_id: Option<String>,
}

impl From<StationUsageRow> for StationAnalytics {
    fn from(row: StationUsageRow) -> Self {
        Self {
            station_id: row.station_id,
            station_views: row.station_views,
            favorites: row.favorite_count,
            search_hits: 0, // Calculated separately
            avg_session_time_seconds: row.avg_session_gap_seconds,
            unique_users: row.unique_users,
            last_viewed_at: row.last_viewed_at.map(|dt| dt.to_rfc3339()),
            first_viewed_at: row.first_viewed_at.map(|dt| dt.to_rfc3339()),
            partner_id: row.partner_id,
        }
    }
}

/// Station analytics with search hits calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationAnalyticsWithSearch {
    /// Station analytics data
    pub analytics: StationAnalytics,
    /// Number of search hits (calculated separately)
    pub search_hits: u64,
}

/// Station analytics query parameters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StationAnalyticsQuery {
    /// Filter by partner_id (for partner isolation)
    pub partner_id: Option<String>,
    /// Partner ID filter (alternative naming)
    pub partner: Option<String>,
}

impl StationAnalyticsQuery {
    /// Validate partner ID format (PREFIX-nanoid(12))
    pub fn validate_partner_id(&self) -> Result<(), String> {
        if let Some(partner_id) = &self.partner_id {
            if let Some(partner) = &self.partner {
                // Both provided, check consistency
                if partner_id != partner {
                    return Err(format!(
                        "partner_id ({}) and partner ({}) must match",
                        partner_id, partner
                    ));
                }
            }
        }

        Ok(())
    }

    /// Get effective partner_id (prioritizes partner_id)
    pub fn effective_partner_id(&self) -> Option<&String> {
        self.partner_id.as_ref().or(self.partner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_analytics_creation() {
        let row = StationUsageRow {
            station_id: "STA-test123".to_string(),
            station_views: 1000,
            total_favorites: 500,
            favorite_count: 50,
            unique_users: 200,
            avg_session_gap_seconds: 180.5,
            last_viewed_at: Some(Utc::now()),
            first_viewed_at: Some(Utc::now()),
            partner_id: Some("STX-xxx".to_string()),
        };

        let analytics: StationAnalytics = row.into();

        assert_eq!(analytics.station_id, "STA-test123");
        assert_eq!(analytics.station_views, 1000);
        assert_eq!(analytics.favorites, 50);
        assert_eq!(analytics.unique_users, 200);
        assert_eq!(analytics.avg_session_time_seconds, 180.5);
        assert_eq!(analytics.partner_id, Some("STX-xxx".to_string()));
    }

    #[test]
    fn test_partner_id_validation() {
        let valid_query = StationAnalyticsQuery {
            partner_id: Some("STX-xxx".to_string()),
            partner: None,
        };

        assert!(valid_query.validate_partner_id().is_ok());

        let invalid_query = StationAnalyticsQuery {
            partner_id: Some("STA-xxx".to_string()),
            partner: Some("STX-yyy".to_string()),
        };

        assert!(invalid_query.validate_partner_id().is_err());
    }
}
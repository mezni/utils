//! Summary analytics DTOs for admin-service
//! Platform-wide KPI aggregation response and data models

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Summary analytics response with platform-wide KPIs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryAnalytics {
    /// Total number of station views
    pub station_views: u64,
    /// Total number of search events
    pub search_volume: u64,
    /// Total number of favorite events
    pub favorite_count: u64,
    /// Number of unique active users
    pub active_users: u64,
    /// Total number of stations tracked
    pub total_stations: u64,
    /// Total number of users tracked
    pub total_users: u64,
    /// Total number of search events
    pub total_searches: u64,
    /// Top search trends (optional, limited to 10)
    pub trends: Vec<SearchTrend>,
}

/// Individual search trend entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchTrend {
    /// Search query text
    pub query_text: String,
    /// Number of search events
    pub search_count: u64,
    /// Number of unique users who searched
    pub unique_searchers: u64,
    /// Number of distinct stations in results
    pub stations_searched: u64,
    /// Average hours between searches
    pub query_frequency_hours: f64,
    /// Last search timestamp
    pub last_search_at: String,
    /// First search timestamp
    pub first_search_at: String,
}

/// KPI aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KPIAggregation {
    /// KPI name
    pub kpi_name: String,
    /// KPI value
    pub value: f64,
    /// KPI unit
    pub unit: String,
    /// Source table
    pub source: String,
    /// Aggregation timestamp
    pub calculated_at: DateTime<Utc>,
}

/// Summary analytics with KPI breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryWithKPIs {
    /// Platform-wide summary
    pub summary: SummaryAnalytics,
    /// Detailed KPI breakdown
    pub kpis: Vec<KPIAggregation>,
}

impl SummaryAnalytics {
    /// Create a new summary with default values
    pub fn new(station_views: u64, search_volume: u64, favorite_count: u64, active_users: u64) -> Self {
        Self {
            station_views,
            search_volume,
            favorite_count,
            active_users,
            total_stations: 0,
            total_users: 0,
            total_searches: 0,
            trends: Vec::new(),
        }
    }

    /// Add a search trend
    pub fn add_trend(&mut self, trend: SearchTrend) {
        self.trends.push(trend);

        // Keep only top 10 trends
        if self.trends.len() > 10 {
            self.trends.sort_by(|a, b| b.search_count.cmp(&a.search_count));
            self.trends.truncate(10);
        }
    }

    /// Calculate cache hit rate from search trends
    pub fn search_hit_rate(&self) -> f64 {
        if self.search_volume == 0 {
            0.0
        } else {
            self.search_volume as f64 / (self.station_views + self.search_volume) as f64
        }
    }
}

impl Default for SummaryAnalytics {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_creation() {
        let summary = SummaryAnalytics::new(1000, 500, 200, 150);

        assert_eq!(summary.station_views, 1000);
        assert_eq!(summary.search_volume, 500);
        assert_eq!(summary.favorite_count, 200);
        assert_eq!(summary.active_users, 150);
        assert_eq!(summary.total_stations, 0);
        assert_eq!(summary.total_users, 0);
        assert_eq!(summary.total_searches, 0);
    }

    #[test]
    fn test_add_trend() {
        let mut summary = SummaryAnalytics::new(0, 0, 0, 0);
        let trend = SearchTrend {
            query_text: "test query".to_string(),
            search_count: 10,
            unique_searchers: 5,
            stations_searched: 3,
            query_frequency_hours: 2.5,
            last_search_at: Utc::now().to_rfc3339(),
            first_search_at: Utc::now().to_rfc3339(),
        };

        summary.add_trend(trend.clone());
        assert_eq!(summary.trends.len(), 1);
        assert_eq!(summary.trends[0].query_text, "test query");
    }

    #[test]
    fn test_trend_limit() {
        let mut summary = SummaryAnalytics::new(0, 0, 0, 0);

        // Add 15 trends
        for i in 0..15 {
            let trend = SearchTrend {
                query_text: format!("query {}", i),
                search_count: i as u64,
                unique_searchers: i,
                stations_searched: i,
                query_frequency_hours: i as f64,
                last_search_at: Utc::now().to_rfc3339(),
                first_search_at: Utc::now().to_rfc3339(),
            };
            summary.add_trend(trend);
        }

        // Should only have 10 trends
        assert_eq!(summary.trends.len(), 10);
    }
}
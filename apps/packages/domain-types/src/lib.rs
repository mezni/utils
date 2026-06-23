pub mod role;
pub mod jwt;
pub mod audit;
pub mod user;
pub mod gis;
pub mod analytics;
pub mod favorites;
pub mod search;
pub mod preferences;
pub mod events;

pub use role::Role;
pub use jwt::JwtClaims;
pub use audit::{AuditEvent, SecurityEventData};
pub use user::UserProfile;
pub use gis::{Station, StationDetail, StationList, Pagination, NearbySearchQuery, StationDetailQuery, Address};
pub use analytics::{
    AnalyticsResponse, AnalyticsMetadata, CacheStatus, StationAnalytics, StationAnalyticsQuery,
    SummaryAnalytics, SummaryWithKPIs, KPIAggregation, SearchTrend, AnalyticsQuery, DateRange,
    KPIQuery, PaginationMetadata,
};
pub use favorites::{AddFavoriteRequest, RemoveFavoriteRequest, FavoriteItem, FavoritesListResponse, FavoritesMetadata, FavoriteResponse};
pub use search::{SearchResult, SearchResponse, SearchMetadata, SearchQuery};
pub use preferences::{Preferences, PreferencesResponse, UpdatePreferencesRequest, Region, MapFilters};

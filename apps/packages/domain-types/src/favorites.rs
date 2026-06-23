use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFavoriteRequest {
    pub station_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveFavoriteRequest {
    pub station_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoriteItem {
    pub station_id: String,
    pub added_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoritesListResponse {
    pub data: Vec<FavoriteItem>,
    pub metadata: FavoritesMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoritesMetadata {
    pub total: usize,
    pub page: u32,
    pub per_page: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FavoriteResponse {
    pub station_id: String,
    pub added_at: chrono::DateTime<chrono::Utc>,
}

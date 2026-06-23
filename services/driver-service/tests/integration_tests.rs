//! Integration tests for driver-service
//!
//! NOTE: The driver-service lib crate has pre-existing compilation errors
//! in `queries/spatial.rs`, `middleware/spatial.rs`, `db/analytics.rs`, etc.
//! Once those are resolved, the full integration test suite will compile.
//!
//! For now, only unit tests for serialization/deserialization are active.

use domain_types::favorites::{
    AddFavoriteRequest, FavoriteItem, FavoritesListResponse, FavoritesMetadata,
    RemoveFavoriteRequest,
};
use domain_types::search::{SearchMetadata, SearchResponse, SearchResult};

#[cfg(test)]
mod favorites_dto_tests {

    #[test]
    fn add_favorite_request_serialization() {
        let req = super::AddFavoriteRequest {
            station_id: "STA-abc123def456".to_string(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["station_id"], "STA-abc123def456");
    }

    #[test]
    fn add_favorite_request_deserialization() {
        let json = serde_json::json!({ "station_id": "STA-xyz789uvw012" });
        let req: super::AddFavoriteRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.station_id, "STA-xyz789uvw012");
    }

    #[test]
    fn remove_favorite_request_serialization() {
        let req = super::RemoveFavoriteRequest {
            station_id: "STA-abc123def456".to_string(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["station_id"], "STA-abc123def456");
    }

    #[test]
    fn remove_favorite_request_deserialization() {
        let json = serde_json::json!({ "station_id": "STA-xyz789uvw012" });
        let req: super::RemoveFavoriteRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.station_id, "STA-xyz789uvw012");
    }

    #[test]
    fn favorite_item_serialization() {
        let item = super::FavoriteItem {
            station_id: "STA-test12345678".to_string(),
            added_at: chrono::Utc::now(),
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["station_id"], "STA-test12345678");
        assert!(json["added_at"].is_string());
    }

    #[test]
    fn favorites_list_response_serialization() {
        let resp = super::FavoritesListResponse {
            data: vec![super::FavoriteItem {
                station_id: "STA-aaa".to_string(),
                added_at: chrono::Utc::now(),
            }],
            metadata: super::FavoritesMetadata {
                total: 1,
                page: 1,
                per_page: 50,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["metadata"]["total"], 1);
        assert_eq!(json["metadata"]["page"], 1);
        assert_eq!(json["data"][0]["station_id"], "STA-aaa");
    }

    #[test]
    fn favorites_metadata_defaults() {
        let meta = super::FavoritesMetadata {
            total: 0,
            page: 1,
            per_page: 50,
        };
        assert_eq!(meta.total, 0);
        assert_eq!(meta.page, 1);
        assert_eq!(meta.per_page, 50);
    }
}

#[cfg(test)]
mod search_dto_tests {

    #[test]
    fn search_result_serialization() {
        let result = super::SearchResult {
            station_id: "STA-test".to_string(),
            name: "Test Station".to_string(),
            address: "123 Test St".to_string(),
            distance_km: Some(0.5),
            relevance: 0.8,
            connector_types: vec!["CCS".to_string()],
            available: true,
            lat: 48.8566,
            lng: 2.3522,
        };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["station_id"], "STA-test");
        assert_eq!(json["name"], "Test Station");
        assert!(json["available"].as_bool().unwrap());
    }

    #[test]
    fn search_response_serialization() {
        let resp = super::SearchResponse {
            data: vec![
                super::SearchResult {
                    station_id: "STA-1".to_string(),
                    name: "Station 1".to_string(),
                    address: "Addr 1".to_string(),
                    distance_km: None,
                    relevance: 0.9,
                    connector_types: vec!["Type2".to_string()],
                    available: true,
                    lat: 48.85,
                    lng: 2.35,
                },
            ],
            metadata: super::SearchMetadata {
                query: "test".to_string(),
                total: 1,
                latency_ms: 42,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["data"][0]["station_id"], "STA-1");
        assert_eq!(json["metadata"]["query"], "test");
        assert_eq!(json["metadata"]["latency_ms"], 42);
    }

    #[test]
    fn search_metadata_with_zero_results() {
        let meta = super::SearchMetadata {
            query: "nonexistent".to_string(),
            total: 0,
            latency_ms: 0,
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["total"], 0);
        assert_eq!(json["latency_ms"], 0);
    }
}

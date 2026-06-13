use std::env;

#[cfg(test)]
mod contract_tests {
    use reqwest::Client;
    use serde_json::Value;

    fn base_url() -> String {
        env::var("API_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
    }

    #[tokio::test]
    async fn test_health_check_returns_200() {
        let client = Client::new();
        let resp = client
            .get(format!("{}/health", base_url()))
            .send()
            .await
            .expect("Health check request failed");

        assert_eq!(resp.status(), 200);

        let body: Value = resp.json().await.expect("Health response is valid JSON");
        assert_eq!(body["status"], "ok");
        assert_eq!(body["service"], "driver-service");
        assert!(body["database"].is_string());
        assert!(body["version"].is_string());
        assert!(body["uptime_secs"].is_number());
    }

    #[tokio::test]
    async fn test_list_stations_returns_paginated_response() {
        let client = Client::new();
        let resp = client
            .get(format!("{}/api/v1/stations", base_url()))
            .send()
            .await
            .expect("List stations request failed");

        assert_eq!(resp.status(), 200);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        assert!(body["data"].is_array());
        assert!(body["total"].is_number());
        assert!(body["page"].is_number());
        assert!(body["per_page"].is_number());
    }

    #[tokio::test]
    async fn test_list_stations_respects_pagination() {
        let client = Client::new();
        let resp = client
            .get(format!("{}/api/v1/stations?page=1&per_page=5", base_url()))
            .send()
            .await
            .expect("List stations request failed");

        assert_eq!(resp.status(), 200);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        let data = body["data"].as_array().unwrap();
        assert!(data.len() <= 5);
        assert_eq!(body["page"], 1);
        assert_eq!(body["per_page"], 5);
    }

    #[tokio::test]
    async fn test_list_stations_station_shape() {
        let client = Client::new();
        let resp = client
            .get(format!("{}/api/v1/stations", base_url()))
            .send()
            .await
            .expect("List stations request failed");

        assert_eq!(resp.status(), 200);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        let stations = body["data"].as_array().unwrap();

        if !stations.is_empty() {
            for station in stations {
                assert!(station["id"].is_string(), "station.id must be a string");
                assert!(station["name"].is_string(), "station.name must be a string");
                assert!(station["address"].is_string(), "station.address must be a string");
                assert!(station["lat"].is_number(), "station.lat must be a number");
                assert!(station["lng"].is_number(), "station.lng must be a number");
                assert!(station["status"].is_string(), "station.status must be a string");
                assert!(station["partner_id"].is_string(), "station.partner_id must be a string");
                assert!(station["created_at"].is_string(), "station.created_at must be a string");
                assert!(station["updated_at"].is_string(), "station.updated_at must be a string");
            }
        }
    }

    #[tokio::test]
    async fn test_nearby_stations_returns_200() {
        let client = Client::new();
        let resp = client
            .get(format!(
                "{}/api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius=50",
                base_url()
            ))
            .send()
            .await
            .expect("Nearby stations request failed");

        assert_eq!(resp.status(), 200);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        assert!(body["data"].is_array());
        assert!(body["total"].is_number());

        for station in body["data"].as_array().unwrap() {
            assert!(station["distance_km"].is_number());
        }
    }

    #[tokio::test]
    async fn test_nearby_stations_validates_coordinates() {
        let client = Client::new();

        // Invalid lat
        let resp = client
            .get(format!(
                "{}/api/v1/stations/nearby?lat=100&lng=10&radius=50",
                base_url()
            ))
            .send()
            .await
            .expect("Nearby stations request failed");

        assert_eq!(resp.status(), 400);

        // Invalid lng
        let resp = client
            .get(format!(
                "{}/api/v1/stations/nearby?lat=36&lng=200&radius=50",
                base_url()
            ))
            .send()
            .await
            .expect("Nearby stations request failed");

        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_get_station_by_id_returns_200() {
        let client = Client::new();

        // First, get a station ID from the list
        let list_resp = client
            .get(format!("{}/api/v1/stations", base_url()))
            .send()
            .await
            .expect("List stations request failed");

        let list_body: Value = list_resp.json().await.expect("Response is valid JSON");
        let stations = list_body["data"].as_array().unwrap();

        if stations.is_empty() {
            eprintln!("No stations available to test get_station_by_id — skipping");
            return;
        }

        let station_id = stations[0]["id"].as_str().unwrap();

        let resp = client
            .get(format!("{}/api/v1/stations/{}", base_url(), station_id))
            .send()
            .await
            .expect("Get station request failed");

        assert_eq!(resp.status(), 200);

        let station: Value = resp.json().await.expect("Response is valid JSON");
        assert_eq!(station["id"], station_id);
        assert!(station["name"].is_string());
        assert!(station["address"].is_string());
        assert!(station["lat"].is_number());
        assert!(station["lng"].is_number());
        assert!(station["status"].is_string());
        assert!(station["partner_id"].is_string());
        assert!(station["created_at"].is_string());
        assert!(station["updated_at"].is_string());
    }

    #[tokio::test]
    async fn test_get_station_returns_404_for_unknown_id() {
        let client = Client::new();
        let resp = client
            .get(format!(
                "{}/api/v1/stations/STA-nonexistent00000000000000",
                base_url()
            ))
            .send()
            .await
            .expect("Get station request failed");

        assert_eq!(resp.status(), 404);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        assert_eq!(body["error"]["code"], "NOT_FOUND");
    }
}

use std::env;

#[cfg(test)]
mod contract_tests {
    use reqwest::Client;
    use serde_json::{json, Value};

    fn fake_uuid() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        format!("test-session-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    fn base_url() -> String {
        env::var("API_BASE_URL").unwrap_or_else(|_| "http://localhost:8081".to_string())
    }

    /// Get a valid station ID (first from list via driver-service) or None.
    async fn get_first_station_id(client: &Client) -> Option<String> {
        let driver_base =
            env::var("DRIVER_API_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
        let resp = client
            .get(format!("{}/api/v1/stations?per_page=1", driver_base))
            .send()
            .await
            .ok()?;
        if resp.status() != 200 {
            return None;
        }
        let body: Value = resp.json().await.ok()?;
        let data = body["data"].as_array()?;
        data.first().and_then(|s| s["id"].as_str().map(String::from))
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
        assert_eq!(body["service"], "admin-service");
        assert!(body["database"].is_string());
        assert!(body["version"].is_string());
        assert!(body["uptime_secs"].is_number());
    }

    #[tokio::test]
    async fn test_create_station_returns_201() {
        let client = Client::new();
        let body = json!({
            "name": "Contract Test Station",
            "address": "42 Test Street, Tunis",
            "lat": 36.8065,
            "lng": 10.1815,
            "partner_id": "PRT-test0000000000000000001",
            "chargers": [
                { "type": "CCS", "power_kw": 150.0, "price_per_kwh": 0.35 }
            ]
        });

        let resp = client
            .post(format!("{}/api/v1/stations", base_url()))
            .json(&body)
            .send()
            .await
            .expect("Create station request failed");

        assert_eq!(resp.status(), 201);

        let station: Value = resp.json().await.expect("Response is valid JSON");
        assert!(station["id"].is_string());
        assert!(station["id"].as_str().unwrap().starts_with("STA-"));
        assert_eq!(station["name"], "Contract Test Station");
        assert_eq!(station["address"], "42 Test Street, Tunis");
        assert_eq!(station["lat"], 36.8065);
        assert_eq!(station["lng"], 10.1815);
        assert_eq!(station["status"], "offline");
        assert!(station["chargers"].is_array());
        assert_eq!(station["chargers"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_create_station_validates_required_fields() {
        let client = Client::new();

        // Empty name
        let resp = client
            .post(format!("{}/api/v1/stations", base_url()))
            .json(&json!({
                "name": "",
                "address": "42 Test Street, Tunis",
                "lat": 36.8065,
                "lng": 10.1815,
                "partner_id": "PRT-test0000000000000000001",
                "chargers": [{"type": "CCS", "power_kw": 150}]
            }))
            .send()
            .await
            .expect("Create station request failed");

        assert_eq!(resp.status(), 400);

        // Empty chargers
        let resp = client
            .post(format!("{}/api/v1/stations", base_url()))
            .json(&json!({
                "name": "Test Station",
                "address": "42 Test Street, Tunis",
                "lat": 36.8065,
                "lng": 10.1815,
                "partner_id": "PRT-test0000000000000000001",
                "chargers": []
            }))
            .send()
            .await
            .expect("Create station request failed");

        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_update_station_returns_200() {
        let client = Client::new();
        let station_id = get_first_station_id(&client).await;

        if let Some(id) = station_id {
            let resp = client
                .put(format!("{}/api/v1/stations/{}", base_url(), id))
                .json(&json!({
                    "name": "Updated Station Name"
                }))
                .send()
                .await
                .expect("Update station request failed");

            // 200 on success, 404 if not found
            let status = resp.status();
            assert!(
                status == 200 || status == 404,
                "Expected 200 or 404, got {}",
                status
            );

            if status == 200 {
                let station: Value = resp.json().await.expect("Response is valid JSON");
                assert_eq!(station["name"], "Updated Station Name");
            }
        } else {
            eprintln!("No stations available — skipping test_update_station");
        }
    }

    #[tokio::test]
    async fn test_update_station_returns_404_for_unknown() {
        let client = Client::new();
        let resp = client
            .put(format!(
                "{}/api/v1/stations/STA-nonexistent00000000000000",
                base_url()
            ))
            .json(&json!({ "name": "Ghost" }))
            .send()
            .await
            .expect("Update station request failed");

        assert_eq!(resp.status(), 404);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        assert_eq!(body["error"]["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn test_delete_station_returns_204() {
        let client = Client::new();
        let station_id = get_first_station_id(&client).await;

        if let Some(id) = station_id {
            let resp = client
                .delete(format!("{}/api/v1/stations/{}", base_url(), id))
                .send()
                .await
                .expect("Delete station request failed");

            let status = resp.status();
            assert!(
                status == 204 || status == 404,
                "Expected 204 or 404, got {}",
                status
            );
        } else {
            eprintln!("No stations available — skipping test_delete_station");
        }
    }

    #[tokio::test]
    async fn test_delete_station_returns_404_for_unknown() {
        let client = Client::new();
        let resp = client
            .delete(format!(
                "{}/api/v1/stations/STA-nonexistent00000000000000",
                base_url()
            ))
            .send()
            .await
            .expect("Delete station request failed");

        assert_eq!(resp.status(), 404);

        let body: Value = resp.json().await.expect("Response is valid JSON");
        assert_eq!(body["error"]["code"], "NOT_FOUND");
    }

    #[tokio::test]
    async fn test_ingest_event_returns_201() {
        let client = Client::new();
        let body = json!({
            "event_type": "station_detail_view",
            "session_id": fake_uuid(),
            "occurred_at": "2026-06-12T10:00:00",
            "payload": { "station_id": "STA-test0000000000000000001" }
        });

        let resp = client
            .post(format!("{}/api/v1/events", base_url()))
            .json(&body)
            .send()
            .await
            .expect("Ingest event request failed");

        assert_eq!(resp.status(), 201);

        let event: Value = resp.json().await.expect("Response is valid JSON");
        assert!(event["id"].is_number());
        assert_eq!(event["event_type"], "station_detail_view");
        assert!(event["ingested_at"].is_string());
    }

    #[tokio::test]
    async fn test_ingest_event_validates_required_fields() {
        let client = Client::new();

        // Empty event_type
        let resp = client
            .post(format!("{}/api/v1/events", base_url()))
            .json(&json!({
                "event_type": "",
                "session_id": fake_uuid(),
                "occurred_at": "2026-06-12T10:00:00"
            }))
            .send()
            .await
            .expect("Ingest event request failed");

        assert_eq!(resp.status(), 400);

        // Empty session_id
        let resp = client
            .post(format!("{}/api/v1/events", base_url()))
            .json(&json!({
                "event_type": "search",
                "session_id": "",
                "occurred_at": "2026-06-12T10:00:00"
            }))
            .send()
            .await
            .expect("Ingest event request failed");

        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_ingest_batch_returns_201() {
        let client = Client::new();
        let session_id = fake_uuid();
        let events: Vec<Value> = (0..5)
            .map(|i| {
                json!({
                    "event_type": "search",
                    "session_id": session_id.clone(),
                    "occurred_at": "2026-06-12T10:00:00",
                    "payload": { "search_query": format!("test {}", i), "result_count": 3 }
                })
            })
            .collect();

        let resp = client
            .post(format!("{}/api/v1/events/batch", base_url()))
            .json(&json!({ "events": events }))
            .send()
            .await
            .expect("Ingest batch request failed");

        assert_eq!(resp.status(), 201);

        let batch_resp: Value = resp.json().await.expect("Response is valid JSON");
        assert_eq!(batch_resp["ingested"], 5);
        assert!(batch_resp["message"].is_string());
    }

    #[tokio::test]
    async fn test_ingest_batch_rejects_empty() {
        let client = Client::new();
        let resp = client
            .post(format!("{}/api/v1/events/batch", base_url()))
            .json(&json!({ "events": [] }))
            .send()
            .await
            .expect("Ingest batch request failed");

        assert_eq!(resp.status(), 400);
    }

    #[tokio::test]
    async fn test_ingest_batch_rejects_over_100() {
        let client = Client::new();
        let events: Vec<Value> = (0..101)
            .map(|_| {
                json!({
                    "event_type": "search",
                    "session_id": fake_uuid(),
                    "occurred_at": "2026-06-12T10:00:00"
                })
            })
            .collect();

        let resp = client
            .post(format!("{}/api/v1/events/batch", base_url()))
            .json(&json!({ "events": events }))
            .send()
            .await
            .expect("Ingest batch request failed");

        assert_eq!(resp.status(), 400);
    }
}

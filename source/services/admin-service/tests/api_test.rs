use std::sync::OnceLock;
use tokio::runtime::Runtime;

struct ServerCtx {
    url: String,
    _rt: Runtime,
}

static SERVER: OnceLock<ServerCtx> = OnceLock::new();

fn server_url() -> &'static str {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    let ctx = SERVER.get_or_init(|| {
        let rt = Runtime::new().expect("server rt");
        let url = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().unwrap().port();
            let url = format!("http://127.0.0.1:{}", port);

            let db = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://bornemap:bornemap@localhost:5432/bornemap".into());
            let pool = admin_service::infrastructure::db::init_pool(&db).await.unwrap();
            let pr = admin_service::infrastructure::repository::PartnerRepository::new(pool.clone());
            let sr = admin_service::infrastructure::repository::StationRepository::new(pool.clone());
            let cr = admin_service::infrastructure::repository::ChargerRepository::new(pool);
            let app = admin_service::presentation::routes::create_router(pr, sr, cr);

            tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            url
        });
        ServerCtx { url, _rt: rt }
    });
    &ctx.url
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::new()
}

#[test]
fn it_001_health_endpoint() {
    let resp = client()
        .get(format!("{}/api/v1/health", server_url()))
        .send()
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["service"], "admin-service");
}

#[test]
fn it_002_partner_crud_lifecycle() {
    let c = client();
    let base = server_url();

    let create = c
        .post(format!("{}/api/v1/partners", base))
        .json(&serde_json::json!({
            "name": "Test Partner",
            "partner_type": "COMPANY",
            "support_phone": "+216123456",
            "support_email": "test@example.com"
        }))
        .send()
        .unwrap();
    assert_eq!(create.status(), 201);
    let partner: serde_json::Value = create.json().unwrap();
    let pid = partner["partner_id"].as_str().unwrap().to_string();
    assert!(pid.starts_with("OPR-"));
    assert_eq!(partner["name"], "Test Partner");

    let get = c.get(format!("{}/api/v1/partners/{}", base, pid)).send().unwrap();
    assert_eq!(get.status(), 200);

    let upd = c
        .put(format!("{}/api/v1/partners/{}", base, pid))
        .json(&serde_json::json!({"name": "Updated Partner"}))
        .send()
        .unwrap();
    assert_eq!(upd.status(), 200);

    let list = c.get(format!("{}/api/v1/partners", base)).send().unwrap();
    assert_eq!(list.status(), 200);
    let lb: serde_json::Value = list.json().unwrap();
    assert!(lb["data"].as_array().unwrap().len() >= 1);

    let del = c.delete(format!("{}/api/v1/partners/{}", base, pid)).send().unwrap();
    assert_eq!(del.status(), 204);

    let gd = c.get(format!("{}/api/v1/partners/{}", base, pid)).send().unwrap();
    assert_eq!(gd.status(), 404);
}

#[test]
fn it_003_station_crud_with_partner_fk() {
    let c = client();
    let base = server_url();

    let pr = c
        .post(format!("{}/api/v1/partners", base))
        .json(&serde_json::json!({"name": "Station Partner", "partner_type": "INDIVIDUAL"}))
        .send()
        .unwrap();
    let p: serde_json::Value = pr.json().unwrap();
    let pid = p["partner_id"].as_str().unwrap();

    let create = c
        .post(format!("{}/api/v1/stations", base))
        .json(&serde_json::json!({
            "name": "Test Station",
            "lat": 36.8, "lon": 10.1,
            "partner_id": pid, "address": "Tunis"
        }))
        .send()
        .unwrap();
    assert_eq!(create.status(), 201);
    let station: serde_json::Value = create.json().unwrap();
    let sid = station["station_id"].as_str().unwrap().to_string();
    assert!(sid.starts_with("STA-"));

    let get = c.get(format!("{}/api/v1/stations/{}", base, sid)).send().unwrap();
    assert_eq!(get.status(), 200);

    let upd = c
        .put(format!("{}/api/v1/stations/{}", base, sid))
        .json(&serde_json::json!({"name": "Updated Station"}))
        .send()
        .unwrap();
    assert_eq!(upd.status(), 200);

    let del = c.delete(format!("{}/api/v1/stations/{}", base, sid)).send().unwrap();
    assert_eq!(del.status(), 204);

    let gd = c.get(format!("{}/api/v1/stations/{}", base, sid)).send().unwrap();
    assert_eq!(gd.status(), 404);
}

#[test]
fn it_004_charger_crud_with_station_fk() {
    let c = client();
    let base = server_url();

    let sr = c
        .post(format!("{}/api/v1/stations", base))
        .json(&serde_json::json!({"name": "Charger Station", "lat": 36.8, "lon": 10.1}))
        .send()
        .unwrap();
    let s: serde_json::Value = sr.json().unwrap();
    let sid = s["station_id"].as_str().unwrap();

    let create = c
        .post(format!("{}/api/v1/chargers", base))
        .json(&serde_json::json!({
            "station_id": sid,
            "connector_type_id": 1, "status_id": 1, "current_type_id": 1,
            "power_kw": 50.0, "count_available": 2, "count_total": 4
        }))
        .send()
        .unwrap();
    assert_eq!(create.status(), 201);
    let charger: serde_json::Value = create.json().unwrap();
    let cid = charger["charger_id"].as_str().unwrap().to_string();
    assert!(cid.starts_with("CHG-"));

    let get = c.get(format!("{}/api/v1/chargers/{}", base, cid)).send().unwrap();
    assert_eq!(get.status(), 200);

    let upd = c
        .put(format!("{}/api/v1/chargers/{}", base, cid))
        .json(&serde_json::json!({"power_kw": 100.0}))
        .send()
        .unwrap();
    assert_eq!(upd.status(), 200);

    let del = c.delete(format!("{}/api/v1/chargers/{}", base, cid)).send().unwrap();
    assert_eq!(del.status(), 204);

    let gd = c.get(format!("{}/api/v1/chargers/{}", base, cid)).send().unwrap();
    assert_eq!(gd.status(), 404);
}

#[test]
fn it_005_soft_delete_hides_from_list() {
    let c = client();
    let base = server_url();

    let pr = c
        .post(format!("{}/api/v1/partners", base))
        .json(&serde_json::json!({"name": "SoftDelete Test"}))
        .send()
        .unwrap();
    let p: serde_json::Value = pr.json().unwrap();
    let pid = p["partner_id"].as_str().unwrap();

    let lb: serde_json::Value = c
        .get(format!("{}/api/v1/partners?search=SoftDelete", base))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(lb["pagination"]["total"].as_i64().unwrap() >= 1);

    c.delete(format!("{}/api/v1/partners/{}", base, pid))
        .send()
        .unwrap();

    let la: serde_json::Value = c
        .get(format!("{}/api/v1/partners?search=SoftDelete", base))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(la["pagination"]["total"], 0);
}

#[test]
fn it_006_pagination_works() {
    let c = client();
    let base = server_url();

    for i in 0..5 {
        c.post(format!("{}/api/v1/partners", base))
            .json(&serde_json::json!({"name": format!("PageTest {}", i)}))
            .send()
            .unwrap();
    }

    let p1: serde_json::Value = c
        .get(format!("{}/api/v1/partners?page=1&per_page=2", base))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(p1["data"].as_array().unwrap().len(), 2);
    assert_eq!(p1["pagination"]["page"], 1);
    assert_eq!(p1["pagination"]["per_page"], 2);
    assert!(p1["pagination"]["total_pages"].as_i64().unwrap() >= 3);
}

#[test]
fn it_007_validation_rejects_bad_input() {
    let c = client();
    let base = server_url();

    let bt = c
        .post(format!("{}/api/v1/partners", base))
        .json(&serde_json::json!({"name": "Bad", "partner_type": "INVALID"}))
        .send()
        .unwrap();
    assert_eq!(bt.status(), 400);

    let en = c
        .post(format!("{}/api/v1/partners", base))
        .json(&serde_json::json!({"name": ""}))
        .send()
        .unwrap();
    assert_eq!(en.status(), 400);

    let bl = c
        .post(format!("{}/api/v1/stations", base))
        .json(&serde_json::json!({"name": "Bad", "lat": 100.0, "lon": 10.0}))
        .send()
        .unwrap();
    assert_eq!(bl.status(), 400);

    let bl2 = c
        .post(format!("{}/api/v1/stations", base))
        .json(&serde_json::json!({"name": "Bad2", "lat": 36.0, "lon": 200.0}))
        .send()
        .unwrap();
    assert_eq!(bl2.status(), 400);

    let bc = c
        .post(format!("{}/api/v1/chargers", base))
        .json(&serde_json::json!({
            "station_id": "STA-nonexistent",
            "connector_type_id": 1, "status_id": 1, "current_type_id": 1,
            "count_available": -1, "count_total": 1
        }))
        .send()
        .unwrap();
    assert_eq!(bc.status(), 400);

    let bc2 = c
        .post(format!("{}/api/v1/chargers", base))
        .json(&serde_json::json!({
            "station_id": "STA-nonexistent",
            "connector_type_id": 1, "status_id": 1, "current_type_id": 1,
            "count_available": 5, "count_total": 1
        }))
        .send()
        .unwrap();
    assert_eq!(bc2.status(), 400);
}

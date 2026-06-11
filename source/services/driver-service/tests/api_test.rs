use actix_web::{test, web, App};
use sqlx::PgPool;
use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};

use driver_service::api;
use driver_service::config;
use driver_service::handlers;

struct TestContext {
    pool: PgPool,
    _container: ContainerAsync<GenericImage>,
}

impl TestContext {
    async fn new() -> Self {
        let container = GenericImage::new("postgis/postgis", "16-3.4")
            .with_env_var("POSTGRES_DB", "platform_db")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Failed to start PostGIS container");

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();

        let database_url = format!("postgres://postgres:postgres@{}:{}/platform_db", host, port);

        let pool = loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            match PgPool::connect(&database_url).await {
                Ok(p) => {
                    let row: Result<(String,), _> = sqlx::query_as("SELECT PostGIS_Version()")
                        .fetch_one(&p)
                        .await;
                    if row.is_ok() {
                        break p;
                    }
                }
                Err(_) => continue,
            }
        };

        let _ = borne_data::migrate(&pool).await;

        Self {
            pool,
            _container: container,
        }
    }

    fn app(
        &self,
    ) -> actix_web::App<
        impl actix_web::dev::ServiceFactory<
            actix_web::dev::ServiceRequest,
            Config = (),
            Response = actix_web::dev::ServiceResponse,
            Error = actix_web::Error,
            InitError = (),
        >,
    > {
        App::new()
            .app_data(web::Data::new(self.pool.clone()))
            .wrap(actix_web::middleware::Logger::default())
            .service(
                web::scope("/api/v1")
                    .service(web::scope("/stations").configure(api::v1::stations::configure))
                    .service(web::scope("/health").configure(api::v1::health::configure)),
            )
    }
}

#[actix_web::test]
async fn health_returns_ok() {
    let ctx = TestContext::new().await;
    let app = test::init_service(ctx.app()).await;
    let req = test::TestRequest::get().uri("/api/v1/health").to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["data"]["status"], "ok");
    assert_eq!(body["data"]["database"], "connected");
}

#[actix_web::test]
async fn list_stations_returns_array() {
    let ctx = TestContext::new().await;
    let app = test::init_service(ctx.app()).await;
    let req = test::TestRequest::get()
        .uri("/api/v1/stations")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    let body: serde_json::Value = test::read_body_json(resp).await;
    assert!(body["data"].is_array());
    assert!(body["meta"]["count"].is_number());
}

#[actix_web::test]
async fn nearby_validates_query_params() {
    let ctx = TestContext::new().await;
    let app = test::init_service(ctx.app()).await;
    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nearby?lat=999&lng=0&radius_m=1000")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 422);
    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"]["code"], "validation_error");
}

#[actix_web::test]
async fn station_detail_not_found() {
    let ctx = TestContext::new().await;
    let app = test::init_service(ctx.app()).await;
    let req = test::TestRequest::get()
        .uri("/api/v1/stations/nonexistent")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"]["code"], "not_found");
}

#[actix_web::test]
async fn concurrent_requests_all_succeed() {
    let ctx = TestContext::new().await;
    let app = test::init_service(ctx.app()).await;
    let app = std::sync::Arc::new(app);

    let mut handles = Vec::new();
    for _ in 0..100 {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let req = test::TestRequest::get().uri("/api/v1/health").to_request();
            let resp = test::call_service(&app, req).await;
            resp.status().is_success()
        }));
    }

    for h in handles {
        let ok = h.await.unwrap();
        assert!(ok, "concurrent request failed");
    }
}

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use serde::Serialize;
use std::env;

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    service: String,
}

#[get("/health/live")]
async fn live() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "alive".to_string(),
        service: "core-service".to_string(),
    })
}

#[get("/health/ready")]
async fn ready() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse {
        status: "ready".to_string(),
        service: "core-service".to_string(),
    })
}

fn check_prerequisites() {
    for tool in &["cargo", "rustc"] {
        if which::which(tool).is_err() {
            log::warn!("{} not found on PATH — some workflows may fail", tool);
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    )
    .format(|buf, record| {
        use std::io::Write;
        let ts = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        let msg = serde_json::json!({
            "timestamp": ts,
            "level": record.level().to_string(),
            "message": record.args().to_string(),
            "service": "core-service"
        });
        writeln!(buf, "{}", msg)
    })
    .init();

    check_prerequisites();

    let port: u16 = env::var("API_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .map_err(|e| {
            log::error!("Invalid API_PORT: {}", e);
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
        })?;

    log::info!("Starting core-service on 0.0.0.0:{}", port);

    HttpServer::new(|| {
        App::new().service(web::scope("/api/v1").service(live).service(ready))
    })
    .bind(("0.0.0.0", port))?
    .shutdown_timeout(30)
    .run()
    .await
}

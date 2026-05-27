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

fn check_prerequisites() -> Result<(), String> {
    if env::var("API_PORT").is_err() {
        env::set_var("API_PORT", "8080");
    }
    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default()
            .default_filter_or("info"),
    )
    .format(|buf, record| {
        use std::io::Write;
        let ts = chrono::Local::now()
            .format("%Y-%m-%dT%H:%M:%S%.3f%z")
            .to_string();
        writeln!(
            buf,
            r#"{{"timestamp":"{}","level":"{}","message":"{}","service":"core-service"}}"#,
            ts,
            record.level(),
            record.args()
        )
    })
    .init();

    if let Err(e) = check_prerequisites() {
        log::error!("Prerequisite check failed: {}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
    }

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
    .run()
    .await
}

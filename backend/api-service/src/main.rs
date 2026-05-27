use actix_web::{web, App, HttpServer, middleware::Logger};
use parking_lot::RwLock;
use std::sync::Arc;

mod handlers;

pub struct AppState {
    pub stations: Arc<RwLock<Vec<domain::StationHub>>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let initial_data = handlers::locate::generate_mock_data();
    let state = web::Data::new(AppState {
        stations: Arc::new(RwLock::new(initial_data)),
    });

    let host = "0.0.0.0";
    let port = 8080;
    println!("api-service running on http://{}:{}", host, port);

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Logger::default())
            .service(
                web::scope("/api/v1")
                    .service(handlers::locate::get_nearby_stations)
            )
    })
    .bind((host, port))?
    .run()
    .await
}

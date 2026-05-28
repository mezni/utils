use actix_web::{web, App, HttpServer, middleware::Logger};
use parking_lot::RwLock;
use std::sync::Arc;

mod domains;

pub struct AppState {
    pub stations: Arc<RwLock<Vec<domains::locate::model::Station>>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let initial_data = domains::locate::model::generate_mock_data();
    let state = web::Data::new(AppState {
        stations: Arc::new(RwLock::new(initial_data)),
    });

    let host = "0.0.0.0";
    let port = 8080;
    println!("⚡ api-service online and listening on http://{}:{}", host, port);

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Logger::default())
            .service(
                web::scope("/api/v1")
                    .configure(domains::locate::init_routes)
            )
    })
    .bind((host, port))?
    .run()
    .await
}

use actix_web::{get, web, Responder, HttpResponse};
use crate::AppState;

#[get("/stations/nearby")]
pub async fn get_nearby_stations(state: web::Data<AppState>) -> impl Responder {
    let data = state.stations.read();
    HttpResponse::Ok().json(&*data)
}

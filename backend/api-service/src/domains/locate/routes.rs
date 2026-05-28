use actix_web::{get, web, Responder, HttpResponse};
use crate::AppState;

#[get("/stations/nearby")]
pub async fn get_nearby_stations(state: web::Data<AppState>) -> impl Responder {
    let data = match state.stations.try_read() {
        Some(guard) => guard,
        None => return HttpResponse::InternalServerError().json("{\"error\":\"lock poisoned\"}"),
    };
    HttpResponse::Ok().json(&*data)
}

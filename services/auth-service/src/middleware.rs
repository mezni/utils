use actix_web::{Error, FromRequest, HttpRequest, dev::Payload};
use futures::future::{Ready, ready};
use uuid::Uuid;

pub struct RequestId(pub String);

impl FromRequest for RequestId {
    type Error = Error;
    type Future = Ready<Result<Self, Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // Get or generate request ID
        let request_id = req
            .headers()
            .get("X-Request-ID")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        ready(Ok(RequestId(request_id)))
    }
}

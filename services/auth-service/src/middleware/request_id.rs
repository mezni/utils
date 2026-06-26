use actix_web::{dev::Payload, Error, FromRequest, HttpRequest};
use futures::future::{ready, Ready};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }
    
    pub fn from_header(header_value: &str) -> Option<Self> {
        if !header_value.is_empty() {
            Some(Self(header_value.to_string()))
        } else {
            None
        }
    }
    
    pub fn as_header_value(&self) -> &str {
        &self.0
    }
}

impl FromRequest for RequestId {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        // Check for existing request ID in headers
        if let Some(header_value) = req.headers().get("X-Request-ID") {
            if let Ok(value) = header_value.to_str() {
                if let Some(request_id) = RequestId::from_header(value) {
                    return ready(Ok(request_id));
                }
            }
        }
        
        // Generate new request ID
        ready(Ok(RequestId::generate()))
    }
}

pub fn extract_request_id(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get("X-Request-ID")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

pub fn set_request_id_header(response: &mut actix_web::HttpResponse, request_id: &str) {
    response.headers_mut().insert(
        "X-Request-ID",
        actix_web::http::HeaderValue::from_str(request_id).unwrap(),
    );
}
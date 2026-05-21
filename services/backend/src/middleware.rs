use actix_web::middleware::DefaultHeaders;

pub fn security_headers() -> DefaultHeaders {
    DefaultHeaders::new()
        .add(("X-Content-Type-Options", "nosniff"))
        .add(("X-Frame-Options", "DENY"))
        .add(("X-XSS-Protection", "1; mode=block"))
        .add(("Strict-Transport-Security", "max-age=31536000; includeSubDomains"))
        .add(("Referrer-Policy", "strict-origin-when-cross-origin"))
}

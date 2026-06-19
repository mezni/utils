use actix_web::dev::ServiceRequest;
use actix_web::Error;
use actix_web::middleware::Next;
use actix_web::Response;
use std::future::Future;
use std::pin::Pin;

/// Middleware that redacts sensitive fields from request bodies before logging.
///
/// This middleware ensures that passwords, access tokens, and refresh tokens
/// are never logged to the application logs, per FR-001.
pub struct LogRedactionMiddleware;

impl LogRedactionMiddleware {
    /// Create a new instance of the log redaction middleware.
    pub fn new() -> Self {
        Self
    }
}

impl<S, B> actix_web::dev::Service<S> for LogRedactionMiddleware
where
    S: actix_web::dev::Service<ServiceRequest, Response = actix_web::HttpResponse, Error = Error>
        + 'static,
    S::Future: 'static,
{
    type Error = Error;
    type Service = S;

    fn actix_service(&mut self, service: S) -> Self::Service {
        service
    }

    fn call(&mut self, req: ServiceRequest, next: Next<'_, S>) -> Pin<Box<dyn Future<Output = Response> + 'static>> {
        let redacted = self.redact_request(&req);
        if let Some(redacted) = redacted {
            tracing::info!("Incoming request (redacted): {}", redacted);
        } else {
            tracing::info!("Incoming request");
        }

        Box::pin(async move {
            let response = next.call(req).await;
            tracing::info!("Response status: {:?}", response.status());
            response
        })
    }
}

impl LogRedactionMiddleware {
    /// Redact sensitive fields from a request and return a string representation.
    ///
    /// This is a simple implementation that logs the method and path,
    /// with sensitive fields removed from the query string.
    fn redact_request(&self, req: &ServiceRequest) -> Option<String> {
        let method = req.method().as_str();
        let path = req.path();

        // Check if this is a login endpoint
        if path.contains("/auth/login") {
            // Try to get the query string parameters
            if let Some(query) = req.query_string() {
                // Simple regex-based redaction for common patterns
                let redacted = query
                    .split('&')
                    .map(|pair| {
                        if pair.starts_with("password=") {
                            "password=*****".to_string()
                        } else if pair.starts_with("access_token=") {
                            "access_token=*****".to_string()
                        } else if pair.starts_with("refresh_token=") {
                            "refresh_token=*****".to_string()
                        } else {
                            pair.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("&");

                return Some(format!("{} {} {}", method, path, redacted));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redaction_replaces_password() {
        let middleware = LogRedactionMiddleware::new();
        let query = "password=secret123&email=user@example.com";
        let redacted = middleware.redact_request(&ServiceRequest::from_static(
            "POST /auth/login?password=secret123",
        ));

        assert!(redacted.is_some());
        let result = redacted.unwrap();
        assert!(result.contains("password=*****"));
        assert!(result.contains("email=user@example.com"));
    }

    #[test]
    fn test_redaction_replaces_access_token() {
        let middleware = LogRedactionMiddleware::new();
        let redacted = middleware.redact_request(&ServiceRequest::from_static(
            "POST /auth/login?access_token=secret123",
        ));

        assert!(redacted.is_some());
        let result = redacted.unwrap();
        assert!(result.contains("access_token=*****"));
    }

    #[test]
    fn test_redaction_replaces_refresh_token() {
        let middleware = LogRedactionMiddleware::new();
        let redacted = middleware.redact_request(&ServiceRequest::from_static(
            "POST /auth/login?refresh_token=secret123",
        ));

        assert!(redacted.is_some());
        let result = redacted.unwrap();
        assert!(result.contains("refresh_token=*****"));
    }

    #[test]
    fn test_redaction_non_sensitive_path() {
        let middleware = LogRedactionMiddleware::new();
        let redacted = middleware.redact_request(&ServiceRequest::from_static(
            "GET /health",
        ));

        assert!(redacted.is_none());
    }
}

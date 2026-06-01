pub const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn common_observability() -> &'static str {
    "common-observability"
}

use actix_governor::governor;
use actix_governor::{Governor, GovernorConfigBuilder, PeerIpKeyExtractor};

pub fn rate_limiter(
) -> Governor<PeerIpKeyExtractor, governor::middleware::NoOpMiddleware<governor::clock::QuantaInstant>>
{
    let burst_size: u32 = std::env::var("RATE_LIMIT_BURST_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let config = GovernorConfigBuilder::default()
        .seconds_per_request(burst_size as u64)
        .burst_size(burst_size)
        .finish()
        .expect("Failed to build rate limiter config");

    Governor::new(&config)
}

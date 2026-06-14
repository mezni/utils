use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_platform_subscriber(service_name: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("info,{}=debug", service_name)));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().json().with_current_span(true))
        .init();
}

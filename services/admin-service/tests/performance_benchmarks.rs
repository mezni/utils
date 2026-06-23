//! Performance benchmarks for analytics API endpoints
//! Tests for User Story 1: Admin Dashboard

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use std::time::Duration;

use admin_service::api::analytics::{AppState, get_summary, get_station_analytics, get_user_activity, get_search_trends};
use admin_service::services::CacheService;

#[cfg(test)]
mod bench {
    use super::*;

    /// Create a mock AppState for benchmarking
    fn create_mock_state() -> Arc<AppState> {
        // In a real benchmark, you would create proper connections
        // For now, we'll use a mock
        let cache_service = Arc::new(CacheService::new(
            admin_service::services::CacheConfig {
                redis_url: "redis://127.0.0.1:6379".to_string(),
                default_ttl_seconds: 60,
                max_connections: 10,
            },
        ));

        Arc::new(AppState {
            kpi_engine: Arc::new(admin_service::services::KPIAggregationEngine::new(
                admin_service::services::KPIConfig::default(),
                web::Data::new(()).into_inner(),
                cache_service.clone(),
            )),
            db_pool: Arc::new(web::Data::new(()).into_inner()),
            cache_service,
        })
    }

    fn bench_get_summary(c: &mut Criterion) {
        let mut group = c.benchmark_group("get_summary");
        group.sample_size(100);

        let app_state = create_mock_state();

        for size in [100, 500, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
                b.iter(|| {
                    // In a real benchmark, we would properly set up the state
                    // For now, we're just demonstrating the benchmark structure
                    let _ = get_summary(
                        black_box(app_state.clone()),
                        black_box(admin_service::api::analytics::AnalyticsQuery::default()),
                        black_box(admin_service::middleware::AuthUser {
                            user_uuid: "test-uuid".to_string(),
                            username: "testuser".to_string(),
                            email: Some("test@example.com".to_string()),
                            role: "admin".to_string(),
                        }),
                    );
                });
            });
        }

        group.finish();
    }

    fn bench_get_station_analytics(c: &mut Criterion) {
        let mut group = c.benchmark_group("get_station_analytics");
        group.sample_size(100);

        let app_state = create_mock_state();

        for size in [100, 500, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
                b.iter(|| {
                    // In a real benchmark, we would properly set up the state
                    // For now, we're just demonstrating the benchmark structure
                    let _ = get_station_analytics(
                        black_box("STA-123456".to_string()),
                        black_box(app_state.clone()),
                        black_box(admin_service::api::analytics::StationAnalyticsQuery::default()),
                        black_box(admin_service::middleware::AuthUser {
                            user_uuid: "test-uuid".to_string(),
                            username: "testuser".to_string(),
                            email: Some("test@example.com".to_string()),
                            role: "admin".to_string(),
                        }),
                    );
                });
            });
        }

        group.finish();
    }

    fn bench_get_user_activity(c: &mut Criterion) {
        let mut group = c.benchmark_group("get_user_activity");
        group.sample_size(100);

        let app_state = create_mock_state();

        for size in [100, 500, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
                b.iter(|| {
                    // In a real benchmark, we would properly set up the state
                    // For now, we're just demonstrating the benchmark structure
                    let _ = get_user_activity(
                        black_box("550e8400-e29b-41d4-a716-446655440000".to_string()),
                        black_box(app_state.clone()),
                        black_box(admin_service::middleware::AuthUser {
                            user_uuid: "test-uuid".to_string(),
                            username: "testuser".to_string(),
                            email: Some("test@example.com".to_string()),
                            role: "admin".to_string(),
                        }),
                    );
                });
            });
        }

        group.finish();
    }

    fn bench_get_search_trends(c: &mut Criterion) {
        let mut group = c.benchmark_group("get_search_trends");
        group.sample_size(100);

        let app_state = create_mock_state();

        for size in [100, 500, 1000].iter() {
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
                b.iter(|| {
                    // In a real benchmark, we would properly set up the state
                    // For now, we're just demonstrating the benchmark structure
                    let _ = get_search_trends(
                        black_box(admin_service::api::analytics::AnalyticsQuery::default()),
                        black_box(app_state.clone()),
                        black_box(admin_service::middleware::AuthUser {
                            user_uuid: "test-uuid".to_string(),
                            username: "testuser".to_string(),
                            email: Some("test@example.com".to_string()),
                            role: "admin".to_string(),
                        }),
                    );
                });
            });
        }

        group.finish();
    }

    fn bench_cache_operations(c: &mut Criterion) {
        let mut group = c.benchmark_group("cache_operations");
        group.sample_size(100);

        let cache_config = admin_service::services::CacheConfig {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            default_ttl_seconds: 60,
            max_connections: 10,
        };

        // Benchmark set operations
        group.bench_function("cache_set", |b| {
            b.iter(|| {
                let cache_service = CacheService::new(cache_config.clone()).expect("Failed to create cache service");
                cache_service.set(black_box("test_key"), black_box("test_value"), black_box(Some(60)))
                    .expect("Failed to set value");
            });
        });

        // Benchmark get operations
        group.bench_function("cache_get", |b| {
            b.iter(|| {
                let cache_service = CacheService::new(cache_config.clone()).expect("Failed to create cache service");
                let _ = cache_service.get::<String>(black_box("test_key"));
            });
        });

        // Benchmark delete operations
        group.bench_function("cache_delete", |b| {
            b.iter(|| {
                let cache_service = CacheService::new(cache_config.clone()).expect("Failed to create cache service");
                cache_service.delete(black_box("test_key")).expect("Failed to delete key");
            });
        });

        group.finish();
    }

    fn bench_kpi_aggregation(c: &mut Criterion) {
        let mut group = c.benchmark_group("kpi_aggregation");
        group.sample_size(100);

        let cache_config = admin_service::services::CacheConfig {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            default_ttl_seconds: 60,
            max_connections: 10,
        };

        let cache_service = Arc::new(CacheService::new(cache_config).expect("Failed to create cache service"));

        group.bench_function("calculate_all_kpis", |b| {
            b.iter(|| {
                let kpi_config = admin_service::services::KPIConfig::default();
                let kpi_engine = admin_service::services::KPIAggregationEngine::new(
                    kpi_config,
                    web::Data::new(()).into_inner(),
                    cache_service.clone(),
                );
                let _ = kpi_engine.calculate_all_kpis();
            });
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench::bench_get_summary,
    bench::bench_get_station_analytics,
    bench::bench_get_user_activity,
    bench::bench_get_search_trends,
    bench::bench_cache_operations,
    bench::bench_kpi_aggregation,
);

criterion_main!(benches);

pub mod service_topology;
pub mod schema_isolation;
pub mod naming;
pub mod openapi_first;
pub mod sqlx_safety;
pub mod frontend_boundary;
pub mod migration_validation;

pub trait Rule {
    fn name(&self) -> &'static str;
    fn check(&self, path: &str, content: &str) -> Vec<String>;
}

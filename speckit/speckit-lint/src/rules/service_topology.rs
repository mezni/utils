use super::Rule;

pub struct ServiceTopologyRule;

impl Rule for ServiceTopologyRule {
    fn name(&self) -> &'static str {
        "service_topology"
    }

    fn check(&self, path: &str, _content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.ends_with("Cargo.toml") {
            if let Some(parent) = path.rsplit('/').nth(1) {
                if parent == "admin-service" {
                    if let Some(grandparent) = path.rsplit('/').nth(2) {
                        if grandparent != "services" {
                            violations.push(format!(
                                "Admin service must be under services/ directory: {path}"
                            ));
                        }
                    }
                }
            }
        }

        if path.contains("infrastructure") && path.ends_with(".yml") {
            if !path.contains("traefik") && !path.contains("docker") {
                violations.push(format!(
                    "Infrastructure files must be under docker/ or traefik/: {path}"
                ));
            }
        }

        violations
    }
}

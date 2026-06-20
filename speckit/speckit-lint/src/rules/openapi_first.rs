use super::Rule;

pub struct OpenapiFirstRule;

impl Rule for OpenapiFirstRule {
    fn name(&self) -> &'static str {
        "openapi_first"
    }

    fn check(&self, path: &str, _content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.starts_with("services/")
            && path.ends_with(".rs")
            && path.contains("routes/")
        {
            let spec_path = "api/openapi/admin.yaml";
            if !std::path::Path::new(spec_path).exists() {
                violations.push(format!(
                    "Route handler exists but OpenAPI spec is missing at {spec_path}"
                ));
            }
        }

        violations
    }
}

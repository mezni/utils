use super::Rule;

pub struct FrontendBoundaryRule;

impl Rule for FrontendBoundaryRule {
    fn name(&self) -> &'static str {
        "frontend_boundary"
    }

    fn check(&self, path: &str, content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.starts_with("apps/dashboard/") && path.ends_with(".ts") || path.ends_with(".tsx") {
            if content.contains("fetch(")
                || content.contains("axios")
                || content.contains("XMLHttpRequest")
            {
                violations.push(format!(
                    "Direct HTTP calls detected in dashboard. Use generated OpenAPI client instead: {path}"
                ));
            }
        }

        violations
    }
}

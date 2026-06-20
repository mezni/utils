use super::Rule;

pub struct SchemaIsolationRule;

impl Rule for SchemaIsolationRule {
    fn name(&self) -> &'static str {
        "schema_isolation"
    }

    fn check(&self, path: &str, content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.ends_with(".sql") && path.contains("migrations") {
            if content.contains("gis.") || content.contains("users.") {
                violations.push(format!(
                    "Migration references out-of-scope schema: {path}"
                ));
            }
            if !content.contains("inventory.")
                && content.to_uppercase().contains("CREATE TABLE")
            {
                violations.push(format!(
                    "Migration must use inventory schema: {path}"
                ));
            }
        }

        violations
    }
}

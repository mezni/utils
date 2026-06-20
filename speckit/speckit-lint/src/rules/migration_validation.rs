use super::Rule;

pub struct MigrationValidationRule;

impl Rule for MigrationValidationRule {
    fn name(&self) -> &'static str {
        "migration_validation"
    }

    fn check(&self, path: &str, content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.ends_with(".sql") && path.contains("migrations") {
            if content.to_uppercase().contains("DROP TABLE")
                || content.to_uppercase().contains("DROP SCHEMA")
            {
                violations.push(format!(
                    "Destructive DDL detected in migration: {path}"
                ));
            }
        }

        violations
    }
}

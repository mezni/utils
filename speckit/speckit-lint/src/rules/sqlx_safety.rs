use super::Rule;

pub struct SqlxSafetyRule;

impl Rule for SqlxSafetyRule {
    fn name(&self) -> &'static str {
        "sqlx_safety"
    }

    fn check(&self, path: &str, content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.ends_with(".rs") && !path.contains("migrations") {
            for line in content.lines() {
                let trimmed = line.trim();
                if !trimmed.starts_with("//")
                    && !trimmed.starts_with("/*")
                    && !trimmed.starts_with("*")
                {
                    if let Some(idx) = trimmed.find("sqlx::query") {
                        let after = &trimmed[idx..];
                        if after.starts_with("sqlx::query(") {
                            violations.push(format!(
                                "Raw SQL string detected via sqlx::query!(). Use sqlx::query_as!() or compile-time macros: {path}"
                            ));
                        }
                    }
                }
            }
        }

        violations
    }
}

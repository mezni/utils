use super::Rule;

pub struct NamingRule;

impl Rule for NamingRule {
    fn name(&self) -> &'static str {
        "naming"
    }

    fn check(&self, path: &str, content: &str) -> Vec<String> {
        let mut violations = Vec::new();

        if path.ends_with(".sql") && path.contains("migrations") {
            if !content.contains("CHECK (id ~") {
                if content.to_uppercase().contains("CREATE TABLE") {
                    let lower = content.to_lowercase();
                    if lower.contains("partner") || lower.contains("station") || lower.contains("charger") {
                        if !lower.contains("opr-") && !lower.contains("sta-") && !lower.contains("chg-") {
                            violations.push(format!(
                                "Entity table missing nanoid CHECK constraint: {path}"
                            ));
                        }
                    }
                }
            }
        }

        violations
    }
}

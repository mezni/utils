//! Query safety validator for admin-service
//! Ensures no dynamic SQL or SQL injection vulnerabilities

use anyhow::Result;
use std::path::Path;
use std::sync::Mutex;
use walkdir::WalkDir;

/// Query safety validator
pub struct QuerySafetyValidator {
    dangerous_patterns: Mutex<Vec<String>>,
    sqlx_macros: Mutex<Vec<String>>,
}

impl QuerySafetyValidator {
    pub fn new() -> Self {
        Self {
            dangerous_patterns: Mutex::new(vec![
                "CONCAT(".to_string(), // String concatenation
                "+ ".to_string(),      // String concatenation with +
                "|| ".to_string(),     // PostgreSQL concatenation
                "format!".to_string(), // String formatting with parameters
                "printf!".to_string(), // Printf-style formatting
            ]),
            sqlx_macros: Mutex::new(vec![
                "sqlx::query!".to_string(),
                "sqlx::query_as!".to_string(),
                "sqlx::query_scalar!".to_string(),
                "sqlx::query!".to_string(),
                "sqlx::query!".to_string(),
            ]),
        }
    }

    /// Scan directory for dynamic SQL patterns
    ///
    /// # Arguments
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    /// True if no dynamic SQL found, False otherwise
    pub fn scan_for_dynamic_sql(&self, dir: &Path) -> Result<bool> {
        let mut dynamic_sql_found = false;

        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "rs" {
                        match self.scan_file(&path) {
                            Ok(found) => {
                                if found {
                                    dynamic_sql_found = true;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error scanning file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
        }

        if dynamic_sql_found {
            eprintln!("❌ Dynamic SQL patterns found in admin-service:");
            return Ok(false);
        }

        println!("✅ No dynamic SQL patterns found in admin-service");
        Ok(true)
    }

    /// Scan a single file for dynamic SQL
    fn scan_file(&self, path: &Path) -> Result<bool> {
        let content = std::fs::read_to_string(path)?;

        let dangerous_patterns = self.dangerous_patterns.lock().unwrap();
        let sqlx_macros = self.sqlx_macros.lock().unwrap();

        for pattern in &*dangerous_patterns {
            if content.contains(pattern) {
                // Check if this is in a comment or string
                if !self.is_in_string_or_comment(&content, pattern) {
                    eprintln!("   Found dangerous pattern: {}", pattern);
                    return Ok(true);
                }
            }
        }

        // Verify all SQL queries use sqlx macros
        if !self.all_sql_uses_macros(&content, &sqlx_macros) {
            eprintln!("   Found SQL without sqlx macros");
            return Ok(true);
        }

        Ok(false)
    }

    /// Check if a pattern is in a string or comment
    fn is_in_string_or_comment(&self, content: &str, pattern: &str) -> bool {
        let mut in_string = false;
        let mut in_comment = false;

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue;
            }

            // Check for string literals (both single and double quotes)
            if let Some((before, after)) = line.split_once('"') {
                in_string = !in_string;
                continue;
            }

            // Check for single-line comments
            if trimmed.starts_with("//") {
                in_comment = true;
                continue;
            }

            // Check for multi-line comments
            if trimmed.starts_with("/*") {
                if let Some((_, after)) = trimmed.split_once("*/") {
                    in_comment = false;
                    line = after;
                    continue;
                }
                in_comment = true;
                continue;
            }

            // Skip if already in comment
            if in_comment {
                continue;
            }
        }

        in_comment || in_string
    }

    /// Verify all SQL queries use sqlx macros
    fn all_sql_uses_macros(&self, content: &str, sqlx_macros: &[String]) -> bool {
        let lines: Vec<&str> = content.lines().collect();

        for (i, line) in lines.iter().enumerate() {
            // Skip if in comment
            if line.trim().starts_with("//") || (i > 0 && lines[i - 1].trim().ends_with("/*")) {
                continue;
            }

            // Check if this is an SQL query
            if line.contains("SELECT") || line.contains("FROM") || line.contains("INSERT") {
                let line_lower = line.to_lowercase();

                // Check if it uses sqlx macros
                let uses_macro = sqlx_macros.iter().any(|macro_name| line.contains(macro_name));

                if !uses_macro {
                    eprintln!("   Line {}: SQL query without sqlx macro", i + 1);
                    return false;
                }
            }
        }

        true
    }

    /// Verify query safety for a specific file
    pub fn verify_query_safety(&self, path: &Path) -> Result<bool> {
        let has_dynamic_sql = self.scan_file(path)?;

        if !has_dynamic_sql {
            println!("✅ File {:?} is query-safe", path);
            Ok(true)
        } else {
            eprintln!("❌ File {:?} contains unsafe SQL patterns:", path);
            Ok(false)
        }
    }
}

impl Default for QuerySafetyValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_creation() {
        let validator = QuerySafetyValidator::new();
        assert!(validator.scan_for_dynamic_sql(Path::new("./tests/fixtures")).unwrap_or(false));
    }

    #[test]
    fn test_no_concat_in_readonly_file() {
        let validator = QuerySafetyValidator::new();
        let content = r#"
            // Safe query with sqlx macro
            let query = sqlx::query!("SELECT * FROM events WHERE user_id = $1", user_id);
        "#;

        assert!(!validator.is_in_string_or_comment(content, "CONCAT("));
    }

    #[test]
    fn test_detects_dynamic_sql() {
        let validator = QuerySafetyValidator::new();
        let content = r#"
            // Dangerous: Dynamic string concatenation
            let query = format!("SELECT * FROM events WHERE user_id = '{}'", user_id);
        "#;

        assert!(validator.is_in_string_or_comment(content, "CONCAT("));
        assert!(validator.is_in_string_or_comment(content, "format!"));
    }
}
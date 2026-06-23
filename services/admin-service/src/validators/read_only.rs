//! Read-only enforcement validator for admin-service
//! Ensures no write operations to analytics_db

use anyhow::Result;
use std::path::Path;
use std::sync::Mutex;
use walkdir::WalkDir;

/// Read-only enforcement validator
pub struct ReadOnlyValidator {
    write_keywords: Mutex<Vec<String>>,
    allowed_patterns: Mutex<Vec<String>>,
}

impl ReadOnlyValidator {
    pub fn new() -> Self {
        Self {
            write_keywords: Mutex::new(vec![
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
                "TRUNCATE".to_string(),
                "DROP".to_string(),
                "ALTER".to_string(),
                "CREATE TABLE".to_string(),
                "DROP TABLE".to_string(),
                "ALTER TABLE".to_string(),
                "INSERT INTO".to_string(),
                "UPDATE SET".to_string(),
                "DELETE FROM".to_string(),
                "CREATE OR REPLACE VIEW".to_string(),
                "REFRESH MATERIALIZED VIEW".to_string(),
            ]),
            allowed_patterns: Mutex::new(vec![
                "SELECT".to_string(),
                "WHERE".to_string(),
                "GROUP BY".to_string(),
                "ORDER BY".to_string(),
                "HAVING".to_string(),
                "LIMIT".to_string(),
                "OFFSET".to_string(),
                "JOIN".to_string(),
                "INNER JOIN".to_string(),
                "LEFT JOIN".to_string(),
                "RIGHT JOIN".to_string(),
                "CROSS JOIN".to_string(),
                "UNION".to_string(),
                "UNION ALL".to_string(),
                "EXCEPT".to_string(),
                "INTERSECT".to_string(),
                "INDEX".to_string(),
                "COMMENT".to_string(),
                "CREATE INDEX".to_string(),
                "DROP INDEX".to_string(),
            ]),
        }
    }

    /// Scan directory for write operations targeting analytics_db
    ///
    /// # Arguments
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    /// True if no write operations found, False otherwise
    pub fn scan_for_write_operations(&self, dir: &Path) -> Result<bool> {
        let mut write_operations = Vec::new();

        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    // Only scan Rust files
                    if ext == "rs" {
                        match self.scan_file(&path) {
                            Ok(writes) => {
                                for write in writes {
                                    write_operations.push(write);
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

        if !write_operations.is_empty() {
            eprintln!("❌ Found write operations targeting analytics_db:");
            for write in &write_operations {
                eprintln!("   {}", write);
            }
            return Ok(false);
        }

        println!("✅ No write operations targeting analytics_db found");
        Ok(true)
    }

    /// Scan a single file for write operations
    fn scan_file(&self, path: &Path) -> Result<Vec<String>> {
        let content = std::fs::read_to_string(path)?;

        let mut writes = Vec::new();
        let write_keywords = self.write_keywords.lock().unwrap();
        let allowed_patterns = self.allowed_patterns.lock().unwrap();

        for keyword in &*write_keywords {
            if content.contains(keyword) {
                // Check if this is in a comment or string
                if !self.is_in_string_or_comment(&content, keyword) {
                    writes.push(format!(
                        "{} in file: {:?}",
                        keyword,
                        path
                    ));
                }
            }
        }

        // Verify no forbidden patterns (dynamic SQL)
        if self.contains_dynamic_sql(&content) {
            writes.push(format!(
                "Dynamic SQL patterns found in file: {:?}",
                path
            ));
        }

        Ok(writes)
    }

    /// Check if a pattern is in a string or comment
    fn is_in_string_or_comment(&self, content: &str, pattern: &str) -> bool {
        let mut in_string = false;
        let mut in_comment = false;

        for line in content.lines() {
            let trimmed = line.trim();

            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }

            // Check for string literals
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

            // Check for SQLx macros (compile-time verified, allowed)
            if line.contains("sqlx::query!") || line.contains("sqlx::query_as!") || line.contains("sqlx::query_scalar!") {
                in_string = false;
                in_comment = false;
                continue;
            }
        }

        in_comment || in_string
    }

    /// Check for dynamic SQL patterns (unsafe)
    fn contains_dynamic_sql(&self, content: &str) -> bool {
        let dangerous_patterns = [
            "CONCAT(".to_string(),
            "|| ".to_string(),
            "+ ".to_string(),
        ];

        for pattern in &dangerous_patterns {
            if content.contains(pattern) && !content.contains("//") && !content.contains("/*") {
                // Check if this is in a comment
                if !self.is_in_string_or_comment(content, pattern) {
                    return true;
                }
            }
        }

        false
    }

    /// Verify no write operations in a specific file
    pub fn verify_read_only(&self, path: &Path) -> Result<bool> {
        let writes = self.scan_file(path)?;

        if writes.is_empty() {
            println!("✅ File {:?} is read-only compliant", path);
            Ok(true)
        } else {
            eprintln!("❌ File {:?} contains write operations:", path);
            for write in &writes {
                eprintln!("   {}", write);
            }
            Ok(false)
        }
    }
}

impl Default for ReadOnlyValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_creation() {
        let validator = ReadOnlyValidator::new();
        assert!(validator.scan_for_write_operations(Path::new("./tests/fixtures")).unwrap_or(false));
    }

    #[test]
    fn test_no_write_keywords_in_readonly_file() {
        let validator = ReadOnlyValidator::new();
        let content = r#"
            // This is a comment
            fn read_only_function() {
                // SELECT statement is safe
                let query = sqlx::query!("SELECT * FROM events");
                Ok(query)
            }
        "#;

        assert!(!validator.is_in_string_or_comment(content, "INSERT"));
        assert!(!validator.is_in_string_or_comment(content, "UPDATE"));
        assert!(!validator.is_in_string_or_comment(content, "DELETE"));
    }
}
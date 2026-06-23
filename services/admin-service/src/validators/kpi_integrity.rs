//! KPI integrity validator for admin-service
//! Ensures all KPIs are derived from telemetry events only

use anyhow::Result;
use std::path::Path;
use std::sync::Mutex;
use walkdir::WalkDir;

/// KPI integrity validator
pub struct KPIIntegrityValidator {
    valid_kpi_sources: Mutex<Vec<String>>,
    allowed_external_sources: Mutex<Vec<String>>,
    invalid_patterns: Mutex<Vec<String>>,
}

impl KPIIntegrityValidator {
    pub fn new() -> Self {
        Self {
            valid_kpi_sources: Mutex::new(vec![
                "analytics_events".to_string(),
                "station_usage".to_string(),
                "user_activity".to_string(),
                "search_trends".to_string(),
                "materialized_view_meta".to_string(),
            ]),
            allowed_external_sources: Mutex::new(vec![
                "AnalyticsEvent".to_string(),
                "StationUsage".to_string(),
                "UserActivity".to_string(),
                "SearchTrend".to_string(),
            ]),
            invalid_patterns: Mutex::new(vec![
                "from_json".to_string(),
                "from_yaml".to_string(),
                "from_xml".to_string(),
                "from_csv".to_string(),
                "from_api".to_string(),
                "from_database_external".to_string(),
                "from_web_service".to_string(),
                "hardcoded_value".to_string(),
                "external_data".to_string(),
            ]),
        }
    }

    /// Scan for KPI calculations that use external data sources
    ///
    /// # Arguments
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    /// True if all KPIs are derived from events only, False otherwise
    pub fn scan_for_kpi_integrity_violations(&self, dir: &Path) -> Result<bool> {
        let mut violations = Vec::new();

        for entry in WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "rs" {
                        match self.scan_file(&path) {
                            Ok(found_violations) => {
                                violations.extend(found_violations);
                            }
                            Err(e) => {
                                eprintln!("Error scanning file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
        }

        if !violations.is_empty() {
            eprintln!("❌ KPI integrity violations found:");
            for violation in &violations {
                eprintln!("   {}", violation);
            }
            return Ok(false);
        }

        println!("✅ All KPIs derived from telemetry events only");
        Ok(true)
    }

    /// Scan a single file for KPI integrity violations
    fn scan_file(&self, path: &Path) -> Result<Vec<String>> {
        let content = std::fs::read_to_string(path)?;

        let valid_kpi_sources = self.valid_kpi_sources.lock().unwrap();
        let invalid_patterns = self.invalid_patterns.lock().unwrap();

        let mut violations = Vec::new();

        // Check for external data source patterns
        for pattern in &*invalid_patterns {
            if content.contains(pattern) {
                // Check if this is in a comment or string
                if !self.is_in_string_or_comment(&content, pattern) {
                    violations.push(format!(
                        "{} pattern found in file: {:?}",
                        pattern,
                        path
                    ));
                }
            }
        }

        // Verify all KPI calculations reference valid sources
        for source in &*valid_kpi_sources {
            if content.contains(source) && !content.contains(&format!("use {}", source)) {
                // Check if this is in a comment
                if !self.is_in_string_or_comment(&content, source) {
                    violations.push(format!(
                        "KPI calculation might use external data: {} in file: {:?}",
                        source,
                        path
                    ));
                }
            }
        }

        Ok(violations)
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

            // Skip if already in comment
            if in_comment {
                continue;
            }
        }

        in_comment || in_string
    }

    /// Verify KPI integrity for a specific file
    pub fn verify_kpi_integrity(&self, path: &Path) -> Result<bool> {
        let violations = self.scan_file(path)?;

        if violations.is_empty() {
            println!("✅ File {:?} is KPI integrity compliant", path);
            Ok(true)
        } else {
            eprintln!("❌ File {:?} contains KPI integrity violations:", path);
            for violation in &violations {
                eprintln!("   {}", violation);
            }
            Ok(false)
        }
    }
}

impl Default for KPIIntegrityValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_creation() {
        let validator = KPIIntegrityValidator::new();
        assert!(validator.scan_for_kpi_integrity_violations(Path::new("./tests/fixtures")).unwrap_or(false));
    }

    #[test]
    fn test_no_external_sources_in_safe_file() {
        let validator = KPIIntegrityValidator::new();
        let content = r#"
            // KPI calculation from events only
            fn calculate_station_views(events: &[AnalyticsEvent]) -> u64 {
                events.iter().filter(|e| e.event_type == "VIEW").count() as u64
            }
        "#;

        assert!(!validator.is_in_string_or_comment(content, "from_api"));
        assert!(!validator.is_in_string_or_comment(content, "hardcoded_value"));
    }

    #[test]
    fn test_detects_external_source_patterns() {
        let validator = KPIIntegrityValidator::new();
        let content = r#"
            // Dangerous: Fetch data from external API
            let data = fetch_from_api("https://api.example.com/kpis");
            let kpi = data.value;
        "#;

        assert!(validator.is_in_string_or_comment(content, "from_api"));
        assert!(validator.is_in_string_or_comment(content, "external_data"));
    }
}
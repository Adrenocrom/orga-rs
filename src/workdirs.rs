//! Working-directory config: the user-maintained list of absolute paths the
//! dashboard's workdir dropdown (and the `list_workdirs` MCP tool) offers.
//!
//! Format — a JSON array of absolute paths:
//!
//! ```json
//! ["/home/me/project-a", "/home/me/project-b"]
//! ```
//!
//! The file is re-read on every request, so editing it applies without a
//! restart. Error policy:
//!
//! - a **missing** file is an empty list — the feature is optional and a
//!   fresh install has no config yet;
//! - a **malformed** file is an error — silently hiding the configured
//!   dirs behind an empty dropdown would be confusing;
//! - **invalid entries** (relative paths, non-strings, blanks) are skipped
//!   with a log line — one bad line must not disable the rest.

use std::path::Path;

use crate::models::check_workdir;

/// Read and validate the workdirs config. See the module docs for the
/// format and the error policy.
pub fn load(path: &Path) -> Result<Vec<String>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(format!(
                "cannot read workdirs config {}: {e}",
                path.display()
            ))
        }
    };
    parse(&text, path)
}

/// Parse the config text. Split from [`load`] so the rules are testable
/// without touching the filesystem.
fn parse(text: &str, path: &Path) -> Result<Vec<String>, String> {
    let value: serde_json::Value = serde_json::from_str(text).map_err(|e| {
        format!("workdirs config {} is not valid JSON: {e}", path.display())
    })?;
    let Some(entries) = value.as_array() else {
        return Err(format!(
            "workdirs config {} must be a JSON array of absolute paths",
            path.display()
        ));
    };
    let mut dirs: Vec<String> = Vec::new();
    for entry in entries {
        let Some(dir) = entry.as_str() else {
            eprintln!(
                "workdirs config {}: skipping non-string entry: {entry}",
                path.display()
            );
            continue;
        };
        let dir = dir.trim();
        if dir.is_empty() {
            eprintln!("workdirs config {}: skipping empty entry", path.display());
            continue;
        }
        // Reuse the task-level validation so "a valid config entry" and
        // "a valid task workdir" cannot drift apart.
        if check_workdir(Some(dir)).is_err() {
            eprintln!(
                "workdirs config {}: skipping '{dir}': workdirs must be absolute paths",
                path.display()
            );
            continue;
        }
        if !dirs.iter().any(|d| d == dir) {
            dirs.push(dir.to_string());
        }
    }
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse against a fixed path; assertions only look at the message
    /// text, not the path.
    fn parse(text: &str) -> Result<Vec<String>, String> {
        super::parse(text, Path::new("/tmp/workdirs-test.json"))
    }

    #[test]
    fn valid_entries_are_trimmed_deduped_and_kept_in_order() {
        let dirs = parse(r#"["/b", " /a ", "/b", "/c"]"#).unwrap();
        assert_eq!(dirs, vec!["/b", "/a", "/c"]);
    }

    #[test]
    fn invalid_entries_are_skipped() {
        let dirs = parse(r#"["relative", 5, null, "", "  ", "/ok", {"path": "/x"}]"#).unwrap();
        assert_eq!(dirs, vec!["/ok"]);
    }

    #[test]
    fn malformed_json_is_an_error() {
        let err = parse("{not json").unwrap_err();
        assert!(err.contains("not valid JSON"), "{err}");
    }

    #[test]
    fn a_non_array_config_is_an_error() {
        let err = parse(r#"{"a": "/x"}"#).unwrap_err();
        assert!(err.contains("must be a JSON array"), "{err}");
    }

    #[test]
    fn a_missing_file_is_an_empty_list() {
        let dirs = load(Path::new("/nonexistent-dir-orga-rs-tests/workdirs.json")).unwrap();
        assert!(dirs.is_empty());
    }

    #[test]
    fn load_reads_and_parses_a_real_file() {
        let path = std::env::temp_dir()
            .join(format!("orga-rs-workdirs-test-{}.json", std::process::id()));
        std::fs::write(&path, r#"["/tmp/one", "relative"]"#).unwrap();
        let dirs = load(&path).unwrap();
        assert_eq!(dirs, vec!["/tmp/one"]);
        let _ = std::fs::remove_file(&path);
    }
}

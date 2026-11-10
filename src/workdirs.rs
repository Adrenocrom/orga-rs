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
//! restart — including edits made through the API ([`add`] / [`remove`]),
//! which rewrite the same file. Error policy:
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

/// Add a directory to the config. The path is validated (absolute, within
/// the length limit) and trimmed; adding an existing entry is a no-op that
/// keeps its position. A missing config file is treated as an empty list,
/// so the first add creates the file.
pub fn add(path: &Path, dir: &str) -> Result<Vec<String>, ConfigError> {
    let dir = dir.trim();
    check_workdir(Some(dir)).map_err(ConfigError::Invalid)?;
    let mut dirs = load(path).map_err(ConfigError::Load)?;
    if !dirs.iter().any(|d| d == dir) {
        dirs.push(dir.to_string());
        save(path, &dirs)?;
    }
    Ok(dirs)
}

/// Remove a directory from the config. The path is trimmed and compared
/// literally (no canonicalization — the config stores what the user
/// entered). Removing an entry that is not in the list is a no-op.
pub fn remove(path: &Path, dir: &str) -> Result<Vec<String>, ConfigError> {
    let dir = dir.trim();
    let mut dirs = load(path).map_err(ConfigError::Load)?;
    let before = dirs.len();
    dirs.retain(|d| d != dir);
    if dirs.len() != before {
        save(path, &dirs)?;
    }
    Ok(dirs)
}

/// Write the list back as a JSON array, creating the parent directory on
/// the way (the default `~/.config/orga` exists after `serve` created the
/// database there, but `ORGA_WORKDIRS` may point anywhere).
fn save(path: &Path, dirs: &[String]) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ConfigError::Save(format!(
                    "cannot create directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
    }
    let text = serde_json::to_string_pretty(dirs).map_err(|e| ConfigError::Save(e.to_string()))?;
    std::fs::write(path, text).map_err(|e| {
        ConfigError::Save(format!("cannot write workdirs config {}: {e}", path.display()))
    })
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

/// Failure of a config mutation ([`add`] / [`remove`]): either the input is
/// invalid (a relative path — the caller's fault, a 400), or the config
/// cannot be read or written (the file's fault, a 500 with the reason).
#[derive(Debug)]
pub enum ConfigError {
    Invalid(String),
    Load(String),
    Save(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Invalid(msg) => write!(f, "{msg}"),
            ConfigError::Load(msg) | ConfigError::Save(msg) => write!(f, "{msg}"),
        }
    }
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

    /// A unique config path per test, so tests never share state.
    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "orga-rs-workdirs-mutate-{}-{name}.json",
            std::process::id()
        ))
    }

    #[test]
    fn add_appends_validates_and_creates_the_file() {
        let path = test_path("add");

        // No config file yet: the first add creates it.
        let dirs = add(&path, "/tmp/project-a").unwrap();
        assert_eq!(dirs, vec!["/tmp/project-a"]);
        assert_eq!(load(&path).unwrap(), vec!["/tmp/project-a"]);

        // A second entry is appended; duplicates keep their position.
        let dirs = add(&path, "/tmp/project-b").unwrap();
        assert_eq!(dirs, vec!["/tmp/project-a", "/tmp/project-b"]);
        let dirs = add(&path, "/tmp/project-a").unwrap();
        assert_eq!(dirs, vec!["/tmp/project-a", "/tmp/project-b"]);

        // The path is trimmed before it is stored.
        let dirs = add(&path, "  /tmp/project-c  ").unwrap();
        assert_eq!(dirs.last().unwrap(), "/tmp/project-c");

        // Relative paths are rejected before anything is written.
        let err = add(&path, "relative").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
        assert!(err.to_string().contains("absolute"), "{err}");
        assert_eq!(load(&path).unwrap().len(), 3);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn remove_deletes_the_entry_and_tolerates_unknown_paths() {
        let path = test_path("remove");
        std::fs::write(&path, r#"["/tmp/a", "/tmp/b"]"#).unwrap();

        let dirs = remove(&path, "/tmp/a").unwrap();
        assert_eq!(dirs, vec!["/tmp/b"]);
        assert_eq!(load(&path).unwrap(), vec!["/tmp/b"]);

        // Removing an entry that is not in the list is a no-op (the file
        // is not rewritten).
        let dirs = remove(&path, "/tmp/never-existed").unwrap();
        assert_eq!(dirs, vec!["/tmp/b"]);

        // Removing the last entry leaves an empty array in the file.
        let dirs = remove(&path, "/tmp/b").unwrap();
        assert!(dirs.is_empty());
        assert_eq!(load(&path).unwrap(), Vec::<String>::new());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn add_fails_on_a_malformed_config_without_touching_it() {
        let path = test_path("malformed");
        std::fs::write(&path, "{not json").unwrap();
        let err = add(&path, "/tmp/x").unwrap_err();
        assert!(matches!(err, ConfigError::Load(_)));
        assert!(err.to_string().contains("not valid JSON"), "{err}");
        // The malformed file is left exactly as it was.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{not json");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn add_creates_missing_parent_directories() {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir()
            .join(format!(
                "orga-rs-workdirs-nested-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .join("deep")
            .join("workdirs.json");
        let dirs = add(&path, "/tmp/x").unwrap();
        assert_eq!(dirs, vec!["/tmp/x"]);
        assert_eq!(load(&path).unwrap(), vec!["/tmp/x"]);
        std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap()).unwrap();
    }
}

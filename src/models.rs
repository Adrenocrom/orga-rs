use serde::{Deserialize, Serialize};

/// Task status. The wire and database representation is the lowercase string
/// form (see [`Status::as_str`]); [`STATUSES`] is derived from the variants so
/// the allowed values are listed in exactly one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Todo,
    InProgress,
    Done,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Status::Todo => "todo",
            Status::InProgress => "in_progress",
            Status::Done => "done",
        }
    }
}

/// Task priority, same conventions as [`Status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Priority {
    Low,
    #[default]
    Medium,
    High,
}

impl Priority {
    pub const fn as_str(self) -> &'static str {
        match self {
            Priority::Low => "low",
            Priority::Medium => "medium",
            Priority::High => "high",
        }
    }
}

/// Allowed wire values, derived from the enums above.
pub const STATUSES: [&str; 3] = [
    Status::Todo.as_str(),
    Status::InProgress.as_str(),
    Status::Done.as_str(),
];
pub const PRIORITIES: [&str; 3] = [
    Priority::Low.as_str(),
    Priority::Medium.as_str(),
    Priority::High.as_str(),
];

/// Maximum title length; matches the dashboard input's `maxlength` so the
/// client-side and server-side limits cannot disagree.
pub const MAX_TITLE_LEN: usize = 200;

/// Maximum length of a task's working directory path.
pub const MAX_WORKDIR_LEN: usize = 1024;

/// A task as stored in the database and sent over the wire. `status` and
/// `priority` are deliberately kept as strings (not the enums above): a
/// hand-edited database row with an unexpected value still round-trips
/// instead of failing every request. Validation happens at the API boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub status: String,
    pub priority: String,
    pub due_date: Option<String>,
    /// The agent assigned to this task, if any. Set implicitly by
    /// `POST /api/tasks/{id}/run` and removable via
    /// `DELETE /api/tasks/{id}/agent`.
    #[serde(default)]
    pub agent_id: Option<i64>,
    /// Optional working directory the agent process executes in. `None`
    /// means the server's working directory. Set via
    /// `PUT /api/tasks/{id}/workdir`.
    #[serde(default)]
    pub workdir: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTask {
    pub title: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
}

impl CreateTask {
    pub fn validate(&self) -> Result<(), String> {
        check_title(&self.title)?;
        check_enum("status", self.status.as_deref(), &STATUSES)?;
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)?;
        check_due_date(self.due_date.as_deref())?;
        check_workdir(self.workdir.as_deref())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateTask {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
}

impl UpdateTask {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(title) = &self.title {
            check_title(title)?;
        }
        check_enum("status", self.status.as_deref(), &STATUSES)?;
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)?;
        check_due_date(self.due_date.as_deref())?;
        check_workdir(self.workdir.as_deref())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Stats {
    pub total: i64,
    pub todo: i64,
    pub in_progress: i64,
    pub done: i64,
    pub overdue: i64,
}

/// Maximum agent name length (same rationale as [`MAX_TITLE_LEN`]).
pub const MAX_AGENT_NAME_LEN: usize = 100;

/// Maximum length of the configured command and prompt template.
pub const MAX_AGENT_TEXT_LEN: usize = 2000;

/// Default timeout for an agent run, in seconds.
pub const DEFAULT_RUN_TIMEOUT_SECS: u64 = 600;

/// Maximum timeout a client may configure for an agent run, in seconds.
pub const MAX_RUN_TIMEOUT_SECS: u64 = 24 * 60 * 60;

/// Maximum number of bytes captured from an agent's combined output
/// (stdout + stderr). The tail is kept: the interesting part of a log is
/// usually the end.
pub const MAX_RUN_OUTPUT_BYTES: usize = 64 * 1024;

/// A configured agent: a name and the shell command that is spawned to work
/// on a task. The command receives the rendered prompt on **stdin**, never
/// on the command line — task content cannot inject shell syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: i64,
    pub name: String,
    /// Shell command, run via `sh -c`. Example: `my-agent --model x`.
    pub command: String,
    /// Optional template for the prompt sent on stdin. `{id}`, `{title}`,
    /// `{description}`, `{status}`, `{priority}`, `{due_date}` are replaced;
    /// empty -> a default prompt.
    #[serde(default)]
    pub prompt_template: Option<String>,
    /// Seconds before a run is killed; 0 or absent -> [`DEFAULT_RUN_TIMEOUT_SECS`].
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateAgent {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub prompt_template: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl CreateAgent {
    pub fn validate(&self) -> Result<(), String> {
        check_agent_name(&self.name)?;
        check_command(&self.command)?;
        check_prompt_template(self.prompt_template.as_deref())?;
        check_timeout(self.timeout_secs)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateAgent {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub prompt_template: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl UpdateAgent {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(name) = &self.name {
            check_agent_name(name)?;
        }
        if let Some(command) = &self.command {
            check_command(command)?;
        }
        check_prompt_template(self.prompt_template.as_deref())?;
        check_timeout(self.timeout_secs)?;
        Ok(())
    }
}

/// Lifecycle of an agent run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    /// Waiting for the agent's single execution slot; no process yet.
    Queued,
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl RunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            RunStatus::Queued => "queued",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::TimedOut => "timed_out",
            RunStatus::Cancelled => "cancelled",
        }
    }
}

/// Allowed wire values, derived from the enum.
pub const RUN_STATUSES: [&str; 6] = [
    RunStatus::Queued.as_str(),
    RunStatus::Running.as_str(),
    RunStatus::Completed.as_str(),
    RunStatus::Failed.as_str(),
    RunStatus::TimedOut.as_str(),
    RunStatus::Cancelled.as_str(),
];

/// One execution of an agent against a task.
#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub id: i64,
    pub task_id: i64,
    /// The agent that was run; `null` when the agent has since been deleted
    /// (the `agent_name` snapshot keeps the entry readable).
    pub agent_id: Option<i64>,
    pub agent_name: String,
    pub status: String,
    /// Process exit code, when the process ended on its own.
    pub exit_code: Option<i64>,
    /// Combined stdout+stderr, truncated to [`MAX_RUN_OUTPUT_BYTES`].
    pub output: String,
    pub started_at: String,
    pub ended_at: Option<String>,
}

pub(crate) fn check_agent_name(name: &str) -> Result<(), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("name must not be empty".to_string());
    }
    if name.chars().count() > MAX_AGENT_NAME_LEN {
        return Err(format!(
            "name must be at most {MAX_AGENT_NAME_LEN} characters"
        ));
    }
    Ok(())
}

fn check_command(command: &str) -> Result<(), String> {
    let command = command.trim();
    if command.is_empty() {
        return Err("command must not be empty".to_string());
    }
    if command.chars().count() > MAX_AGENT_TEXT_LEN {
        return Err(format!(
            "command must be at most {MAX_AGENT_TEXT_LEN} characters"
        ));
    }
    Ok(())
}

fn check_prompt_template(template: Option<&str>) -> Result<(), String> {
    match template.map(str::trim) {
        None | Some("") => Ok(()),
        Some(t) if t.chars().count() > MAX_AGENT_TEXT_LEN => Err(format!(
            "prompt_template must be at most {MAX_AGENT_TEXT_LEN} characters"
        )),
        Some(_) => Ok(()),
    }
}

fn check_timeout(timeout: Option<u64>) -> Result<(), String> {
    match timeout {
        None | Some(0) => Ok(()), // 0 -> default
        Some(t) if t > MAX_RUN_TIMEOUT_SECS => Err(format!(
            "timeout_secs must be at most {MAX_RUN_TIMEOUT_SECS}"
        )),
        Some(_) => Ok(()),
    }
}

/// Render the prompt sent to an agent on stdin. Placeholders `{id}`,
/// `{title}`, `{description}`, `{status}`, `{priority}`, `{due_date}` and
/// `{workdir}` are replaced with the task's values (`{due_date}` and
/// `{workdir}` -> "none" when unset). A missing/empty template yields a
/// sensible default prompt.
pub fn render_prompt(template: Option<&str>, task: &Task) -> String {
    let template = template.map(str::trim).filter(|t| !t.is_empty());
    let body = template.unwrap_or(
        "Work on this task and report the result:\n\
         id: {id}\ntitle: {title}\ndescription: {description}\n\
         status: {status}\npriority: {priority}\ndue_date: {due_date}\nworkdir: {workdir}",
    );
    let due = task.due_date.as_deref().unwrap_or("none");
    let workdir = task.workdir.as_deref().unwrap_or("none");
    body
        .replace("{id}", &task.id.to_string())
        .replace("{title}", &task.title)
        .replace("{description}", &task.description)
        .replace("{status}", &task.status)
        .replace("{priority}", &task.priority)
        .replace("{due_date}", due)
        .replace("{workdir}", workdir)
}

/// A task's working directory must be an absolute path (the agent process
/// is spawned with it as `current_dir`). `None` or an empty string mean
/// "no working directory" (the server's own working directory is used).
/// Relative paths are rejected: they would silently resolve against the
/// server's working directory, which changes with where `serve` was started.
pub(crate) fn check_workdir(workdir: Option<&str>) -> Result<(), String> {
    match workdir.map(str::trim) {
        None | Some("") => Ok(()),
        Some(s) => {
            if !s.starts_with('/') {
                return Err(format!(
                    "invalid workdir '{s}': must be an absolute path, e.g. /home/me/project"
                ));
            }
            if s.chars().count() > MAX_WORKDIR_LEN {
                return Err(format!(
                    "workdir must be at most {MAX_WORKDIR_LEN} characters"
                ));
            }
            Ok(())
        }
    }
}

pub(crate) fn check_title(title: &str) -> Result<(), String> {
    let title = title.trim();
    if title.is_empty() {
        return Err("title must not be empty".to_string());
    }
    if title.chars().count() > MAX_TITLE_LEN {
        return Err(format!("title must be at most {MAX_TITLE_LEN} characters"));
    }
    Ok(())
}

/// A due date must be a canonical `YYYY-MM-DD` calendar date. `None` or an
/// empty string mean "no due date" (and, on update, "clear the due date").
/// Canonical form matters: SQLite's `date()` must parse the value for the
/// overdue counter, and the dashboard compares due dates as plain strings.
pub(crate) fn check_due_date(due: Option<&str>) -> Result<(), String> {
    match due.map(str::trim) {
        None | Some("") => Ok(()),
        Some(s) => {
            // Parse and re-format: chrono is lenient about digit counts
            // ("2026-1-5" parses), so only a round-trip-identical value passes.
            let canonical = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()
                .filter(|d| d.format("%Y-%m-%d").to_string() == s);
            if canonical.is_some() {
                Ok(())
            } else {
                Err(format!(
                    "invalid due_date '{s}': expected YYYY-MM-DD, e.g. 2026-11-10"
                ))
            }
        }
    }
}

pub(crate) fn check_enum(name: &str, value: Option<&str>, allowed: &[&str]) -> Result<(), String> {
    match value {
        Some(v) if !allowed.contains(&v) => Err(format!(
            "invalid {name} '{v}': expected one of {}",
            allowed.join(", ")
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(title: &str) -> CreateTask {
        CreateTask {
            title: title.to_string(),
            description: None,
            status: None,
            priority: None,
            due_date: None,
            workdir: None,
        }
    }

    #[test]
    fn validation_rejects_bad_enums_and_blank_titles() {
        let mut c = create("ok");
        assert!(c.validate().is_ok());

        c.status = Some("bogus".into());
        assert!(c.validate().is_err());

        c.status = Some("done".into());
        c.priority = Some("urgent".into());
        assert!(c.validate().is_err());

        c.priority = Some("high".into());
        assert!(c.validate().is_ok());

        c.title = "   ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn update_validation_ignores_absent_title() {
        let u = UpdateTask {
            title: None,
            description: None,
            status: Some("done".into()),
            priority: None,
            due_date: None,
            workdir: None,
        };
        assert!(u.validate().is_ok());

        let u = UpdateTask {
            title: Some("".into()),
            ..u
        };
        assert!(u.validate().is_err());
    }

    #[test]
    fn due_date_must_be_a_canonical_calendar_date() {
        let mut c = create("ok");
        for good in ["2026-11-10", "2999-01-01", " 2026-11-10 ", ""] {
            c.due_date = Some(good.to_string());
            assert!(c.validate().is_ok(), "{good:?} should be accepted");
        }
        for bad in [
            "tomorrow",
            "2026-1-5", // parses in chrono, but is not canonical
            "2026-02-30", // not a real calendar date
            "2026-11-10T00:00",
            "11/10/2026",
        ] {
            c.due_date = Some(bad.to_string());
            assert!(c.validate().is_err(), "{bad:?} should be rejected");
        }
        c.due_date = None;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn title_length_is_capped() {
        let mut c = create(&"x".repeat(MAX_TITLE_LEN));
        assert!(c.validate().is_ok());

        c.title = "x".repeat(MAX_TITLE_LEN + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn enum_str_forms_are_the_single_source_of_truth() {
        assert_eq!(STATUSES, ["todo", "in_progress", "done"]);
        assert_eq!(PRIORITIES, ["low", "medium", "high"]);
        assert_eq!(Status::default().as_str(), "todo");
        assert_eq!(Priority::default().as_str(), "medium");
    }

    fn task() -> Task {
        Task {
            id: 7,
            title: "Ship it".into(),
            description: "the final polish".into(),
            status: "todo".into(),
            priority: "high".into(),
            due_date: Some("2026-11-10".into()),
            agent_id: None,
            workdir: None,
            created_at: "2026-11-10T09:00:00+00:00".into(),
            updated_at: "2026-11-10T09:00:00+00:00".into(),
        }
    }

    #[test]
    fn agent_validation_rejects_blank_and_overlong_fields() {
        let mut a = CreateAgent {
            name: "helper".into(),
            command: "echo hi".into(),
            prompt_template: None,
            timeout_secs: None,
        };
        assert!(a.validate().is_ok());

        a.name = "  ".into();
        assert!(a.validate().is_err());
        a.name = "x".repeat(MAX_AGENT_NAME_LEN + 1);
        assert!(a.validate().is_err());
        a.name = "helper".into();

        a.command = "  ".into();
        assert!(a.validate().is_err());
        a.command = "x".repeat(MAX_AGENT_TEXT_LEN + 1);
        assert!(a.validate().is_err());
        a.command = "echo hi".into();

        a.prompt_template = Some("x".repeat(MAX_AGENT_TEXT_LEN + 1));
        assert!(a.validate().is_err());
        a.prompt_template = Some("".into()); // empty -> default prompt
        assert!(a.validate().is_ok());
        a.prompt_template = None;

        a.timeout_secs = Some(MAX_RUN_TIMEOUT_SECS);
        assert!(a.validate().is_ok());
        a.timeout_secs = Some(MAX_RUN_TIMEOUT_SECS + 1);
        assert!(a.validate().is_err());
        a.timeout_secs = Some(0); // 0 -> default timeout
        assert!(a.validate().is_ok());
    }

    #[test]
    fn update_agent_validation_checks_only_given_fields() {
        let u = UpdateAgent {
            name: None,
            command: None,
            prompt_template: None,
            timeout_secs: None,
        };
        assert!(u.validate().is_ok());

        let u = UpdateAgent {
            name: Some("  ".into()),
            ..u
        };
        assert!(u.validate().is_err());

        let u = UpdateAgent {
            name: None,
            command: Some("".into()),
            ..u
        };
        assert!(u.validate().is_err());
    }

    #[test]
    fn workdir_must_be_an_absolute_path() {
        let mut c = create("ok");
        for good in ["/home/me/project", "/tmp", "  ", "", "/a/b/c"] {
            c.workdir = Some(good.to_string());
            assert!(c.validate().is_ok(), "{good:?} should be accepted");
        }
        for bad in ["relative/path", "~/project", "C:\\work", "."] {
            c.workdir = Some(bad.to_string());
            assert!(c.validate().is_err(), "{bad:?} should be rejected");
        }
        c.workdir = None;
        assert!(c.validate().is_ok());

        // over-long paths are rejected
        c.workdir = Some(format!("/{}", "x".repeat(MAX_WORKDIR_LEN)));
        assert!(c.validate().is_err());
    }

    #[test]
    fn render_prompt_replaces_placeholders_and_defaults_sensibly() {
        let t = task();
        // default prompt when no template is given
        let p = render_prompt(None, &t);
        assert!(p.contains("id: 7"));
        assert!(p.contains("title: Ship it"));
        assert!(p.contains("description: the final polish"));
        assert!(p.contains("status: todo"));
        assert!(p.contains("priority: high"));
        assert!(p.contains("due_date: 2026-11-10"));
        assert!(p.contains("workdir: none"));

        // custom template with placeholders
        let p = render_prompt(Some("Do: {title} (#{id})"), &t);
        assert_eq!(p, "Do: Ship it (#7)");

        // empty template -> default prompt
        assert_eq!(render_prompt(Some("   "), &t), render_prompt(None, &t));

        // no due date -> "none"
        let mut t2 = t.clone();
        t2.due_date = None;
        assert!(render_prompt(Some("{due_date}"), &t2).contains("none"));

        // a workdir is rendered into the prompt
        let mut t3 = t.clone();
        t3.workdir = Some("/tmp/work".into());
        assert!(render_prompt(Some("{workdir}"), &t3).contains("/tmp/work"));
        assert!(render_prompt(None, &t3).contains("workdir: /tmp/work"));
    }

    #[test]
    fn run_status_str_forms_are_the_single_source_of_truth() {
        assert_eq!(
            RUN_STATUSES,
            ["queued", "running", "completed", "failed", "timed_out", "cancelled"]
        );
    }
}

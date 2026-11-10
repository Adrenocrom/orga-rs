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
}

impl CreateTask {
    pub fn validate(&self) -> Result<(), String> {
        check_title(&self.title)?;
        check_enum("status", self.status.as_deref(), &STATUSES)?;
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)?;
        check_due_date(self.due_date.as_deref())
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
}

impl UpdateTask {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(title) = &self.title {
            check_title(title)?;
        }
        check_enum("status", self.status.as_deref(), &STATUSES)?;
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)?;
        check_due_date(self.due_date.as_deref())
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
}

use serde::{Deserialize, Serialize};

pub const STATUSES: [&str; 3] = ["todo", "in_progress", "done"];
pub const PRIORITIES: [&str; 3] = ["low", "medium", "high"];

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
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)
    }
}

#[derive(Debug, Clone, Deserialize)]
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
        check_enum("priority", self.priority.as_deref(), &PRIORITIES)
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

fn check_title(title: &str) -> Result<(), String> {
    if title.trim().is_empty() {
        Err("title must not be empty".to_string())
    } else {
        Ok(())
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
}

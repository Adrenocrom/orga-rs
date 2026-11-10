use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, Row};

use crate::models::{CreateTask, Stats, Task, UpdateTask};

/// Shared handle passed into the web server's router.
pub type SharedDb = Arc<Mutex<Db>>;

const COLUMNS: &str = "id, title, description, status, priority, due_date, created_at, updated_at";

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        // WAL + busy timeout so the dashboard and the MCP server can safely
        // share one database file from separate processes.
        let _mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                status      TEXT NOT NULL DEFAULT 'todo',
                priority    TEXT NOT NULL DEFAULT 'medium',
                due_date    TEXT,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);",
        )?;
        Ok(Self { conn })
    }

    fn row_to_task(row: &Row) -> rusqlite::Result<Task> {
        Ok(Task {
            id: row.get(0)?,
            title: row.get(1)?,
            description: row.get(2)?,
            status: row.get(3)?,
            priority: row.get(4)?,
            due_date: row.get(5)?,
            created_at: row.get(6)?,
            updated_at: row.get(7)?,
        })
    }

    /// List tasks, optionally filtered. High priority first, then newest first.
    pub fn list(&self, status: Option<&str>, priority: Option<&str>) -> rusqlite::Result<Vec<Task>> {
        let mut stmt = self.conn.prepare(&format!(
            "SELECT {COLUMNS} FROM tasks
             WHERE (?1 IS NULL OR status = ?1)
               AND (?2 IS NULL OR priority = ?2)
             ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, id DESC"
        ))?;
        let rows = stmt.query_map(params![status, priority], Self::row_to_task)?;
        rows.collect()
    }

    pub fn get(&self, id: i64) -> rusqlite::Result<Option<Task>> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT {COLUMNS} FROM tasks WHERE id = ?1"))?;
        let mut rows = stmt.query_map(params![id], Self::row_to_task)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn create(&self, input: &CreateTask) -> rusqlite::Result<Task> {
        let now = chrono::Utc::now().to_rfc3339();
        let title = input.title.trim().to_string();
        let description = input
            .description
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        let status = input.status.clone().unwrap_or_else(|| "todo".to_string());
        let priority = input.priority.clone().unwrap_or_else(|| "medium".to_string());
        let due_date = normalize_due(&input.due_date);

        self.conn.execute(
            "INSERT INTO tasks (title, description, status, priority, due_date, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![title, description, status, priority, due_date, now],
        )?;
        let id = self.conn.last_insert_rowid();
        self.get(id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    /// Merge `input` into the existing task. Returns `None` if the id is unknown.
    pub fn update(&self, id: i64, input: &UpdateTask) -> rusqlite::Result<Option<Task>> {
        let Some(existing) = self.get(id)? else {
            return Ok(None);
        };
        let merged = Task {
            id,
            title: input
                .title
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or(existing.title),
            description: input
                .description
                .as_deref()
                .map(str::trim)
                .map(str::to_string)
                .unwrap_or(existing.description),
            status: input.status.clone().unwrap_or(existing.status),
            priority: input.priority.clone().unwrap_or(existing.priority),
            due_date: match input.due_date.as_deref().map(str::trim) {
                None => existing.due_date, // not provided -> keep
                Some("") => None,           // empty string -> clear
                Some(s) => Some(s.to_string()),
            },
            created_at: existing.created_at,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        self.conn.execute(
            "UPDATE tasks
             SET title = ?1, description = ?2, status = ?3, priority = ?4, due_date = ?5, updated_at = ?6
             WHERE id = ?7",
            params![
                merged.title,
                merged.description,
                merged.status,
                merged.priority,
                merged.due_date,
                merged.updated_at,
                id
            ],
        )?;
        Ok(Some(merged))
    }

    /// Returns `false` if the id did not exist.
    pub fn delete(&self, id: i64) -> rusqlite::Result<bool> {
        Ok(self.conn.execute("DELETE FROM tasks WHERE id = ?1", params![id])? > 0)
    }

    /// Aggregate counters for the dashboard header.
    pub fn stats(&self) -> rusqlite::Result<Stats> {
        self.conn.query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(status = 'todo'), 0),
                    COALESCE(SUM(status = 'in_progress'), 0),
                    COALESCE(SUM(status = 'done'), 0),
                    COALESCE(SUM(due_date IS NOT NULL
                                  AND status != 'done'
                                  AND date(due_date) < date('now')), 0)
             FROM tasks",
            [],
            |row| {
                Ok(Stats {
                    total: row.get(0)?,
                    todo: row.get(1)?,
                    in_progress: row.get(2)?,
                    done: row.get(3)?,
                    overdue: row.get(4)?,
                })
            },
        )
    }
}

/// Treat a missing or whitespace-only due date as "no due date".
fn normalize_due(due: &Option<String>) -> Option<String> {
    due.as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CreateTask, UpdateTask};

    fn db() -> Db {
        Db::open(":memory:").expect("open in-memory db")
    }

    fn create(title: &str) -> CreateTask {
        CreateTask {
            title: title.to_string(),
            description: Some("desc".into()),
            status: None,
            priority: None,
            due_date: None,
        }
    }

    #[test]
    fn create_get_delete_roundtrip() {
        let db = db();
        let task = db.create(&create("Write tests")).unwrap();
        assert_eq!(task.title, "Write tests");
        assert_eq!(task.status, "todo");
        assert_eq!(task.priority, "medium");
        assert!(task.due_date.is_none());

        assert_eq!(db.get(task.id).unwrap().unwrap().title, "Write tests");
        assert!(db.get(999).unwrap().is_none());

        assert!(db.delete(task.id).unwrap());
        assert!(!db.delete(task.id).unwrap());
        assert!(db.get(task.id).unwrap().is_none());
    }

    #[test]
    fn update_merges_fields_and_can_clear_due_date() {
        let db = db();
        let mut input = create("Ship it");
        input.due_date = Some("2999-01-01".into());
        input.priority = Some("high".into());
        let task = db.create(&input).unwrap();
        assert_eq!(task.priority, "high");

        let patch = UpdateTask {
            title: Some("  ".into()), // blank title is ignored, old value kept
            description: None,
            status: Some("done".into()),
            priority: None,
            due_date: Some("".into()), // empty string clears the due date
        };
        let updated = db.update(task.id, &patch).unwrap().unwrap();
        assert_eq!(updated.title, "Ship it");
        assert_eq!(updated.status, "done");
        assert_eq!(updated.priority, "high");
        assert!(updated.due_date.is_none());

        assert!(db.update(42, &patch).unwrap().is_none());
    }

    #[test]
    fn list_filters_and_orders_by_priority() {
        let db = db();
        let mut low = create("low task");
        low.priority = Some("low".into());
        let mut high = create("high task");
        high.priority = Some("high".into());
        db.create(&low).unwrap();
        db.create(&create("medium task")).unwrap();
        db.create(&high).unwrap();

        let all = db.list(None, None).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].title, "high task"); // high priority first

        let only = db.list(None, Some("low")).unwrap();
        assert_eq!(only.len(), 1);
        assert_eq!(only[0].title, "low task");
    }

    #[test]
    fn stats_counts_statuses_and_overdue() {
        let db = db();
        let mut overdue = create("overdue");
        overdue.due_date = Some("2000-01-01".into());
        db.create(&overdue).unwrap();

        let mut future = create("future");
        future.due_date = Some("2999-01-01".into());
        db.create(&future).unwrap();

        let mut done_but_late = create("done but late");
        done_but_late.status = Some("done".into());
        done_but_late.due_date = Some("2000-01-01".into());
        db.create(&done_but_late).unwrap();

        let s = db.stats().unwrap();
        assert_eq!(s.total, 3);
        assert_eq!(s.todo, 2);
        assert_eq!(s.done, 1);
        assert_eq!(s.overdue, 1); // the done-but-late task does not count
    }
}

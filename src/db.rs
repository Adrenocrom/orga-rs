use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, Row, TransactionBehavior};

use crate::models::{CreateTask, Priority, Stats, Status, Task, UpdateTask};

/// Shared handle passed into the web server's router.
pub type SharedDb = Arc<Mutex<Db>>;

const COLUMNS: &str = "id, title, description, status, priority, due_date, created_at, updated_at";

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        // WAL + busy timeout so a second `serve` instance (or an external
        // sqlite3 CLI session) can read and write the same file while this
        // process runs. The MCP bridge does not need this: it never touches
        // the database, it forwards every call over HTTP to this process.
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
        Self::get_on(&self.conn, id)
    }

    /// `get` against any connection — or an open transaction (see `update`).
    fn get_on(conn: &Connection, id: i64) -> rusqlite::Result<Option<Task>> {
        let mut stmt = conn.prepare(&format!("SELECT {COLUMNS} FROM tasks WHERE id = ?1"))?;
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
        let status = input
            .status
            .clone()
            .unwrap_or_else(|| Status::Todo.as_str().to_string());
        let priority = input
            .priority
            .clone()
            .unwrap_or_else(|| Priority::Medium.as_str().to_string());
        let due_date = normalize_due(&input.due_date);

        self.conn.execute(
            "INSERT INTO tasks (title, description, status, priority, due_date, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![title, description, status, priority, due_date, now],
        )?;
        let id = self.conn.last_insert_rowid();
        // The row was just inserted through this connection and cannot have
        // been deleted in between (single connection behind a mutex), so a
        // missing row here means a broken invariant, not a normal failure.
        Ok(self.get(id)?.expect("row just inserted must be readable"))
    }

    /// Merge `input` into the existing task. Returns `None` if the id is unknown.
    ///
    /// The read-modify-write runs inside an IMMEDIATE transaction so that a
    /// concurrent writer on the same database file (a second `serve`
    /// instance, an external sqlite3 session) cannot slip a change in
    /// between the read and the write — it would be silently overwritten.
    ///
    /// A blank title is an error here as well as at the API boundary: one
    /// policy, enforced at both layers, so a future caller that skips the
    /// web validation cannot silently keep a stale title.
    pub fn update(&mut self, id: i64, input: &UpdateTask) -> Result<Option<Task>, UpdateError> {
        let title = match input.title.as_deref() {
            None => None, // not provided -> keep the current title
            Some(t) => {
                let t = t.trim();
                if t.is_empty() {
                    return Err(UpdateError::BlankTitle);
                }
                Some(t.to_string())
            }
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = Self::get_on(&tx, id)? else {
            return Ok(None); // tx drops -> rollback; nothing was written
        };
        let merged = Task {
            id,
            title: title.unwrap_or(existing.title),
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
        let changed = tx.execute(
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
        debug_assert_eq!(changed, 1, "the row was read inside this transaction");
        tx.commit()?;
        Ok(Some(merged))
    }

    /// Returns `false` if the id did not exist.
    pub fn delete(&self, id: i64) -> rusqlite::Result<bool> {
        Ok(self.conn.execute("DELETE FROM tasks WHERE id = ?1", params![id])? > 0)
    }

    /// Aggregate counters for the dashboard header.
    ///
    /// "Overdue" compares against UTC midnight (`date('now')`), matching the
    /// dashboard's `toISOString()` comparison — one deliberate timezone for
    /// both sides, so a task flips overdue at the same moment everywhere.
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

/// Failure modes of [`Db::update`], kept distinct so a blank title (a client
/// error, 400) is not conflated with a database failure (500).
#[derive(Debug)]
pub enum UpdateError {
    BlankTitle,
    Db(rusqlite::Error),
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::BlankTitle => write!(f, "title must not be empty"),
            UpdateError::Db(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for UpdateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UpdateError::BlankTitle => None,
            UpdateError::Db(e) => Some(e),
        }
    }
}

impl From<rusqlite::Error> for UpdateError {
    fn from(e: rusqlite::Error) -> Self {
        UpdateError::Db(e)
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
        let mut db = db();
        let mut input = create("Ship it");
        input.due_date = Some("2999-01-01".into());
        input.priority = Some("high".into());
        let task = db.create(&input).unwrap();
        assert_eq!(task.priority, "high");

        let patch = UpdateTask {
            title: None, // omitted -> keep the current title
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
    fn update_rejects_blank_title() {
        let mut db = db();
        let task = db.create(&create("Ship it")).unwrap();

        let patch = UpdateTask {
            title: Some("   ".into()),
            ..UpdateTask::default()
        };
        assert!(matches!(
            db.update(task.id, &patch),
            Err(UpdateError::BlankTitle)
        ));
        // the rejected update left the row untouched
        assert_eq!(db.get(task.id).unwrap().unwrap().title, "Ship it");
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

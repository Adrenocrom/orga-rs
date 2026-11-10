use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, Row, TransactionBehavior};

use crate::models::{
    Agent, CreateAgent, CreateTask, Priority, Run, RunStatus, Stats, Status, Task, UpdateAgent,
    UpdateTask,
};

/// Shared handle passed into the web server's router.
pub type SharedDb = Arc<Mutex<Db>>;

const COLUMNS: &str =
    "id, title, description, status, priority, due_date, agent_id, workdir, created_at, updated_at";

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
                agent_id    INTEGER REFERENCES agents(id),
                workdir     TEXT,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE TABLE IF NOT EXISTS agents (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL,
                command         TEXT NOT NULL,
                prompt_template TEXT,
                timeout_secs    INTEGER,
                created_at      TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                agent_id   INTEGER REFERENCES agents(id) ON DELETE SET NULL,
                agent_name TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'running',
                exit_code  INTEGER,
                output     TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL,
                ended_at   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runs_task ON runs(task_id);
            CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
        )?;
        // Migration for databases created before agents/workdirs existed:
        // add the missing columns if (and only if) they are missing. Checked
        // via pragma so opening an existing file twice stays idempotent.
        let has_agent_id: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'agent_id'")?
            .exists([])?;
        if !has_agent_id {
            conn.execute_batch(
                "ALTER TABLE tasks ADD COLUMN agent_id INTEGER REFERENCES agents(id)",
            )?;
        }
        let has_workdir: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'workdir'")?
            .exists([])?;
        if !has_workdir {
            conn.execute_batch("ALTER TABLE tasks ADD COLUMN workdir TEXT")?;
        }
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
            agent_id: row.get(6)?,
            workdir: row.get(7)?,
            created_at: row.get(8)?,
            updated_at: row.get(9)?,
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
            "INSERT INTO tasks (title, description, status, priority, due_date, workdir, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
            params![
                title,
                description,
                status,
                priority,
                due_date,
                normalize_workdir(input.workdir.as_deref()),
                now
            ],
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
            workdir: match input.workdir.as_deref().map(str::trim) {
                None => existing.workdir, // not provided -> keep
                Some("") => None,          // empty string -> clear
                Some(s) => Some(s.to_string()),
            },
            agent_id: existing.agent_id, // assignment is managed by /run and /agent
            created_at: existing.created_at,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let changed = tx.execute(
            "UPDATE tasks
             SET title = ?1, description = ?2, status = ?3, priority = ?4, due_date = ?5,
                 workdir = ?6, updated_at = ?7
             WHERE id = ?8",
            params![
                merged.title,
                merged.description,
                merged.status,
                merged.priority,
                merged.due_date,
                merged.workdir,
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

    // ---------- agents ----------

    fn row_to_agent(row: &Row) -> rusqlite::Result<Agent> {
        Ok(Agent {
            id: row.get(0)?,
            name: row.get(1)?,
            command: row.get(2)?,
            prompt_template: row.get(3)?,
            timeout_secs: row.get(4)?,
        })
    }

    pub fn list_agents(&self) -> rusqlite::Result<Vec<Agent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, command, prompt_template, timeout_secs
             FROM agents ORDER BY name COLLATE NOCASE",
        )?;
        let rows = stmt.query_map([], Self::row_to_agent)?;
        rows.collect()
    }

    pub fn get_agent(&self, id: i64) -> rusqlite::Result<Option<Agent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, command, prompt_template, timeout_secs
             FROM agents WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_agent)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn create_agent(&self, input: &CreateAgent) -> rusqlite::Result<Agent> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO agents (name, command, prompt_template, timeout_secs, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                input.name.trim(),
                input.command.trim(),
                normalize_template(input.prompt_template.as_deref()),
                input.timeout_secs,
                now
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        self.get_agent(id)?.ok_or_else(|| {
            rusqlite::Error::QueryReturnedNoRows
        })
    }

    /// Merge `input` into the existing agent. Returns `None` if the id is
    /// unknown. An empty `prompt_template` clears it (back to the default
    /// prompt); a `timeout_secs` of 0 disables the timeout entirely.
    pub fn update_agent(
        &mut self,
        id: i64,
        input: &UpdateAgent,
    ) -> Result<Option<Agent>, UpdateError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = Self::get_agent_on(&tx, id)? else {
            return Ok(None);
        };
        let merged = Agent {
            id,
            name: match input.name.as_deref() {
                Some(n) => n.trim().to_string(),
                None => existing.name,
            },
            command: match input.command.as_deref() {
                Some(c) => c.trim().to_string(),
                None => existing.command,
            },
            prompt_template: match input.prompt_template.as_deref() {
                None => existing.prompt_template, // not provided -> keep
                Some(t) => normalize_template(Some(t)), // "" -> None (default prompt)
            },            timeout_secs: match input.timeout_secs {
                None => existing.timeout_secs, // not provided -> keep
                Some(t) => Some(t),            // 0 -> no timeout (a real value)
            },
        };
        tx.execute(
            "UPDATE agents
             SET name = ?1, command = ?2, prompt_template = ?3, timeout_secs = ?4
             WHERE id = ?5",
            params![
                merged.name,
                merged.command,
                merged.prompt_template,
                merged.timeout_secs,
                id
            ],
        )?;
        tx.commit()?;
        Ok(Some(merged))
    }

    fn get_agent_on(conn: &Connection, id: i64) -> rusqlite::Result<Option<Agent>> {
        let mut stmt = conn.prepare(
            "SELECT id, name, command, prompt_template, timeout_secs
             FROM agents WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_agent)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Delete an agent. Returns `false` if the id did not exist. The
    /// assignment on tasks is cleared (ON DELETE SET NULL via the FK);
    /// historical runs keep their `agent_name` snapshot. Queued runs are
    /// marked failed first — after the deletion their `agent_id` is NULL,
    /// so nothing would ever pick them up.
    pub fn delete_agent(&self, id: i64) -> rusqlite::Result<bool> {
        // Fail queued runs before the FK nulls their agent_id.
        self.conn.execute(
            "UPDATE runs
             SET status = 'failed', ended_at = ?1,
                 output = 'agent deleted while this run was queued'
             WHERE agent_id = ?2 AND status = 'queued'",
            params![chrono::Utc::now().to_rfc3339(), id],
        )?;
        Ok(self.conn.execute("DELETE FROM agents WHERE id = ?1", params![id])? > 0)
    }

    /// Assign an agent to a task. Returns `false` if the task or the agent
    /// does not exist.
    pub fn assign_agent(&self, task_id: i64, agent_id: i64) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE tasks SET agent_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![agent_id, chrono::Utc::now().to_rfc3339(), task_id],
        )?;
        Ok(changed == 1)
    }

    /// Remove the agent assignment from a task. Returns `false` if the task
    /// does not exist (removing from an unassigned task is a no-op success).
    pub fn unassign_agent(&self, task_id: i64) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE tasks SET agent_id = NULL, updated_at = ?1 WHERE id = ?2",
            params![chrono::Utc::now().to_rfc3339(), task_id],
        )?;
        Ok(changed == 1)
    }

    /// Set or clear the working directory of a task. Returns `false` if the
    /// task does not exist. An empty string clears it.
    pub fn set_workdir(&self, task_id: i64, workdir: Option<&str>) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE tasks SET workdir = ?1, updated_at = ?2 WHERE id = ?3",
            params![
                normalize_workdir(workdir),
                chrono::Utc::now().to_rfc3339(),
                task_id
            ],
        )?;
        Ok(changed == 1)
    }

    // ---------- runs ----------

    fn row_to_run(row: &Row) -> rusqlite::Result<Run> {
        Ok(Run {
            id: row.get(0)?,
            task_id: row.get(1)?,
            agent_id: row.get(2)?,
            agent_name: row.get(3)?,
            status: row.get(4)?,
            exit_code: row.get(5)?,
            output: row.get(6)?,
            started_at: row.get(7)?,
            ended_at: row.get(8)?,
        })
    }

    /// List runs, newest first, optionally filtered by task and status.
    pub fn list_runs(
        &self,
        task_id: Option<i64>,
        status: Option<&str>,
        limit: u32,
    ) -> rusqlite::Result<Vec<Run>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id, r.task_id, r.agent_id, r.agent_name, r.status,
                    r.exit_code, r.output, r.started_at, r.ended_at
             FROM runs r
             WHERE (?1 IS NULL OR r.task_id = ?1)
               AND (?2 IS NULL OR r.status = ?2)
             ORDER BY r.id DESC
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![task_id, status, limit], Self::row_to_run)?;
        rows.collect()
    }

    pub fn get_run(&self, id: i64) -> rusqlite::Result<Option<Run>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id, r.task_id, r.agent_id, r.agent_name, r.status,
                    r.exit_code, r.output, r.started_at, r.ended_at
             FROM runs r WHERE r.id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_run)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Insert a run and return it. The agent name is snapshotted so history
    /// stays readable after the agent is deleted. `status` is `queued` when
    /// the agent's single execution slot is taken (the run starts when the
    /// slot frees) or `running` when the process is spawned immediately.
    pub fn create_run(
        &self,
        task_id: i64,
        agent: &Agent,
        status: RunStatus,
    ) -> rusqlite::Result<Run> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO runs (task_id, agent_id, agent_name, status, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![task_id, agent.id, agent.name, status.as_str(), now],
        )?;
        let id = self.conn.last_insert_rowid();
        self.get_run(id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    /// Flip a `queued` run to `running` (the agent's slot freed and the
    /// process is about to be spawned). Returns `false` if the run is no
    /// longer queued (e.g. it was cancelled while waiting).
    pub fn start_run(&self, id: i64) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE runs SET status = 'running' WHERE id = ?1 AND status = 'queued'",
            params![id],
        )?;
        Ok(changed == 1)
    }

    /// The oldest `queued` run for an agent, if any — the next one to start
    /// when the agent's execution slot frees.
    pub fn next_queued_run(&self, agent_id: i64) -> rusqlite::Result<Option<Run>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id, r.task_id, r.agent_id, r.agent_name, r.status,
                    r.exit_code, r.output, r.started_at, r.ended_at
             FROM runs r
             WHERE r.agent_id = ?1 AND r.status = 'queued'
             ORDER BY r.id ASC
             LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![agent_id], Self::row_to_run)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Record the outcome of a run. Returns `false` if the run row vanished
    /// (e.g. the task was deleted mid-run and the cascade removed the run).
    pub fn finish_run(
        &self,
        id: i64,
        status: RunStatus,
        exit_code: Option<i64>,
        output: &str,
    ) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE runs
             SET status = ?1, exit_code = ?2, output = ?3, ended_at = ?4
             WHERE id = ?5",
            params![
                status.as_str(),
                exit_code,
                output,
                chrono::Utc::now().to_rfc3339(),
                id
            ],
        )?;
        Ok(changed == 1)
    }

    /// All runs still `queued` or `running` — used at startup to mark runs
    /// orphaned by a restart as failed.
    pub fn unfinished_runs(&self) -> rusqlite::Result<Vec<Run>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.id, r.task_id, r.agent_id, r.agent_name, r.status,
                    r.exit_code, r.output, r.started_at, r.ended_at
             FROM runs r WHERE r.status IN ('queued', 'running')",
        )?;
        let rows = stmt.query_map([], Self::row_to_run)?;
        rows.collect()
    }

    /// Put a run back into the queue (used when a picked-up queued run
    /// cannot be started). Returns `false` if the run is not `running`.
    pub fn requeue_run(&self, id: i64) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE runs SET status = 'queued' WHERE id = ?1 AND status = 'running'",
            params![id],
        )?;
        Ok(changed == 1)
    }

    /// Mark a run as cancelled. Works for `queued` runs (nothing to kill,
    /// just drop out of the queue) and `running` runs (the caller kills the
    /// process via the registry). Returns `false` if the run does not exist
    /// or is already finished.
    pub fn cancel_run(&self, id: i64) -> rusqlite::Result<bool> {
        let changed = self.conn.execute(
            "UPDATE runs SET status = 'cancelled', ended_at = ?1
             WHERE id = ?2 AND status IN ('queued', 'running')",
            params![chrono::Utc::now().to_rfc3339(), id],
        )?;
        Ok(changed == 1)
    }

    /// Mark every `queued` run as cancelled. Used by the server's graceful
    /// shutdown: a queued run has no process yet, but the queue's pickup
    /// would spawn one the moment a slot frees — flipping the rows first
    /// keeps the shutdown from starting new agents on the way out.
    pub fn cancel_queued_runs(&self) -> rusqlite::Result<usize> {
        let changed = self.conn.execute(
            "UPDATE runs SET status = 'cancelled', ended_at = ?1
             WHERE status = 'queued'",
            params![chrono::Utc::now().to_rfc3339()],
        )?;
        Ok(changed)
    }

    /// Delete a run record. Returns `false` if the id did not exist. The
    /// caller owns the policy for live runs: the web layer stops a running
    /// process before deleting (the supervisor tolerates its final write
    /// landing on a row that is already gone), and a queued run leaves the
    /// queue simply by losing its row.
    pub fn delete_run(&self, id: i64) -> rusqlite::Result<bool> {
        Ok(self.conn.execute("DELETE FROM runs WHERE id = ?1", params![id])? > 0)
    }
}

/// Treat a missing or whitespace-only prompt template as "use the default".
fn normalize_template(template: Option<&str>) -> Option<String> {
    template
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
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

/// Treat a missing or whitespace-only workdir as "no workdir".
fn normalize_workdir(workdir: Option<&str>) -> Option<String> {
    workdir
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
            workdir: None,
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
            workdir: None,
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
    fn deleting_an_agent_fails_its_queued_runs() {
        let db = db();
        let task = db.create(&create("queued work")).unwrap();
        let agent = db
            .create_agent(&CreateAgent {
                name: "doomed".into(),
                command: "true".into(),
                prompt_template: None,
                timeout_secs: None,
            })
            .unwrap();

        // A queued run (as if the agent's slot were taken).
        let run = db
            .create_run(task.id, &agent, crate::models::RunStatus::Queued)
            .unwrap();
        assert_eq!(run.status, "queued");

        // Deleting the agent must not leave the run waiting forever.
        assert!(db.delete_agent(agent.id).unwrap());
        let run = db.get_run(run.id).unwrap().unwrap();
        assert_eq!(run.status, "failed");
        assert!(run.output.contains("agent deleted"));
        assert!(run.ended_at.is_some());
    }

    #[test]
    fn delete_run_removes_the_record() {
        let db = db();
        let task = db.create(&create("runnable")).unwrap();
        let agent = db
            .create_agent(&CreateAgent {
                name: "runner".into(),
                command: "true".into(),
                prompt_template: None,
                timeout_secs: None,
            })
            .unwrap();
        let run = db
            .create_run(task.id, &agent, crate::models::RunStatus::Queued)
            .unwrap();

        assert!(db.delete_run(run.id).unwrap());
        assert!(db.get_run(run.id).unwrap().is_none());
        assert!(!db.delete_run(run.id).unwrap()); // already gone
    }

    #[test]
    fn agent_timeout_zero_is_a_real_value_and_survives_updates() {
        let mut db = db();
        let agent = db
            .create_agent(&CreateAgent {
                name: "no-limit".into(),
                command: "true".into(),
                prompt_template: None,
                timeout_secs: Some(0), // no timeout
            })
            .unwrap();
        assert_eq!(agent.timeout_secs, Some(0));

        // an update that omits the field keeps the 0
        let updated = db
            .update_agent(
                agent.id,
                &UpdateAgent {
                    name: Some("renamed".into()),
                    ..UpdateAgent::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.timeout_secs, Some(0));

        // an explicit 0 also survives (it is not rewritten to the default)
        let updated = db
            .update_agent(
                agent.id,
                &UpdateAgent {
                    timeout_secs: Some(0),
                    ..UpdateAgent::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.timeout_secs, Some(0));

        // a positive value still works, and null means "keep"
        let updated = db
            .update_agent(
                agent.id,
                &UpdateAgent {
                    timeout_secs: Some(30),
                    ..UpdateAgent::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.timeout_secs, Some(30));
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

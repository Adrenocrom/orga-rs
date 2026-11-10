//! Agent runner: spawns a configured agent command and supervises it.
//!
//! The command is run through `sh -c` (the user configures it, so pipes and
//! arguments are allowed), but the task content travels **only on stdin** —
//! a task title can never inject shell syntax into the command line.
//!
//! **Per-agent queue:** every agent has a single execution slot. When a run
//! is requested while the agent already has a live run, the new run is
//! recorded as `queued` and starts automatically when the slot frees (FIFO).
//! The slot is owned by [`AgentSlots`], a map of agent id → slot; the
//! supervisor releases the slot when the process ends, then pulls the next
//! queued run for that agent.
//!
//! **Working directory:** when the task has a `workdir`, the process is
//! spawned with it as `current_dir`. The directory must exist at spawn time;
//! a missing directory fails the run with a clear message instead of
//! spawning a process in an unexpected place.
//!
//! Each run is recorded in the `runs` table: `queued` while waiting for the
//! slot, `running` while the process is alive, then `completed` (exit 0),
//! `failed` (non-zero exit or spawn error), `timed_out` (killed after the
//! configured timeout) or `cancelled` (killed via [`RunRegistry::cancel`]).
//! Output (stdout + stderr) is captured up to [`MAX_RUN_OUTPUT_BYTES`],
//! keeping the tail — the interesting part of a log is usually the end.
//!
//! Runs do not survive a server restart as processes: at startup,
//! [`fail_orphaned_runs`] flips still-`running` rows to `failed` and
//! re-queues nothing (a queued run's task may have changed since).

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::db::{Db, SharedDb};
use crate::models::{
    render_prompt, Agent, Run, RunStatus, Task, DEFAULT_RUN_TIMEOUT_SECS, MAX_RUN_OUTPUT_BYTES,
};

/// The registry shared through the router state.
pub type SharedRegistry = std::sync::Arc<RunRegistry>;

/// Live runs by id: the cancel channel of each supervisor task. Entries are
/// added by [`start`] and removed by the supervisor when the run ends, so a
/// missing entry means "no live process for this run".
#[derive(Default)]
pub struct RunRegistry {
    senders: Mutex<HashMap<i64, mpsc::Sender<()>>>,
}

impl RunRegistry {
    /// Ask a live run to stop. Returns `false` when no live supervisor
    /// exists for the id (the run already ended, or never started).
    ///
    /// A `Full` channel means a cancellation is already pending — that also
    /// counts as cancelled.
    pub fn cancel(&self, run_id: i64) -> bool {
        let Ok(map) = self.senders.lock() else {
            return false;
        };
        match map.get(&run_id) {
            Some(tx) => matches!(tx.try_send(()), Ok(()) | Err(mpsc::error::TrySendError::Full(_))),
            None => false,
        }
    }

    fn insert(&self, run_id: i64, tx: mpsc::Sender<()>) {
        if let Ok(mut map) = self.senders.lock() {
            map.insert(run_id, tx);
        }
    }

    fn remove(&self, run_id: i64) {
        if let Ok(mut map) = self.senders.lock() {
            map.remove(&run_id);
        }
    }
}

/// One execution slot per agent. `try_acquire` returns `false` when the
/// agent already has a live run — the caller records the run as `queued`
/// instead. The supervisor releases the slot when the process ends.
#[derive(Default)]
pub struct AgentSlots {
    busy: Mutex<std::collections::HashSet<i64>>,
}

impl AgentSlots {
    /// Try to take the agent's slot. `false` = busy, queue the run.
    fn try_acquire(&self, agent_id: i64) -> bool {
        match self.busy.lock() {
            Ok(mut set) => set.insert(agent_id), // insert returns false when present
            Err(_) => true, // poisoned: fail open (spawn) rather than silently queue
        }
    }

    /// Release the agent's slot, then return the next queued run for that
    /// agent (if any) so the caller can start it. Reading the next queued
    /// run *after* releasing keeps the window race-free: any run that
    /// arrives between release and read is picked up by the next release.
    fn release(&self, agent_id: i64) {
        if let Ok(mut set) = self.busy.lock() {
            set.remove(&agent_id);
        }
    }
}

/// Spawn an agent for `task`, record the run, assign the agent to the task
/// and supervise the process in the background. Returns the run immediately
/// (`running` when the agent's slot was free, `queued` when it was taken);
/// the caller does not wait for the process.
pub async fn start(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    task: Task,
    agent: Agent,
) -> Result<Run, String> {
    // 1. Record the run and the assignment while holding the lock briefly.
    //    The slot is taken before the row is written so the status in the
    //    row matches reality.
    let run = {
        let guard = db.lock().map_err(|_| "db mutex poisoned".to_string())?;
        guard
            .assign_agent(task.id, agent.id)
            .map_err(|e| format!("database error: {e}"))?;
        let status = if slots.try_acquire(agent.id) {
            RunStatus::Running
        } else {
            RunStatus::Queued
        };
        guard
            .create_run(task.id, &agent, status)
            .map_err(|e| format!("database error: {e}"))?
    };

    // 2. A queued run has no process; the supervisor of the current run will
    //    pick it up when the slot frees.
    if run.status == RunStatus::Queued.as_str() {
        return Ok(run);
    }

    spawn_and_supervise(db, registry, slots, run.id, task, agent).await?;
    Ok(run)
}

/// Spawn the process for a run and start its supervisor. Used both for
/// fresh runs (slot was free) and for queued runs picked up later.
async fn spawn_and_supervise(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    run_id: i64,
    task: Task,
    agent: Agent,
) -> Result<(), String> {
    let prompt = render_prompt(agent.prompt_template.as_deref(), &task);
    let timeout_secs = agent
        .timeout_secs
        .filter(|t| *t > 0)
        .unwrap_or(DEFAULT_RUN_TIMEOUT_SECS);

    // The task's workdir is where the agent executes. It must exist at
    // spawn time; a missing directory is a failed run, not a spawn in the
    // wrong place.
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&agent.command)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(workdir) = task.workdir.as_deref() {
        command.current_dir(workdir);
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            // The run row already exists; do not leave it dangling as
            // "running" forever, and free the slot for the next queued run.
            mark_failed(&db, run_id, &format!("cannot spawn command: {e}"));
            release_and_pull_next(db.clone(), registry, slots, agent.id);
            return Err(format!("cannot spawn command: {e}"));
        }
    };

    // Feed the prompt on stdin, then close it so agents that read to EOF
    // (e.g. `cat`) terminate. A write error (typically EPIPE) means the
    // child exited without reading stdin — a legitimate configuration
    // (e.g. `true`); the run's outcome is the child's own exit status, so
    // supervision continues instead of failing the run.
    let mut stdin = child.stdin.take().expect("stdin was piped");
    if let Err(e) = stdin.write_all(prompt.as_bytes()).await {
        eprintln!("run {run_id}: agent closed stdin before the prompt was written: {e}");
    }
    let _ = stdin.shutdown().await;
    drop(stdin);

    // 3. Supervise in the background.
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
    registry.insert(run_id, cancel_tx);
    tokio::spawn(supervise(
        db,
        registry,
        slots,
        run_id,
        child,
        cancel_rx,
        timeout_secs,
        agent.id,
    ));

    Ok(())
}

/// Release the agent's slot and start the next queued run for that agent,
/// if any. Called whenever a run of the agent ends (or fails to spawn).
fn release_and_pull_next(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    agent_id: i64,
) {
    slots.release(agent_id);
    // Look for the next queued run of this agent. A queued run may have been
    // cancelled while waiting — start_run() only flips still-queued rows,
    // so a cancelled one is skipped naturally.
    let next = {
        let Ok(guard) = db.lock() else { return };
        match guard.next_queued_run(agent_id) {
            Ok(Some(run)) => match guard.start_run(run.id) {
                // The task may have been deleted while the run was queued;
                // the cascade removed the run row, so start_run reports
                // false. Skip it and try the next one.
                Ok(true) => Some(run),
                Ok(false) => None,
                Err(_) => None,
            },
            Ok(None) => None,
            Err(_) => None,
        }
    };
    let Some(run) = next else { return };
    // Re-read the task (it may have changed while queued) and the agent
    // (its command may have been edited), then spawn.
    let (task, agent) = {
        let Ok(guard) = db.lock() else { return };
        let task = match guard.get(run.task_id) {
            Ok(Some(t)) => t,
            _ => return,
        };
        let agent = match guard.get_agent(agent_id) {
            Ok(Some(a)) => a,
            _ => return,
        };
        (task, agent)
    };
    // The slot was released above; re-acquire it for this run. It cannot be
    // taken: only this code path starts runs for a freed slot, and it runs
    // once per release. (A concurrent `start` for the same agent would see
    // the slot busy only if it arrived before the release — in which case
    // its run is queued and will be picked up by the next release.)
    if !slots.try_acquire(agent_id) {
        // Should not happen; if it does, put the run back in the queue.
        let _ = {
            let Ok(guard) = db.lock() else { return };
            guard
                .requeue_run(run.id)
                .map(|_| ())
                .unwrap_or(())
        };
        return;
    }
    tokio::spawn(async move {
        if let Err(e) = spawn_and_supervise(db, registry, slots, run.id, task, agent).await {
            eprintln!("cannot start queued run {}: {e}", run.id);
        }
    });
}

/// Supervise one run: wait for exit, timeout or cancellation, then persist
/// the outcome, drop the registry entry and release the agent's slot
/// (starting the next queued run, if any).
async fn supervise(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    run_id: i64,
    mut child: tokio::process::Child,
    mut cancel: mpsc::Receiver<()>,
    timeout_secs: u64,
    agent_id: i64,
) {
    // Read both streams to EOF in the background, keeping only the tail so a
    // chatty process can never block on a full pipe.
    let mut stdout = child.stdout.take().expect("stdout was piped");
    let mut stderr = child.stderr.take().expect("stderr was piped");
    let out_task = tokio::spawn(async move {
        let a = read_tail(&mut stdout).await;
        let b = read_tail(&mut stderr).await;
        (a, b)
    });

    // Wait for the process, the timeout or a cancellation request.
    let outcome = tokio::select! {
        status = child.wait() => match status {
            Ok(status) => Outcome::Exited(status),
            Err(_) => Outcome::SpawnError,
        },
        _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => Outcome::TimedOut,
        _ = cancel.recv() => Outcome::Cancelled,
    };

    if matches!(outcome, Outcome::TimedOut | Outcome::Cancelled) {
        let _ = child.start_kill();
    }

    // The pipes close when the process dies, so the readers finish on their
    // own; a little slack for slow writers, then give up.
    let (out, err) = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        out_task,
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        _ => (Vec::new(), Vec::new()),
    };

    let mut output = out;
    output.extend_from_slice(&err);
    let output = truncate_tail(&output);

    let (status, exit_code) = match outcome {
        Outcome::Exited(status) => {
            let code = status.code().map(|c| c as i64);
            if status.success() {
                (RunStatus::Completed, code)
            } else {
                (RunStatus::Failed, code)
            }
        }
        Outcome::TimedOut => (RunStatus::TimedOut, None),
        Outcome::Cancelled => (RunStatus::Cancelled, None),
        Outcome::SpawnError => (RunStatus::Failed, None),
    };

    registry.remove(run_id);
    if let Ok(guard) = db.lock() {
        // A vanished row (task deleted mid-run, cascade) is fine.
        let _ = guard.finish_run(run_id, status, exit_code, &output);
    }

    // The slot frees with the run; hand it to the next queued run.
    release_and_pull_next(db, registry, slots, agent_id);
}

enum Outcome {
    Exited(std::process::ExitStatus),
    TimedOut,
    Cancelled,
    SpawnError,
}

/// Read a stream to EOF, keeping only the last [`MAX_RUN_OUTPUT_BYTES`].
async fn read_tail(r: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<u8> {
    let mut tail: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                tail.extend_from_slice(&chunk[..n]);
                if tail.len() > MAX_RUN_OUTPUT_BYTES {
                    let drop = tail.len() - MAX_RUN_OUTPUT_BYTES;
                    tail.drain(..drop);
                }
            }
        }
    }
    tail
}

/// Keep the tail of the combined output, byte-capped.
fn truncate_tail(bytes: &[u8]) -> String {
    if bytes.len() <= MAX_RUN_OUTPUT_BYTES {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        let start = bytes.len() - MAX_RUN_OUTPUT_BYTES;
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}

/// Mark a freshly created run as failed (spawn/write errors).
fn mark_failed(db: &SharedDb, run_id: i64, message: &str) {
    if let Ok(guard) = db.lock() {
        let _ = guard.finish_run(run_id, RunStatus::Failed, None, message);
    }
}

/// Mark runs that were `running` or `queued` when the server died as
/// failed. Called once at startup, before the web server accepts requests.
/// Queued runs are not re-queued: their tasks may have changed since, and
/// an honest "failed" beats a surprise process spawn on startup.
pub fn fail_orphaned_runs(db: &Db) {
    match db.unfinished_runs() {
        Ok(runs) => {
            for run in runs {
                let _ = db.finish_run(
                    run.id,
                    RunStatus::Failed,
                    None,
                    "server restarted while this run was waiting or active",
                );
            }
        }
        Err(e) => eprintln!("cannot check for orphaned runs: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_tail_keeps_the_end() {
        assert_eq!(truncate_tail(b"short"), "short");
        let long = vec![b'x'; MAX_RUN_OUTPUT_BYTES + 10];
        let kept = truncate_tail(&long);
        assert_eq!(kept.len(), MAX_RUN_OUTPUT_BYTES);
        assert!(kept.chars().all(|c| c == 'x'));
    }

    #[tokio::test]
    async fn read_tail_keeps_only_the_cap() {
        let mut data = vec![b'a'; MAX_RUN_OUTPUT_BYTES + 100];
        data.extend_from_slice(b"THE-END");
        let mut cursor = std::io::Cursor::new(data);
        let tail = read_tail(&mut cursor).await;
        assert_eq!(tail.len(), MAX_RUN_OUTPUT_BYTES);
        assert!(tail.ends_with(b"THE-END"));
    }

    /// Seed a real task and agent row so the foreign keys hold, and return
    /// them as the structs `start` expects.
    fn seed(db: &SharedDb, command: &str, template: Option<&str>, timeout: Option<u64>) -> (Task, Agent) {
        let guard = db.lock().unwrap();
        let task = guard
            .create(&crate::models::CreateTask {
                title: "say hi".into(),
                description: Some("".into()),
                status: None,
                priority: None,
                due_date: None,
                workdir: None,
            })
            .unwrap();
        let agent = guard
            .create_agent(&crate::models::CreateAgent {
                name: "tester".into(),
                command: command.into(),
                prompt_template: template.map(str::to_string),
                timeout_secs: timeout,
            })
            .unwrap();
        (task, agent)
    }

    fn setup() -> (SharedDb, SharedRegistry, std::sync::Arc<AgentSlots>) {
        let db: SharedDb = std::sync::Arc::new(Mutex::new(Db::open(":memory:").unwrap()));
        let registry: SharedRegistry = std::sync::Arc::new(RunRegistry::default());
        let slots = std::sync::Arc::new(AgentSlots::default());
        (db, registry, slots)
    }

    /// Poll a run until it leaves `not_status`, then return it.
    async fn wait_for(db: &SharedDb, run_id: i64, not_status: &str) -> Run {
        for _ in 0..400 {
            let run = {
                let guard = db.lock().unwrap();
                guard.get_run(run_id).unwrap().unwrap()
            };
            if run.status != not_status {
                return run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("run did not leave '{not_status}' in time");
    }

    /// Poll a run until it reaches `status` (or panic on timeout).
    async fn wait_until(db: &SharedDb, run_id: i64, status: &str) -> Run {
        for _ in 0..400 {
            let run = {
                let guard = db.lock().unwrap();
                guard.get_run(run_id).unwrap().unwrap()
            };
            if run.status == status {
                return run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("run did not reach '{status}' in time");
    }

    #[tokio::test]
    async fn a_full_run_lifecycle_against_a_real_command() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "cat", Some("hello {title}"), Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        assert_eq!(run.status, "running");

        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert_eq!(finished.exit_code, Some(0));
        assert!(finished.output.contains("hello say hi"));
        assert!(finished.ended_at.is_some());
    }

    #[tokio::test]
    async fn failing_command_is_recorded_as_failed() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "echo boom >&2; exit 3", None, Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "failed");
        assert_eq!(finished.exit_code, Some(3));
        assert!(finished.output.contains("boom"));
    }

    #[tokio::test]
    async fn timeout_kills_a_hung_command() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 60", None, Some(1));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "timed_out");
    }

    #[tokio::test]
    async fn cancel_stops_a_running_command() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 60", None, Some(60));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        assert!(registry.cancel(run.id));
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "cancelled");
    }

    #[tokio::test]
    async fn cancel_of_unknown_run_returns_false() {
        let registry = RunRegistry::default();
        assert!(!registry.cancel(999));
    }

    #[tokio::test]
    async fn second_run_of_the_same_agent_is_queued_and_runs_after() {
        let (db, registry, slots) = setup();
        // A long sleep holds the slot; cancelling it frees the slot at a
        // known moment, which makes the pickup deterministic.
        let (task, agent) = seed(&db, "sleep 30", None, Some(60));

        // First run takes the slot and runs.
        let first = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(first.status, "running");

        // Second run of the same agent must queue.
        let second = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(second.status, "queued");

        // A different agent is not affected by the busy slot.
        let (task2, agent2) = {
            let guard = db.lock().unwrap();
            let agent2 = guard
                .create_agent(&crate::models::CreateAgent {
                    name: "other".into(),
                    command: "true".into(),
                    prompt_template: None,
                    timeout_secs: Some(5),
                })
                .unwrap();
            (task.clone(), agent2)
        };
        let other = start(db.clone(), registry.clone(), slots.clone(), task2, agent2)
            .await
            .unwrap();
        assert_eq!(other.status, "running");

        // Make the queued run fast: a queued run re-reads the agent config
        // when it is picked up, so it runs the *current* command.
        {
            let mut guard = db.lock().unwrap();
            guard
                .update_agent(
                    agent.id,
                    &crate::models::UpdateAgent {
                        name: None,
                        command: Some("true".into()),
                        prompt_template: None,
                        timeout_secs: None,
                    },
                )
                .unwrap();
        }

        // Cancelling the first run frees the slot; the queued run is picked
        // up, starts and completes.
        assert!(registry.cancel(first.id));
        let first_done = wait_for(&db, first.id, "running").await;
        assert_eq!(first_done.status, "cancelled");
        let second_done = wait_until(&db, second.id, "completed").await;
        assert!(second_done.ended_at.is_some());
    }

    #[tokio::test]
    async fn cancelling_a_queued_run_skips_it_when_the_slot_frees() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 30", None, Some(60));

        let first = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        let second = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(second.status, "queued");

        // Cancel the queued run through the DB (no live process exists).
        {
            let guard = db.lock().unwrap();
            assert!(guard.cancel_run(second.id).unwrap());
        }

        // When the slot frees, the cancelled run is skipped, not started.
        assert!(registry.cancel(first.id));
        let first_done = wait_for(&db, first.id, "running").await;
        assert_eq!(first_done.status, "cancelled");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let second_row = {
            let guard = db.lock().unwrap();
            guard.get_run(second.id).unwrap().unwrap()
        };
        assert_eq!(second_row.status, "cancelled");
    }

    #[tokio::test]
    async fn workdir_is_used_as_the_process_working_directory() {
        let (db, registry, slots) = setup();
        let (mut task, agent) = seed(&db, "pwd", None, Some(5));
        task.workdir = Some("/tmp".to_string());

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert!(finished.output.trim().contains("tmp"), "pwd output: {}", finished.output);
    }

    #[tokio::test]
    async fn missing_workdir_fails_the_run_with_a_clear_message() {
        let (db, registry, slots) = setup();
        let (mut task, agent) = seed(&db, "pwd", None, Some(5));
        task.workdir = Some("/no/such/directory/anywhere".to_string());

        let result = start(db.clone(), registry.clone(), slots.clone(), task, agent).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("cannot spawn command"), "error was: {err}");
        // The run row was marked failed, not left dangling as running.
        let runs = {
            let guard = db.lock().unwrap();
            guard.list_runs(None, None, 10).unwrap()
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, "failed");
        assert!(runs[0].output.contains("cannot spawn command"));
    }
}

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
//! configured timeout, when one is set) or `cancelled` (killed via
//! [`RunRegistry::cancel`]).
//! Output (stdout + stderr) is captured up to [`MAX_RUN_OUTPUT_BYTES`],
//! keeping the tail — the interesting part of a log is usually the end.
//!
//! **Killing the whole tree:** the command runs as `sh -c <command>`, so the
//! direct child is usually just a shell wrapper around the real agent
//! process. Killing only the child would leave the agent running — the bug
//! this module guards against. Every run is therefore spawned in its **own
//! process group** ([`Command::process_group`]), and a kill (cancel, delete,
//! timeout) is delivered to the **group** (`kill(-pgid, SIGKILL)`), taking
//! the wrapper and every process it started down together. A side effect of
//! the separate group: the agent no longer receives the terminal's Ctrl-C,
//! so the server's graceful shutdown must kill live runs itself —
//! [`shutdown_all`] does.
//!
//! Every prompt is terminated with the [`END_OF_INPUT_MARKER`] line so
//! agents can detect the end of the task text without waiting for stdin
//! EOF (the runner closes stdin right after, so EOF still follows).
//!
//! **Live streaming:** while a run is live, its "conversation" — the prompt
//! the agent received plus every chunk of stdout/stderr as it is produced —
//! is available through [`stream_events`], which the web layer exposes as
//! Server-Sent Events. Late subscribers are caught up from a replay buffer
//! (tail-capped like the stored output) before the live chunks follow.
//!
//! Runs do not survive a server restart as processes: at startup,
//! [`fail_orphaned_runs`] flips still-`running` rows to `failed` and
//! re-queues nothing (a queued run's task may have changed since).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{broadcast, mpsc};

use crate::db::{Db, SharedDb};
use crate::models::{
    render_prompt, Agent, Run, RunStatus, Task, MAX_RUN_OUTPUT_BYTES,
};

/// Marker appended to every prompt so the agent can tell where the task
/// text ends — useful for agents that read stdin without waiting for EOF.
const END_OF_INPUT_MARKER: &str = "###END_OF_INPUT###";

/// The registry shared through the router state.
pub type SharedRegistry = std::sync::Arc<RunRegistry>;

/// One event of a live run's conversation, as sent to SSE subscribers.
#[derive(Debug, Clone)]
pub enum RunEvent {
    /// The prompt the agent received on stdin (replayed to late joiners).
    Prompt(String),
    /// A chunk of the agent's stdout/stderr, in arrival order.
    Output(String),
    /// A lifecycle note: "queued", "running", "completed", …
    Status(String),
    /// The conversation is over (the run reached a final status).
    End,
}

/// Live conversation state of one run: a replay buffer (so a subscriber
/// that joins mid-run sees what happened so far) plus a broadcast channel
/// for the chunks still to come. The buffer is capped like the stored
/// output — the interesting part of a log is usually the end.
pub struct RunStream {
    /// Sequence numbers of the events sent so far; the first event of a
    /// replayed batch carries this number so the client can dedupe.
    seq: AtomicU64,
    buffer: Mutex<Vec<(u64, RunEvent)>>,
    tx: broadcast::Sender<RunEvent>,
}

impl RunStream {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            seq: AtomicU64::new(0),
            buffer: Mutex::new(Vec::new()),
            tx,
        }
    }

    /// Record an event: append it to the replay buffer (dropping the head
    /// when it exceeds the cap) and broadcast it to live subscribers.
    fn push(&self, event: RunEvent) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut buffer) = self.buffer.lock() {
            buffer.push((seq, event.clone()));
            let mut total = buffer.iter().map(|(_, e)| event_len(e)).sum::<usize>();
            while total > MAX_RUN_OUTPUT_BYTES && buffer.len() > 1 {
                let (_, dropped) = buffer.remove(0);
                total -= event_len(&dropped);
            }
        }
        let _ = self.tx.send(event); // no subscribers is fine
    }

    /// Everything recorded so far, oldest first.
    fn replay(&self) -> Vec<(u64, RunEvent)> {
        self.buffer.lock().map(|b| b.clone()).unwrap_or_default()
    }

    /// A receiver for the events still to come.
    fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.tx.subscribe()
    }
}

/// Rough size of an event for the replay-buffer cap.
fn event_len(event: &RunEvent) -> usize {
    match event {
        RunEvent::Prompt(s) | RunEvent::Output(s) | RunEvent::Status(s) => s.len(),
        RunEvent::End => 0,
    }
}

/// Live runs by id: the cancel channel of each supervisor task, plus the
/// conversation stream of runs that are queued or running. Entries are
/// added by [`start`] and removed by the supervisor when the run ends, so a
/// missing entry means "no live process for this run".
#[derive(Default)]
pub struct RunRegistry {
    senders: Mutex<HashMap<i64, mpsc::Sender<()>>>,
    streams: Mutex<HashMap<i64, std::sync::Arc<RunStream>>>,
    /// Fires when the last live supervisor finishes — the shutdown hook
    /// waits on it so killed processes are really gone before the exit.
    idle: tokio::sync::Notify,
    /// The spawn gate: `true` once the server is shutting down. The mutex
    /// is held across every check-spawn-register sequence (see
    /// [`RunRegistry::spawn_or_refuse`]), so a shutdown that closes the
    /// gate can never miss a run that is mid-spawn: either the run
    /// registered before the gate closed — [`RunRegistry::cancel_all`]
    /// then reaches it — or it is refused and no process is created.
    /// Without the gate, a run started by an in-flight request during the
    /// shutdown would never be cancelled and the shutdown would hang.
    shutting_down: Mutex<bool>,
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

    /// Ask **every** live run to stop. Used by the server's graceful
    /// shutdown: agent processes run in their own process groups (see
    /// [`kill_tree`]), so they no longer receive the terminal's Ctrl-C —
    /// the server must kill them explicitly or they would outlive it.
    ///
    /// Closes the spawn gate first (under its lock), so no run can slip
    /// into existence between this sweep and the exit: a concurrent spawn
    /// either registered before the gate closed — and is cancelled here —
    /// or is refused by the gate and never creates a process.
    pub fn cancel_all(&self) {
        if let Ok(mut gate) = self.shutting_down.lock() {
            *gate = true;
        }
        if let Ok(map) = self.senders.lock() {
            for tx in map.values() {
                let _ = tx.try_send(());
            }
        }
    }

    /// The spawn gate. `Ok(())` when the server is live and the run may be
    /// spawned; `Err` when it is shutting down — the run must not start.
    /// The gate lock is held by the caller for the whole check-spawn-
    /// register sequence (see [`spawn_and_supervise`]), which is what makes
    /// the shutdown race-free.
    pub fn spawn_or_refuse(&self) -> Result<std::sync::MutexGuard<'_, bool>, String> {
        let gate = self
            .shutting_down
            .lock()
            .map_err(|_| "registry mutex poisoned".to_string())?;
        if *gate {
            return Err("server is shutting down, the run was not started".to_string());
        }
        Ok(gate)
    }

    fn insert(&self, run_id: i64, tx: mpsc::Sender<()>) {
        if let Ok(mut map) = self.senders.lock() {
            map.insert(run_id, tx);
        }
    }

    fn remove(&self, run_id: i64) {
        let now_empty = if let Ok(mut map) = self.senders.lock() {
            map.remove(&run_id);
            map.is_empty()
        } else {
            false
        };
        // The last supervisor just finished: wake the shutdown hook (if it
        // is waiting) so the server can exit with every process reaped.
        if now_empty {
            self.idle.notify_waiters();
        }
    }

    /// Resolve when no live supervisor is left. Returns immediately when
    /// the registry is already empty; otherwise waits for the last
    /// supervisor to finish (each removal re-checks, so a run that ends
    /// while waiting is noticed).
    pub async fn wait_idle(&self) {
        loop {
            if self.senders.lock().map(|m| m.is_empty()).unwrap_or(true) {
                return;
            }
            // notify_waiters() only wakes current waiters, so re-check in
            // a loop; a short sleep bounds the wait if a notification is
            // missed between the check and the sleep.
            tokio::select! {
                _ = self.idle.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }

    /// The conversation stream of a run that is queued or running, if it
    /// is live in this server process.
    pub fn stream(&self, run_id: i64) -> Option<std::sync::Arc<RunStream>> {
        self.streams.lock().ok().and_then(|m| m.get(&run_id).cloned())
    }

    /// Everything recorded so far for a live run, plus a receiver for the
    /// events still to come. The web layer uses this to serve SSE: replay
    /// first, then live events. `None` when the run is not live (it either
    /// finished — its stored output is served instead — or never existed).
    pub fn subscribe(&self, run_id: i64) -> Option<(Vec<(u64, RunEvent)>, broadcast::Receiver<RunEvent>)> {
        let stream = self.stream(run_id)?;
        Some((stream.replay(), stream.subscribe()))
    }

    /// Register the stream of a run that is about to be queued or spawned.
    /// The supervisor removes it when the run reaches a final status.
    fn insert_stream(&self, run_id: i64, stream: std::sync::Arc<RunStream>) {
        if let Ok(mut map) = self.streams.lock() {
            map.insert(run_id, stream);
        }
    }

    fn remove_stream(&self, run_id: i64) {
        if let Ok(mut map) = self.streams.lock() {
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

    // 2. The conversation stream exists from the moment the run row does,
    //    so a subscriber can attach to a queued run and watch it start.
    //    A queued run has no process; the supervisor of the current run
    //    picks it up when the slot frees.
    let stream = std::sync::Arc::new(RunStream::new());
    registry.insert_stream(run.id, stream.clone());
    if run.status == RunStatus::Queued.as_str() {
        stream.push(RunEvent::Status("queued".to_string()));
        return Ok(run);
    }

    spawn_and_supervise(db, registry, slots, run.id, task, agent, stream).await?;
    Ok(run)
}

/// The effective timeout of a run: `Some(secs)` arms a timer, `None` means
/// the run has no timeout at all (it ends only by exit or cancellation).
/// A configured 0 counts as "no timeout" — the same value the dashboard
/// sends for an explicitly empty field.
fn resolve_timeout(configured: Option<u64>) -> Option<u64> {
    configured.filter(|t| *t > 0)
}

/// Spawn the process for a run and start its supervisor. Used both for
/// fresh runs (slot was free) and for queued runs picked up later.
///
/// **Shutdown safety:** the cancel sender is registered in the registry
/// *before* the process is spawned, and both happen under the registry's
/// spawn gate. A shutdown therefore can never miss a run that is mid-spawn:
/// it either sees the sender (and its cancel is buffered — the supervisor
/// kills the tree the moment it starts) or the gate refuses the spawn and
/// no process is created. Without the gate, a run started by an in-flight
/// request during the shutdown would never be cancelled and the shutdown
/// would hang (or worse, the process would outlive the server).
async fn spawn_and_supervise(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    run_id: i64,
    task: Task,
    agent: Agent,
    stream: std::sync::Arc<RunStream>,
) -> Result<(), String> {
    let prompt = render_prompt(agent.prompt_template.as_deref(), &task);
    // Append the end-of-input marker unless the rendered prompt already
    // ends with it (a template may include it deliberately). Either way
    // the prompt ends with a newline so the marker is a line of its own.
    let prompt = if prompt.trim_end().ends_with(END_OF_INPUT_MARKER) {
        format!("{}\n", prompt.trim_end())
    } else {
        format!("{prompt}\n{END_OF_INPUT_MARKER}\n")
    };
    // The timeout is optional: absent (or 0) means the process is only
    // ended by its own exit or a cancellation — no timer is armed.
    let timeout_secs = resolve_timeout(agent.timeout_secs);
    // The conversation opens with the run starting and the prompt the
    // agent received — a subscriber sees what the agent was asked to do
    // before its output arrives. (Emitted here rather than in `supervise`
    // so a queued run's pickup does not duplicate it.)
    stream.push(RunEvent::Status("running".to_string()));
    stream.push(RunEvent::Prompt(prompt.clone()));

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
    // Own process group: the direct child is only the `sh -c` wrapper, so a
    // kill of the child alone would leave the real agent process running.
    // The group lets kill_tree() take the wrapper and everything it spawned
    // down in one signal. (On non-Unix there is no process-group API; the
    // kill falls back to the child alone.)
    #[cfg(unix)]
    command.process_group(0);

    // Register the cancel sender and spawn the process, both under the
    // gate (no `.await` inside — the guard is not held across one).
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
    let child = {
        let _gate = match registry.spawn_or_refuse() {
            Ok(gate) => gate,
            Err(e) => {
                // The server is shutting down: no process exists, but the
                // run row does — it must not dangle as "running".
                fail_spawn(&db, &registry, &slots, &stream, run_id, agent.id, &e);
                return Err(e);
            }
        };
        registry.insert(run_id, cancel_tx);
        command.spawn()
    };
    let mut child = match child {
        Ok(child) => child,
        Err(e) => {
            // The sender was registered above; without a supervisor it
            // would hang the shutdown's wait for idle runs, so remove it.
            registry.remove(run_id);
            let message = format!("cannot spawn command: {e}");
            fail_spawn(&db, &registry, &slots, &stream, run_id, agent.id, &message);
            return Err(message);
        }
    };

    // Take stdin out before the supervisor takes ownership of the child.
    let mut stdin = child.stdin.take().expect("stdin was piped");
    // The supervisor is started before the prompt is written so that a
    // buffered cancellation (a shutdown raced the spawn) kills the tree
    // even while the write is still in flight — a blocked write on a
    // full pipe then fails with EPIPE instead of hanging forever.
    let child_pid = child.id();
    tokio::spawn(supervise(
        db,
        registry,
        slots,
        run_id,
        child_pid,
        child,
        cancel_rx,
        timeout_secs,
        agent.id,
        stream,
    ));

    // Feed the prompt on stdin, then close it so agents that read to EOF
    // (e.g. `cat`) terminate. A write error (typically EPIPE) means the
    // child exited without reading stdin — a legitimate configuration
    // (e.g. `true`); the run's outcome is the child's own exit status, so
    // supervision continues instead of failing the run.
    if let Err(e) = stdin.write_all(prompt.as_bytes()).await {
        eprintln!("run {run_id}: agent closed stdin before the prompt was written: {e}");
    }
    let _ = stdin.shutdown().await;
    drop(stdin);

    Ok(())
}

/// Record a spawn-stage failure (spawn error, or a refusal because the
/// server is shutting down): the run row must not dangle as `running`, the
/// conversation stream must end, and the agent's slot must be freed for the
/// next queued run (of which there is none during a shutdown — the queue
/// was cancelled before the gate closed).
fn fail_spawn(
    db: &SharedDb,
    registry: &SharedRegistry,
    slots: &std::sync::Arc<AgentSlots>,
    stream: &std::sync::Arc<RunStream>,
    run_id: i64,
    agent_id: i64,
    message: &str,
) {
    mark_failed(db, run_id, message);
    stream.push(RunEvent::Status("failed".to_string()));
    stream.push(RunEvent::End);
    registry.remove_stream(run_id);
    release_and_pull_next(db.clone(), registry.clone(), slots.clone(), agent_id);
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
    // (its command may have been edited), then spawn. The run row is
    // re-checked as well: it may have been cancelled or deleted after the
    // flip to `running` but before this spawn (the flip and the spawn are
    // not one atomic step) — such a run must not start.
    let (task, agent, stream) = {
        let Ok(guard) = db.lock() else { return };
        match guard.get_run(run.id) {
            Ok(Some(r)) if r.status == "running" => {}
            _ => return, // deleted or no longer running — do not spawn
        }
        let task = match guard.get(run.task_id) {
            Ok(Some(t)) => t,
            _ => return,
        };
        let agent = match guard.get_agent(agent_id) {
            Ok(Some(a)) => a,
            _ => return,
        };
        // The stream was registered when the run was created; it is still
        // there because the run is still live (checked above).
        let stream = match registry.stream(run.id) {
            Some(s) => s,
            None => return,
        };
        (task, agent, stream)
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
        if let Err(e) = spawn_and_supervise(db, registry, slots, run.id, task, agent, stream).await {
            eprintln!("cannot start queued run {}: {e}", run.id);
        }
    });
}

/// Supervise one run: wait for exit, timeout or cancellation, then persist
/// the outcome, drop the registry entry and release the agent's slot
/// (starting the next queued run, if any). `timeout_secs` is `None` when
/// the agent has no timeout configured — the run then ends only by exit or
/// cancellation.
async fn supervise(
    db: SharedDb,
    registry: SharedRegistry,
    slots: std::sync::Arc<AgentSlots>,
    run_id: i64,
    child_pid: Option<u32>,
    mut child: tokio::process::Child,
    mut cancel: mpsc::Receiver<()>,
    timeout_secs: Option<u64>,
    agent_id: i64,
    stream: std::sync::Arc<RunStream>,
) {
    // Read both streams to EOF in the background, keeping only the tail so a
    // chatty process can never block on a full pipe. Every chunk is also
    // pushed into the live stream as it arrives.
    let mut stdout = child.stdout.take().expect("stdout was piped");
    let mut stderr = child.stderr.take().expect("stderr was piped");
    let stream_for_io = stream.clone();
    let out_task = tokio::spawn(async move {
        let a = read_tail(&mut stdout, Some(&stream_for_io)).await;
        let b = read_tail(&mut stderr, Some(&stream_for_io)).await;
        (a, b)
    });

    // Wait for the process, the timeout (when one is configured) or a
    // cancellation request. Without a timeout the timer arm never fires:
    // `pending()` is a future that never resolves.
    let outcome = tokio::select! {
        status = child.wait() => match status {
            Ok(status) => Outcome::Exited(status),
            Err(_) => Outcome::SpawnError,
        },
        _ = async {
            match timeout_secs {
                Some(secs) => tokio::time::sleep(std::time::Duration::from_secs(secs)).await,
                None => std::future::pending::<()>().await,
            }
        } => Outcome::TimedOut,
        _ = cancel.recv() => Outcome::Cancelled,
    };

    if matches!(outcome, Outcome::TimedOut | Outcome::Cancelled) {
        kill_tree(child_pid, &mut child);
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

    // The conversation is over: tell the subscribers, then stop serving
    // the stream (a late subscriber falls back to the stored output).
    stream.push(RunEvent::Status(status.as_str().to_string()));
    stream.push(RunEvent::End);
    registry.remove_stream(run_id);

    // The slot frees with the run; hand it to the next queued run.
    release_and_pull_next(db, registry, slots, agent_id);
}

enum Outcome {
    Exited(std::process::ExitStatus),
    TimedOut,
    Cancelled,
    SpawnError,
}

/// Kill a run's whole process tree. The direct child is the `sh -c`
/// wrapper; the real agent is a grandchild it spawned, so killing only the
/// child would leave the agent running (the bug this fixes). Every run is
/// spawned in its own process group, so one signal to the group takes the
/// wrapper, the agent and anything else the command started down together.
///
/// `pid` is the wrapper's pid, captured at spawn time (it doubles as the
/// process-group id). The child is killed as well (not just the group) so
/// the exit is reaped and the run reaches a final status even on platforms
/// without process groups, where the group kill is a no-op.
fn kill_tree(pid: Option<u32>, child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        // The child leads its own group (process_group(0) at spawn), so
        // its pid doubles as the pgid. ESRCH means the group is already
        // gone — the child exited between the outcome and here; not an
        // error.
        // SAFETY: plain kill(2) with a checked pid.
        unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    }
    let _ = child.start_kill();
    #[cfg(not(unix))]
    let _ = pid;
}

/// Read a stream to EOF, keeping only the last [`MAX_RUN_OUTPUT_BYTES`].
/// When a live stream is given, every chunk is also pushed to it as it
/// arrives (lossy-converted to a string — the stored tail keeps the exact
/// bytes; the live view is for humans watching).
async fn read_tail(
    r: &mut (impl tokio::io::AsyncRead + Unpin),
    stream: Option<&std::sync::Arc<RunStream>>,
) -> Vec<u8> {
    let mut tail: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if let Some(stream) = stream {
                    stream.push(RunEvent::Output(
                        String::from_utf8_lossy(&chunk[..n]).into_owned(),
                    ));
                }
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

/// Stop every live run on the way out. Called by the server's graceful
/// shutdown, after the listener stopped accepting requests.
///
/// Agent processes run in their own process groups (see [`kill_tree`]), so
/// they do not receive the terminal's Ctrl-C — without this call they would
/// outlive the server. The order matters:
///
/// 1. flip queued rows to `cancelled` first — otherwise a supervisor that
///    releases its slot during the shutdown would pick the next queued run
///    up and spawn a fresh process on the way out;
/// 2. then ask every live supervisor to kill its tree. The supervisors
///    finish asynchronously; the shutdown waits for them (see
///    [`RunRegistry::wait_idle`]) so the processes are really gone before
///    the server exits.
pub async fn shutdown_all(db: SharedDb, registry: SharedRegistry) {
    if let Ok(guard) = db.lock() {
        // Best effort: a database error must not block the shutdown.
        let _ = guard.cancel_queued_runs();
    }
    registry.cancel_all();
    registry.wait_idle().await;
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
        let tail = read_tail(&mut cursor, None).await;
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
    async fn the_prompt_travels_on_stdin_never_on_the_command_line() {
        let (db, registry, slots) = setup();
        // The command ignores its arguments entirely: if the prompt were
        // passed on the command line, the output would be empty.
        let (task, agent) = seed(&db, "cat", Some("PROMPT {title}"), Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert_eq!(finished.output, "PROMPT say hi\n###END_OF_INPUT###\n");
    }

    #[tokio::test]
    async fn the_end_of_input_marker_is_appended_to_every_prompt() {
        let (db, registry, slots) = setup();
        // `cat` echoes stdin verbatim, so the output shows exactly what the
        // agent received: the rendered prompt followed by the marker.
        let (task, agent) = seed(&db, "cat", None, Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert!(finished.output.ends_with("###END_OF_INPUT###\n"), "output: {}", finished.output);
    }

    #[tokio::test]
    async fn a_prompt_that_already_ends_with_the_marker_is_not_duplicated() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "cat", Some("do it\n###END_OF_INPUT###"), Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert_eq!(finished.output, "do it\n###END_OF_INPUT###\n");
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
    async fn timeout_zero_means_no_timer_at_all() {
        let (db, registry, slots) = setup();
        // A command that finishes on its own after a moment: with a 1 s
        // timer it would be killed (timed_out), with no timer it completes.
        // (The old semantics — 0 -> 600 s default — would also let this
        // pass, so the resolution matrix is pinned separately below.)
        let (task, agent) = seed(&db, "sleep 2; echo done", None, Some(0));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "completed");
        assert_eq!(finished.exit_code, Some(0));
        assert!(finished.output.contains("done"));
    }

    #[test]
    fn timeout_resolution_absent_and_zero_mean_no_timer() {
        // The timeout is optional: absent (None) and an explicit 0 both
        // mean "no timeout"; only a positive value arms a timer.
        assert_eq!(resolve_timeout(None), None);
        assert_eq!(resolve_timeout(Some(0)), None);
        assert_eq!(resolve_timeout(Some(1)), Some(1));
        assert_eq!(resolve_timeout(Some(600)), Some(600));
    }

    #[tokio::test]
    async fn a_run_without_timeout_can_still_be_cancelled() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 60", None, Some(0));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        assert!(registry.cancel(run.id));
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "cancelled");
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

    // ---------- killing the whole process tree ----------

    /// Is a pid alive? `kill(pid, 0)` probes without sending a signal.
    #[cfg(unix)]
    fn pid_alive(pid: i32) -> bool {
        // SAFETY: signal 0 only checks permission/existence.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    /// Wait until a pid is gone (or panic on timeout). A killed process
    /// disappears asynchronously, so a single check is not enough.
    #[cfg(unix)]
    async fn wait_for_pid_gone(pid: i32) {
        for _ in 0..200 {
            if !pid_alive(pid) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("pid {pid} is still alive");
    }

    /// The regression test for the bug this task fixes: cancelling a run
    /// must kill the **agent process**, not just the `sh -c` wrapper around
    /// it. The command backgrounds a `sleep` (the "agent", a grandchild)
    /// and then `wait`s, so the wrapper stays alive and the run is live —
    /// but a kill of the child alone would leave the grandchild running.
    /// After the cancel, the grandchild's pid must be gone.
    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_kills_the_agent_process_not_just_the_wrapper() {
        let (db, registry, slots) = setup();
        let marker = tree_test_marker();
        let (task, agent) = seed(&db, &tree_test_command(&marker), None, Some(0));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        assert_eq!(run.status, "running");
        let grandchild = wait_for_grandchild_pid(&marker).await;
        assert!(pid_alive(grandchild), "the grandchild must be alive before the cancel");

        // Cancel: the wrapper is alive, but only the group kill also
        // reaches the grandchild.
        assert!(registry.cancel(run.id));
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "cancelled");
        wait_for_pid_gone(grandchild).await;
        let _ = std::fs::remove_file(&marker);
    }

    /// Same guarantee for the timeout: a hung agent (a grandchild) must be
    /// killed when the run times out, not just its `sh -c` wrapper.
    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_the_agent_process_not_just_the_wrapper() {
        let (db, registry, slots) = setup();
        let marker = tree_test_marker();
        let (task, agent) = seed(&db, &tree_test_command(&marker), None, Some(1));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        let grandchild = wait_for_grandchild_pid(&marker).await;

        // The 1 s timeout fires; the group kill must reach the grandchild.
        let finished = wait_for(&db, run.id, "running").await;
        assert_eq!(finished.status, "timed_out");
        wait_for_pid_gone(grandchild).await;
        let _ = std::fs::remove_file(&marker);
    }

    /// A unique marker file path per test invocation, so parallel tests
    /// never share one.
    #[cfg(unix)]
    fn tree_test_marker() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "orga-rs-tree-test-{}-{}.pid",
            std::process::id(),
            run_marker_seq()
        ))
    }

    /// The command under test: it backgrounds a `sleep 300` (the "agent"
    /// grandchild), reports that pid to the marker file, then `wait`s —
    /// the wrapper stays alive (the run is `running`) while the grandchild
    /// runs. A child-only kill would orphan the grandchild: the reported
    /// bug.
    #[cfg(unix)]
    fn tree_test_command(marker: &std::path::Path) -> String {
        format!("sleep 300 & echo $! > \"{}\"; wait", marker.to_string_lossy())
    }

    /// Poll the marker file until the grandchild has reported its pid.
    #[cfg(unix)]
    async fn wait_for_grandchild_pid(marker: &std::path::Path) -> i32 {
        for _ in 0..200 {
            if let Ok(text) = std::fs::read_to_string(marker) {
                if let Ok(pid) = text.trim().parse::<i32>() {
                    return pid;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("the grandchild never reported its pid");
    }

    /// The shutdown sweep must kill live runs (their process groups do not
    /// receive the terminal's Ctrl-C) and must not start queued ones.
    #[tokio::test]
    async fn shutdown_all_kills_live_runs_and_cancels_queued_ones() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 300", None, Some(0));

        // A live run and a queued run of the same agent.
        let live = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(live.status, "running");
        let queued = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(queued.status, "queued");

        shutdown_all(db.clone(), registry.clone()).await;

        // The live run was killed and recorded as cancelled.
        let live_row = {
            let guard = db.lock().unwrap();
            guard.get_run(live.id).unwrap().unwrap()
        };
        assert_eq!(live_row.status, "cancelled");
        // The queued run was cancelled, not started.
        let queued_row = {
            let guard = db.lock().unwrap();
            guard.get_run(queued.id).unwrap().unwrap()
        };
        assert_eq!(queued_row.status, "cancelled");
        // The registry is idle: every supervisor finished.
        assert!(registry.senders.lock().unwrap().is_empty());
        // And the gate is closed: a fresh run is refused, not spawned.
        let (task2, agent2) = seed(&db, "true", None, Some(5));
        let refused = start(db.clone(), registry.clone(), slots.clone(), task2, agent2).await;
        assert!(refused.is_err(), "a run after shutdown must be refused");
    }

    /// A unique suffix per test invocation, so parallel tests never share
    /// a marker file.
    fn run_marker_seq() -> u64 {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

    // ---------- live conversation streaming ----------

    /// Collect the events of a live run's stream until it ends (or the
    /// poll budget runs out), then return them.
    async fn collect_stream(registry: &SharedRegistry, run_id: i64) -> Vec<RunEvent> {
        let (replay, mut live) = registry.subscribe(run_id).expect("run is live");
        let mut events: Vec<RunEvent> = replay.into_iter().map(|(_, e)| e).collect();
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(500), live.recv()).await {
                Ok(Ok(event)) => {
                    let over = matches!(event, RunEvent::End);
                    events.push(event);
                    if over {
                        return events;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                    events.push(RunEvent::Status("lagged".into()));
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return events,
                Err(_) => return events, // poll budget exhausted
            }
        }
    }

    /// The event kinds in order, for compact assertions. Status events are
    /// shown by their status string (queued/running/completed/…).
    fn kinds(events: &[RunEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                RunEvent::Prompt(_) => "prompt".to_string(),
                RunEvent::Output(_) => "output".to_string(),
                RunEvent::Status(s) => s.clone(),
                RunEvent::End => "end".to_string(),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_running_run_streams_prompt_output_and_end() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "cat", Some("hello {title}"), Some(5));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();

        let events = collect_stream(&registry, run.id).await;
        assert_eq!(
            kinds(&events),
            vec![
                "running".to_string(),
                "prompt".to_string(),
                "output".to_string(),
                "completed".to_string(),
                "end".to_string()
            ]
        );
        // The prompt is exactly what the agent received on stdin.
        assert!(matches!(&events[1], RunEvent::Prompt(p) if p.contains("hello say hi")));
        // The output chunk carries the agent's stdout.
        assert!(matches!(&events[2], RunEvent::Output(o) if o.contains("hello say hi")));
    }

    #[tokio::test]
    async fn a_late_subscriber_sees_the_replay_then_the_rest() {
        let (db, registry, slots) = setup();
        // A slow command keeps the run alive while the subscriber attaches.
        let (task, agent) = seed(&db, "echo first; sleep 0.4; echo second", None, Some(10));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        // Wait for the first output chunk to exist, then subscribe — the
        // replay must contain it.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let events = collect_stream(&registry, run.id).await;
        let joined: String = events
            .iter()
            .filter_map(|e| match e {
                RunEvent::Output(o) => Some(o.as_str()),
                _ => None,
            })
            .collect();
        // The replayed "first" is not lost, and "second" arrives live.
        assert!(joined.contains("first"), "joined: {joined}");
        assert!(joined.contains("second"), "joined: {joined}");
        assert!(events.iter().any(|e| matches!(e, RunEvent::End)));
    }

    #[tokio::test]
    async fn a_queued_run_streams_from_queued_to_end() {
        let (db, registry, slots) = setup();
        // Hold the slot with a long sleep; the second run queues.
        let (task, agent) = seed(&db, "sleep 30", None, Some(60));
        let first = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        let second = start(db.clone(), registry.clone(), slots.clone(), task.clone(), agent.clone())
            .await
            .unwrap();
        assert_eq!(second.status, "queued");

        // Make the queued run fast (it re-reads the config at pickup).
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

        // The queued run's stream opens with the queued note.
        let (replay, mut live) = registry.subscribe(second.id).expect("queued run is live");
        assert!(replay
            .iter()
            .any(|(_, e)| matches!(e, RunEvent::Status(s) if s == "queued")));

        // Free the slot; the queued run starts and its stream follows.
        assert!(registry.cancel(first.id));
        let mut events: Vec<RunEvent> = replay.into_iter().map(|(_, e)| e).collect();
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(2), live.recv()).await {
                Ok(Ok(event)) => {
                    let over = matches!(event, RunEvent::End);
                    events.push(event);
                    if over {
                        break;
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                _ => panic!("queued run's stream did not end in time"),
            }
        }
        assert_eq!(
            kinds(&events),
            vec![
                "queued".to_string(),
                "running".to_string(),
                "prompt".to_string(),
                "completed".to_string(),
                "end".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn the_stream_ends_when_the_run_is_cancelled() {
        let (db, registry, slots) = setup();
        let (task, agent) = seed(&db, "sleep 60", None, Some(60));

        let run = start(db.clone(), registry.clone(), slots.clone(), task, agent)
            .await
            .unwrap();
        assert!(registry.cancel(run.id));

        let events = collect_stream(&registry, run.id).await;
        assert!(events.iter().any(|e| matches!(e, RunEvent::Status(s) if s == "cancelled")));
        assert!(events.iter().any(|e| matches!(e, RunEvent::End)));
        // The stream is gone once the run ended.
        assert!(registry.subscribe(run.id).is_none());
    }

    #[test]
    fn the_replay_buffer_keeps_only_the_tail() {
        // The replay buffer is capped like the stored output: a late
        // subscriber sees the last ~64 KiB, not the whole conversation.
        let stream = RunStream::new();
        stream.push(RunEvent::Status("running".into()));
        // ~200 bytes per chunk × 1000 chunks ≈ 200 KB, well over the cap.
        for i in 0..1000 {
            stream.push(RunEvent::Output(format!(
                "chunk {i} {}",
                "x".repeat(200)
            )));
        }
        let replay = stream.replay();
        let total: usize = replay.iter().map(|(_, e)| event_len(e)).sum();
        assert!(total <= MAX_RUN_OUTPUT_BYTES + 1024, "total: {total}");
        // The oldest chunks were dropped; the newest are all there.
        let texts: Vec<&str> = replay
            .iter()
            .filter_map(|(_, e)| match e {
                RunEvent::Output(o) => Some(o.as_str()),
                _ => None,
            })
            .collect();
        assert!(!texts.iter().any(|t| t.contains("chunk 0 ")), "the head was dropped");
        assert!(texts.iter().any(|t| t.contains("chunk 999 ")), "the tail is kept");
    }
}

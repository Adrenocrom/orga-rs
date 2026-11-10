use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard};

use axum::{
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

use crate::db::{Db, SharedDb, UpdateError};
use crate::models::{
    check_enum, check_workdir, Agent, CreateAgent, CreateTask, Run, Stats, Task, UpdateAgent,
    UpdateTask, PRIORITIES, RUN_STATUSES, STATUSES,
};
use crate::runner::{self, AgentSlots, RunEvent, RunRegistry, SharedRegistry};

/// Router state: the database, the live-run registry, the per-agent
/// execution slots (one live run per agent) and the path of the workdirs
/// config file (re-read per request, so edits apply without a restart).
#[derive(Clone)]
pub struct AppState {
    pub db: SharedDb,
    pub registry: SharedRegistry,
    pub slots: std::sync::Arc<AgentSlots>,
    pub workdirs: std::path::PathBuf,
}

pub async fn serve(
    db: SharedDb,
    addr: SocketAddr,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Dashboard listening on http://{addr}");
    println!("MCP clients: run `orga-rs mcp` with ORGA_URL=http://{addr}");
    if token.is_some() {
        println!("Auth enabled: API requests must send 'Authorization: Bearer <ORGA_TOKEN>'");
    }
    let state = AppState {
        db,
        registry: Arc::new(RunRegistry::default()),
        slots: Arc::new(AgentSlots::default()),
        workdirs: crate::workdirs_path(),
    };
    axum::serve(listener, router(state, token))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolve on Ctrl-C / SIGTERM: stop accepting connections and let in-flight
/// requests finish before the process exits.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// The full app: dashboard page + JSON API. Exposed for integration tests.
/// `token`, when set, guards every `/api/*` route (see [`require_token`]);
/// the dashboard page itself stays open because it contains no data.
pub fn router(state: AppState, token: Option<String>) -> Router {
    let api = Router::new()
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route(
            "/api/tasks/{id}",
            get(get_task).put(update_task).delete(delete_task),
        )
        .route(
            "/api/tasks/{id}/run",
            post(run_task),
        )
        .route(
            "/api/tasks/{id}/agent",
            axum::routing::put(assign_agent).delete(unassign_agent),
        )
        .route(
            "/api/tasks/{id}/workdir",
            axum::routing::put(set_workdir).delete(clear_workdir),
        )
        .route("/api/agents", get(list_agents).post(create_agent))
        .route(
            "/api/agents/{id}",
            get(get_agent).put(update_agent).delete(delete_agent),
        )
        .route("/api/runs", get(list_runs))
        .route(
            "/api/runs/{id}",
            get(get_run).delete(delete_run),
        )
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/runs/{id}/stream", get(stream_run))
        .route("/api/workdirs", get(list_workdirs).post(add_workdir).delete(remove_workdir))
        .route("/api/stats", get(get_stats))
        .route_layer(middleware::from_fn_with_state(token, require_token))
        .with_state(state);
    Router::new().route("/", get(dashboard)).merge(api)
}

/// Bearer-token check for `/api/*`, active when the server was started with
/// `ORGA_TOKEN`. Without the variable set, every request passes through.
async fn require_token(State(token): State<Option<String>>, req: Request, next: Next) -> Response {
    let Some(expected) = token else {
        return next.run(req).await;
    };
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match provided {
        Some(p) if token_matches(&expected, p) => next.run(req).await,
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "missing or invalid Authorization header; expected 'Bearer <ORGA_TOKEN>'"
            })),
        )
            .into_response(),
    }
}

/// Constant-time-ish comparison so the token is not leaked through timing.
fn token_matches(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .bytes()
        .zip(provided.bytes())
        .map(|(a, b)| a ^ b)
        .fold(0u8, |acc, diff| acc | diff)
        == 0
}

// ---------- HTML dashboard ----------

async fn dashboard() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

// ---------- REST API ----------

#[derive(Deserialize)]
struct ListQuery {
    status: Option<String>,
    priority: Option<String>,
}

async fn list_tasks(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<Task>>, AppError> {
    // Reject unknown filter values instead of silently matching nothing —
    // a typo in a script (or an LLM tool call) would otherwise look like
    // "no tasks".
    check_enum("status", q.status.as_deref(), &STATUSES).map_err(AppError::BadRequest)?;
    check_enum("priority", q.priority.as_deref(), &PRIORITIES).map_err(AppError::BadRequest)?;
    let tasks = lock_db(&state.db)?.list(q.status.as_deref(), q.priority.as_deref())?;
    Ok(Json(tasks))
}

async fn create_task(
    State(state): State<AppState>,
    Json(input): Json<CreateTask>,
) -> Result<(StatusCode, Json<Task>), AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let task = lock_db(&state.db)?.create(&input)?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Task>, AppError> {
    lock_db(&state.db)?.get(id)?.map(Json).ok_or(AppError::NotFound)
}

async fn update_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<UpdateTask>,
) -> Result<Json<Task>, AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let mut guard = lock_db(&state.db)?;
    guard
        .update(id, &input)
        .map_err(|e| match e {
            // Defense-in-depth: validate() already rejects blank titles; the
            // DB layer enforces the same policy for any future caller.
            UpdateError::BlankTitle => AppError::BadRequest("title must not be empty".to_string()),
            UpdateError::Db(e) => AppError::from(e),
        })?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn delete_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    if lock_db(&state.db)?.delete(id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

async fn get_stats(State(state): State<AppState>) -> Result<Json<Stats>, AppError> {
    Ok(Json(lock_db(&state.db)?.stats()?))
}

// ---------- agents ----------

async fn list_agents(State(state): State<AppState>) -> Result<Json<Vec<Agent>>, AppError> {
    Ok(Json(lock_db(&state.db)?.list_agents()?))
}

async fn create_agent(
    State(state): State<AppState>,
    Json(input): Json<CreateAgent>,
) -> Result<(StatusCode, Json<Agent>), AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let agent = lock_db(&state.db)?.create_agent(&input)?;
    Ok((StatusCode::CREATED, Json(agent)))
}

async fn get_agent(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Agent>, AppError> {
    lock_db(&state.db)?.get_agent(id)?.map(Json).ok_or(AppError::NotFound)
}

async fn update_agent(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<UpdateAgent>,
) -> Result<Json<Agent>, AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    lock_db(&state.db)?
        .update_agent(id, &input)?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn delete_agent(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    if lock_db(&state.db)?.delete_agent(id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

// ---------- assignment + runs ----------

#[derive(Deserialize)]
struct AssignAgent {
    agent_id: i64,
}

#[derive(Default, Deserialize)]
struct RunBody {
    #[serde(default)]
    agent_id: Option<i64>,
}

/// Assign an agent to a task without running it yet.
async fn assign_agent(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<AssignAgent>,
) -> Result<Json<Task>, AppError> {
    let guard = lock_db(&state.db)?;
    if guard.get_agent(input.agent_id)?.is_none() {
        return Err(AppError::BadRequest(format!(
            "agent {} does not exist",
            input.agent_id
        )));
    }
    if !guard.assign_agent(id, input.agent_id)? {
        return Err(AppError::NotFound);
    }
    guard.get(id)?.map(Json).ok_or(AppError::NotFound)
}

/// Remove the agent assignment from a task.
async fn unassign_agent(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Task>, AppError> {
    let guard = lock_db(&state.db)?;
    if !guard.unassign_agent(id)? {
        return Err(AppError::NotFound);
    }
    guard.get(id)?.map(Json).ok_or(AppError::NotFound)
}

#[derive(Deserialize)]
struct WorkdirBody {
    workdir: String,
}

/// Set the working directory the task's agent processes execute in.
/// An empty string clears it.
async fn set_workdir(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<WorkdirBody>,
) -> Result<Json<Task>, AppError> {
    check_workdir(Some(&input.workdir)).map_err(AppError::BadRequest)?;
    let guard = lock_db(&state.db)?;
    if !guard.set_workdir(id, Some(&input.workdir))? {
        return Err(AppError::NotFound);
    }
    guard.get(id)?.map(Json).ok_or(AppError::NotFound)
}

/// Clear the task's working directory (agents run in the server's).
async fn clear_workdir(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Task>, AppError> {
    let guard = lock_db(&state.db)?;
    if !guard.set_workdir(id, None)? {
        return Err(AppError::NotFound);
    }
    guard.get(id)?.map(Json).ok_or(AppError::NotFound)
}

/// Run the task's assigned agent (or the one given in the body) and return
/// the run immediately — `running` when the agent's execution slot was free,
/// `queued` when another run of the same agent is still live (it starts
/// automatically when the slot frees). The process is supervised in the
/// background; poll `GET /api/runs/{run_id}` for the outcome.
async fn run_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    body: Option<Json<RunBody>>,
) -> Result<(StatusCode, Json<Run>), AppError> {
    let agent_id = match body {
        Some(Json(RunBody { agent_id: Some(a) })) => a,
        _ => {
            // fall back to the task's assigned agent
            lock_db(&state.db)?
                .get(id)?
                .and_then(|t| t.agent_id)
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "no agent_id given and the task has no assigned agent".to_string(),
                    )
                })?
        }
    };

    let (task, agent) = {
        let guard = lock_db(&state.db)?;
        let task = guard.get(id)?.ok_or(AppError::NotFound)?;
        let agent = guard
            .get_agent(agent_id)?
            .ok_or_else(|| AppError::BadRequest(format!("agent {agent_id} does not exist")))?;
        (task, agent)
    };

    let run = runner::start(
        state.db.clone(),
        state.registry.clone(),
        state.slots.clone(),
        task,
        agent,
    )
    .await
    .map_err(AppError::BadRequest)?;
    Ok((StatusCode::CREATED, Json(run)))
}

// ---------- workdirs config ----------

/// The configured working directories (see [`crate::workdirs`]): the list
/// the dashboard's workdir dropdown and the `list_workdirs` MCP tool offer.
/// The config file is re-read on every call, so editing it applies without
/// a restart. A malformed config is a 500 — but unlike other internal
/// errors it carries the real reason (file + JSON error) in the body: it is
/// user-fixable configuration feedback, not sensitive internals, and
/// silently serving an empty list would hide the user's configuration.
async fn list_workdirs(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, axum::response::Response> {
    match crate::workdirs::load(&state.workdirs) {
        Ok(dirs) => Ok(Json(dirs)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e })),
        )
            .into_response()),
    }
}

#[derive(Deserialize)]
struct WorkdirEntryBody {
    workdir: String,
}

/// Add a directory to the workdirs config (see [`crate::workdirs`]).
/// The path must be absolute; adding an existing entry is a no-op. The
/// config file is created when it does not exist yet. Returns the updated
/// list — the same shape `GET /api/workdirs` serves.
async fn add_workdir(
    State(state): State<AppState>,
    Json(input): Json<WorkdirEntryBody>,
) -> Result<(StatusCode, Json<Vec<String>>), axum::response::Response> {
    match crate::workdirs::add(&state.workdirs, &input.workdir) {
        Ok(dirs) => Ok((StatusCode::CREATED, Json(dirs))),
        Err(crate::workdirs::ConfigError::Invalid(msg)) => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": msg })),
        )
            .into_response()),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response()),
    }
}

/// Remove a directory from the workdirs config. Removing an entry that is
/// not in the list is a no-op (idempotent). Returns the updated list.
async fn remove_workdir(
    State(state): State<AppState>,
    Query(input): Query<WorkdirEntryBody>,
) -> Result<Json<Vec<String>>, axum::response::Response> {
    match crate::workdirs::remove(&state.workdirs, &input.workdir) {
        Ok(dirs) => Ok(Json(dirs)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response()),
    }
}

// ---------- runs ----------

#[derive(Deserialize)]
struct RunsQuery {
    task_id: Option<i64>,
    status: Option<String>,
    limit: Option<u32>,
}

async fn list_runs(
    State(state): State<AppState>,
    Query(q): Query<RunsQuery>,
) -> Result<Json<Vec<Run>>, AppError> {
    check_enum("status", q.status.as_deref(), &RUN_STATUSES).map_err(AppError::BadRequest)?;
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let runs = lock_db(&state.db)?.list_runs(q.task_id, q.status.as_deref(), limit)?;
    Ok(Json(runs))
}

async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Run>, AppError> {
    lock_db(&state.db)?.get_run(id)?.map(Json).ok_or(AppError::NotFound)
}

/// Cancel a run. 404 when the run row does not exist; 409 when the run is
/// already finished. A `queued` run has no process — it is dropped from the
/// queue directly; a `running` run's process is killed via the registry.
async fn cancel_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    let run = lock_db(&state.db)?.get_run(id)?.ok_or(AppError::NotFound)?;
    match run.status.as_str() {
        "queued" => {
            // No process exists; just drop it from the queue.
            if !lock_db(&state.db)?.cancel_run(id)? {
                return Err(AppError::Conflict("run is not queued".to_string()));
            }
            Ok(StatusCode::NO_CONTENT)
        }
        "running" => {
            if !state.registry.cancel(id) {
                // The row says running but no live supervisor exists (the
                // server restarted, or the supervisor task was lost). There
                // is no process to kill, so record the outcome directly.
                lock_db(&state.db)?.cancel_run(id)?;
                return Err(AppError::Conflict(
                    "run is not running (no live process)".to_string(),
                ));
            }
            Ok(StatusCode::NO_CONTENT)
        }
        _ => Err(AppError::Conflict("run is not queued or running".to_string())),
    }
}

/// Delete a run record. A live run is stopped first, then its row is
/// removed — the run disappears from history entirely. 404 when the run
/// row does not exist.
async fn delete_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    // One lock across the status check and the delete: the queue's pickup
    // needs the same lock, so a queued run cannot be started in between —
    // no process is ever spawned for a row that is about to disappear.
    // (Calling registry.cancel under the DB lock is safe: it only touches
    // the registry's own mutex and never the database.)
    let deleted = {
        let guard = lock_db(&state.db)?;
        let run = guard.get_run(id)?.ok_or(AppError::NotFound)?;
        if run.status == "running" {
            // Ask the live process to stop. `false` means no live
            // supervisor exists (e.g. the server restarted mid-run) —
            // nothing to kill. Either way the supervisor tolerates its
            // final write landing on a row that is already gone, and
            // still releases the agent's slot.
            state.registry.cancel(id);
        }
        guard.delete_run(id)?
    };
    if !deleted {
        return Err(AppError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

// ---------- live run streaming (SSE) ----------

/// The SSE response of [`stream_run`]: boxed so the live path (replay +
/// broadcast stream) and the finished path (a plain iterator) share one
/// concrete type; `KeepAliveStream` because both use a keep-alive.
type RunSse = Sse<
    axum::response::sse::KeepAliveStream<
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Infallible>> + Send>>,
    >,
>;

/// Box a stream into the trait object [`RunSse`] expects. The explicit
/// return type is the coercion site — `Box::pin(x)` alone keeps the
/// concrete type.
fn boxed_stream(
    stream: impl tokio_stream::Stream<Item = Result<Event, Infallible>> + Send + 'static,
) -> std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Infallible>> + Send>> {
    Box::pin(stream)
}/// Stream the conversation of a run as Server-Sent Events:
///
/// - `event: prompt`  — the prompt the agent received on stdin
/// - `event: output`  — a chunk of the agent's stdout/stderr, in arrival order
/// - `event: status`  — a lifecycle note (`queued`, `running`, `completed`, …)
/// - `event: resync`  — the client lagged behind the broadcast channel;
///   re-fetch the run to see the full output
/// - `event: end`     — the run reached a final status; the stream closes
///
/// A live run (queued or running) is followed until it ends. A finished
/// run replays its stored output as one `output` event and closes — the
/// endpoint is a uniform way to watch any run. Unknown runs are 404.
///
/// The replayed events carry an `id:` field (the stream's sequence number)
/// so a client that reconnects with `Last-Event-ID` can skip what it has
/// already seen; live events carry no id (they are not replayable).
async fn stream_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<RunSse, AppError> {
    // The registry lookup and the DB read happen in this order on purpose:
    // a run that ends between the two checks is served from the registry
    // (its final events are still in the replay buffer), and a run that is
    // not in the registry is served from the DB — the two paths cannot
    // both miss a run that exists.
    let Some((replay, live)) = state.registry.subscribe(id) else {
        // Not live: either finished (serve the stored output) or unknown.
        let run = lock_db(&state.db)?.get_run(id)?.ok_or(AppError::NotFound)?;
        let mut events = vec![Ok(sse_event(None, &RunEvent::Status(run.status.clone())))];
        if !run.output.is_empty() {
            events.push(Ok(sse_event(None, &RunEvent::Output(run.output.clone()))));
        }
        events.push(Ok(sse_event(None, &RunEvent::End)));
        return Ok(Sse::new(boxed_stream(tokio_stream::iter(events)))
            .keep_alive(KeepAlive::default()));
    };

    // Replay first (with sequence ids), then everything the run still does.
    // The broadcast receiver is created before the replay is read, so no
    // event can be missed in between; the supervisor's `end` event closes
    // the stream (it drops the last sender when it finishes).
    let replay_iter = replay
        .into_iter()
        .map(|(seq, event)| Ok(sse_event(Some(seq), &event)));
    let live_stream = BroadcastStream::new(live).filter_map(|event| match event {
        Ok(event) => Some(Ok(sse_event(None, &event))),
        // The client fell behind the broadcast channel: tell it to re-fetch
        // instead of silently showing a gap.
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_)) => {
            Some(Ok(Event::default().event("resync")))
        }
    });
    Ok(Sse::new(boxed_stream(
        tokio_stream::iter(replay_iter).chain(live_stream),
    ))
    .keep_alive(KeepAlive::default()))
}

/// One SSE event from a run event. `seq` is attached as the SSE id when the
/// event comes from the replay buffer (so a reconnecting client can skip
/// it); live events have no id.
fn sse_event(seq: Option<u64>, event: &RunEvent) -> Event {
    let mut builder = Event::default();
    if let Some(seq) = seq {
        builder = builder.id(seq.to_string());
    }
    match event {
        RunEvent::Prompt(text) => builder.event("prompt").data(text.clone()),
        RunEvent::Output(text) => builder.event("output").data(text.clone()),
        RunEvent::Status(text) => builder.event("status").data(text.clone()),
        RunEvent::End => builder.event("end"),
    }
}

// ---------- errors ----------

/// Lock the shared database, mapping mutex poisoning (a panic in another
/// handler while holding the lock) to a 500 instead of panicking here too.
fn lock_db(db: &SharedDb) -> Result<MutexGuard<'_, Db>, AppError> {
    db.lock()
        .map_err(|e| AppError::internal("db mutex poisoned", e))
}

enum AppError {
    NotFound,
    BadRequest(String),
    /// The request conflicts with the current state (e.g. cancelling a run
    /// that is no longer running).
    Conflict(String),
    /// Client-facing message; the real error was logged server-side.
    Internal(String),
}

/// Correlation counter for internal errors: the client sees the number, the
/// server log has the details, so reports can be matched without leaking
/// internals (DB paths, SQL error text) to the client.
static ERROR_SEQ: AtomicU64 = AtomicU64::new(0);

impl AppError {
    fn internal(context: &str, err: impl std::fmt::Display) -> Self {
        let id = ERROR_SEQ.fetch_add(1, Ordering::Relaxed);
        eprintln!("[error #{id}] {context}: {err}");
        AppError::Internal(format!("internal server error (reference #{id})"))
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(e: rusqlite::Error) -> Self {
        AppError::internal("database", e)
    }
}

impl From<UpdateError> for AppError {
    fn from(e: UpdateError) -> Self {
        match e {
            UpdateError::BlankTitle => AppError::BadRequest("title must not be empty".to_string()),
            UpdateError::Db(e) => AppError::from(e),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "task not found".to_string()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::http::header::CONTENT_TYPE;
    use tower::ServiceExt; // for `oneshot`

    use super::*;
    use crate::db::Db;

    fn app() -> Router {
        app_with_token(None)
    }

    fn app_with_token(token: Option<String>) -> Router {
        app_with(token, workdirs_test_path())
    }

    fn app_with(token: Option<String>, workdirs: std::path::PathBuf) -> Router {
        let db: SharedDb = Arc::new(Mutex::new(Db::open(":memory:").unwrap()));
        let state = AppState {
            db,
            registry: Arc::new(RunRegistry::default()),
            slots: Arc::new(AgentSlots::default()),
            workdirs,
        };
        router(state, token)
    }

    /// A unique workdirs config path per `app()` call, so tests never share
    /// state. The file is not created — a missing config is an empty list.
    fn workdirs_test_path() -> std::path::PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "orga-rs-workdirs-webtest-{}-{}.json",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    async fn call(app: &Router, method: &str, uri: &str, body: Option<&str>) -> Response {
        call_with(app, method, uri, body, None).await
    }

    async fn call_with(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<&str>,
        token: Option<&str>,
    ) -> Response {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header("Authorization", format!("Bearer {token}"));
        }
        if body.is_some() {
            builder = builder.header(CONTENT_TYPE, "application/json");
        }
        let req = match body {
            Some(json) => builder.body(Body::from(json.to_string())).unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        app.clone().oneshot(req).await.unwrap()
    }

    async fn body_json(res: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn dashboard_page_is_served() {
        let res = call(&app(), "GET", "/", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(html.contains("orga"));
        assert!(html.contains("/api/tasks"));
        // The conversation view ships with its raw/markdown toggle and the
        // renderer (smoke test: the embedded page contains the feature).
        assert!(html.contains("id=\"stream-md\""));
        assert!(html.contains("renderMarkdown"));
        assert!(html.contains("Render as Markdown"));
        // A finished run's captured output is collapsible (native
        // <details>, collapsed by default, state kept across re-renders).
        assert!(html.contains("details class=\"run-output\""));
        assert!(html.contains("expandedRuns"));
        // The projects section manages the workdirs config (add/remove).
        assert!(html.contains("id=\"projects-section\""));
        assert!(html.contains("renderProjects"));
        assert!(html.contains("addProject"));
        assert!(html.contains("removeProject"));
    }

    /// The dashboard's script block must stay syntactically balanced. A
    /// full JS parser is out of scope for a Rust test suite; this walks
    /// the script with a small lexer that skips the constructs where
    /// brackets do not count — comments, strings, regex literals and
    /// template literals (including `${…}` interpolations, which nest in
    /// this script) — and checks that the remaining brackets balance.
    /// This catches the classic embedding mistakes: a stray brace in a
    /// template literal, a truncated regex, a quote eaten by a comment.
    #[test]
    fn dashboard_script_brackets_are_balanced() {
        let html = include_str!("dashboard.html");
        let script = html
            .split("<script>")
            .nth(1)
            .and_then(|rest| rest.split("</script>").next())
            .expect("script block");

        let mut depth = 0i32;
        // Brace depth at which each open `${` interpolation closes; the
        // script nests templates inside interpolations (renderRuns etc.).
        let mut templates: Vec<i32> = Vec::new();
        let mut in_template = false;
        let mut chars = script.chars().peekable();

        while let Some(c) = chars.next() {
            if in_template {
                match c {
                    '\\' => { chars.next(); }
                    '`' => in_template = false,
                    '$' if chars.peek() == Some(&'{') => {
                        chars.next();
                        templates.push(depth);
                        depth += 1;      // the interpolation's opening brace
                        in_template = false; // its body is code again
                    }
                    _ => {}
                }
                continue;
            }
            match c {
                '/' => match chars.peek() {
                    // line comment
                    Some('/') => {
                        for c2 in chars.by_ref() {
                            if c2 == '\n' { break; }
                        }
                    }
                    // block comment
                    Some('*') => {
                        chars.next();
                        let mut prev = '\0';
                        for c2 in chars.by_ref() {
                            if prev == '*' && c2 == '/' { break; }
                            prev = c2;
                        }
                    }
                    // regex literal (the script contains no division, so
                    // every other `/` starts one); `[…]` classes may hold
                    // a `/` without closing it
                    _ => {
                        let mut in_class = false;
                        while let Some(c2) = chars.next() {
                            match c2 {
                                '\\' => { chars.next(); }
                                '[' => in_class = true,
                                ']' => in_class = false,
                                '/' if !in_class => break,
                                _ => {}
                            }
                        }
                        // trailing flags (g, i, …)
                        while matches!(chars.peek(), Some(n) if n.is_ascii_alphabetic()) {
                            chars.next();
                        }
                    }
                },
                // string literal
                '\'' | '"' => {
                    let quote = c;
                    while let Some(c2) = chars.next() {
                        if c2 == '\\' { chars.next(); }
                        else if c2 == quote { break; }
                    }
                }
                '`' => in_template = true,
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => {
                    depth -= 1;
                    // A `}` that brings the depth back to the top `${`'s
                    // entry value closes the interpolation, not code.
                    if c == '}' && templates.last() == Some(&depth) {
                        templates.pop();
                        in_template = true;
                    }
                }
                _ => {}
            }
        }
        assert_eq!(depth, 0, "unbalanced brackets in the dashboard script");
        assert!(templates.is_empty(), "unclosed template interpolation");
    }

    #[tokio::test]
    async fn full_rest_crud_cycle() {
        let app = app();

        // create
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"Write integration tests","priority":"high"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let task = body_json(res).await;
        let id = task["id"].as_i64().unwrap();
        assert_eq!(task["priority"], "high");
        assert_eq!(task["status"], "todo");

        // list + filter
        let res = call(&app, "GET", "/api/tasks?priority=high", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        let tasks = body_json(res).await;
        assert_eq!(tasks.as_array().unwrap().len(), 1);

        // get
        let res = call(&app, "GET", &format!("/api/tasks/{id}"), None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["title"], "Write integration tests");

        // update
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"status":"done"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["status"], "done");

        // stats
        let res = call(&app, "GET", "/api/stats", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        let stats = body_json(res).await;
        assert_eq!(stats["total"], 1);
        assert_eq!(stats["done"], 1);

        // delete
        let res = call(&app, "DELETE", &format!("/api/tasks/{id}"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/tasks/{id}"), None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_input_is_rejected_with_400() {
        let app = app();

        // invalid enum value
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"x","status":"bogus"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(body_json(res).await["error"]
            .as_str()
            .unwrap()
            .contains("invalid status"));

        // blank title
        let res = call(&app, "POST", "/api/tasks", Some(r#"{"title":"  "}"#)).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // malformed JSON body
        let res = call(&app, "POST", "/api/tasks", Some("{not json")).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // unknown id
        let res = call(
            &app,
            "PUT",
            "/api/tasks/999",
            Some(r#"{"status":"done"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_filters_and_fields_are_rejected_with_400() {
        let app = app();

        // unknown filter values are rejected instead of silently matching nothing
        let res = call(&app, "GET", "/api/tasks?status=bogus", None).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(body_json(res).await["error"]
            .as_str()
            .unwrap()
            .contains("invalid status"));

        let res = call(&app, "GET", "/api/tasks?priority=nope", None).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // valid filters still work
        let res = call(&app, "GET", "/api/tasks?status=todo&priority=high", None).await;
        assert_eq!(res.status(), StatusCode::OK);

        // due_date must be a canonical YYYY-MM-DD calendar date
        for bad in ["tomorrow", "2026-1-5", "2026-02-30", "11/10/2026"] {
            let res = call(
                &app,
                "POST",
                "/api/tasks",
                Some(&format!(r#"{{"title":"x","due_date":"{bad}"}}"#)),
            )
            .await;
            assert_eq!(res.status(), StatusCode::BAD_REQUEST, "due_date '{bad}'");
        }

        // a valid date is accepted
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"x","due_date":"2026-11-10"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);

        // over-long titles are rejected (server-side counterpart of maxlength)
        let long = "x".repeat(201);
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(&format!(r#"{{"title":"{long}"}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn update_validates_fields_and_clears_due_date() {
        let app = app();
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"t","due_date":"2999-01-01"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let id = body_json(res).await["id"].as_i64().unwrap();

        // invalid enum on PUT is rejected
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"status":"bogus"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // invalid due_date on PUT is rejected
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"due_date":"soon"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // blank title on PUT is rejected
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"title":"  "}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // an empty string clears the due date over HTTP
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"due_date":""}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_json(res).await["due_date"].is_null());
    }

    #[tokio::test]
    async fn token_guards_the_api_but_not_the_dashboard() {
        let app = app_with_token(Some("s3cret".to_string()));

        // the dashboard page contains no data and stays open
        let res = call(&app, "GET", "/", None).await;
        assert_eq!(res.status(), StatusCode::OK);

        // API without a token -> 401
        let res = call(&app, "GET", "/api/tasks", None).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // wrong token -> 401
        let res = call_with(&app, "GET", "/api/tasks", None, Some("wrong")).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // correct token -> full access
        let res = call_with(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"with token"}"#),
            Some("s3cret"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let res = call_with(&app, "GET", "/api/tasks", None, Some("s3cret")).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await.as_array().unwrap().len(), 1);
    }

    // ---------- agents + runs ----------

    /// A task and an agent, created through the API, with their ids.
    async fn seed_task_agent(app: &Router) -> (i64, i64) {
        let res = call(app, "POST", "/api/tasks", Some(r#"{"title":"to work on"}"#)).await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let task_id = body_json(res).await["id"].as_i64().unwrap();

        let res = call(
            app,
            "POST",
            "/api/agents",
            Some(r#"{"name":"echoer","command":"cat","prompt_template":"work on {title}"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let agent_id = body_json(res).await["id"].as_i64().unwrap();
        (task_id, agent_id)
    }

    #[tokio::test]
    async fn agent_crud_and_validation() {
        let app = app();

        // invalid agents are rejected
        let res = call(&app, "POST", "/api/agents", Some(r#"{"name":"","command":"x"}"#)).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let res = call(&app, "POST", "/api/agents", Some(r#"{"name":"x","command":"  "}"#)).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let res = call(
            &app,
            "POST",
            "/api/agents",
            Some(r#"{"name":"x","command":"y","timeout_secs":99999999}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // create
        let res = call(
            &app,
            "POST",
            "/api/agents",
            Some(r#"{"name":"coder","command":"echo done","timeout_secs":30}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let agent = body_json(res).await;
        let id = agent["id"].as_i64().unwrap();
        assert_eq!(agent["timeout_secs"], 30);

        // list + get
        let res = call(&app, "GET", "/api/agents", None).await;
        assert_eq!(body_json(res).await.as_array().unwrap().len(), 1);
        let res = call(&app, "GET", &format!("/api/agents/{id}"), None).await;
        assert_eq!(body_json(res).await["name"], "coder");

        // update: partial, omitted fields keep their value
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{id}"),
            Some(r#"{"name":"coder v2"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let agent = body_json(res).await;
        assert_eq!(agent["name"], "coder v2");
        assert_eq!(agent["command"], "echo done");

        // timeout 0 means "no timeout" and round-trips as 0, not null
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{id}"),
            Some(r#"{"timeout_secs":0}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["timeout_secs"], 0);

        // unknown id -> 404
        let res = call(&app, "PUT", "/api/agents/999", Some(r#"{"name":"x"}"#)).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // delete
        let res = call(&app, "DELETE", &format!("/api/agents/{id}"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/agents/{id}"), None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn assign_unassign_and_run_task() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // assign without running
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{task_id}/agent"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["agent_id"], agent_id);

        // assigning a nonexistent agent -> 400
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{task_id}/agent"),
            Some(r#"{"agent_id":999}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // run with the assigned agent (empty body)
        let res = call(&app, "POST", &format!("/api/tasks/{task_id}/run"), Some("{}")).await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let run = body_json(res).await;
        let run_id = run["id"].as_i64().unwrap();
        assert_eq!(run["status"], "running");
        assert_eq!(run["agent_name"], "echoer");

        // the run finishes quickly (cat echoes the prompt); poll for it
        let mut finished = None;
        for _ in 0..100 {
            let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
            let run = body_json(res).await;
            if run["status"] != "running" {
                finished = Some(run);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let run = finished.expect("run did not finish in time");
        assert_eq!(run["status"], "completed");
        assert_eq!(run["exit_code"], 0);
        assert!(run["output"].as_str().unwrap().contains("work on to work on"));

        // list runs, filtered and unfiltered
        let res = call(&app, "GET", &format!("/api/runs?task_id={task_id}"), None).await;
        assert_eq!(body_json(res).await.as_array().unwrap().len(), 1);
        let res = call(&app, "GET", "/api/runs?status=completed", None).await;
        assert_eq!(body_json(res).await.as_array().unwrap().len(), 1);
        let res = call(&app, "GET", "/api/runs?status=bogus", None).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // unassign
        let res = call(&app, "DELETE", &format!("/api/tasks/{task_id}/agent"), None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_json(res).await["agent_id"].is_null());
    }

    #[tokio::test]
    async fn run_without_agent_is_rejected_and_run_can_be_cancelled() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // no assigned agent and no agent_id in the body -> 400
        let res = call(&app, "POST", &format!("/api/tasks/{task_id}/run"), Some("{}")).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        // run a long-lived command, then cancel it
        let res = call(
            &app,
            "POST",
            "/api/agents",
            Some(r#"{"name":"sleeper","command":"sleep 60","timeout_secs":60}"#),
        )
        .await;
        let sleeper_id = body_json(res).await["id"].as_i64().unwrap();
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{sleeper_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let run_id = body_json(res).await["id"].as_i64().unwrap();

        // cancel the live run
        let res = call(&app, "POST", &format!("/api/runs/{run_id}/cancel"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // wait for the supervisor to record the cancellation
        let mut status = String::new();
        for _ in 0..100 {
            let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
            status = body_json(res).await["status"].as_str().unwrap().to_string();
            if status != "running" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(status, "cancelled");

        // cancelling again -> 409 (no longer running)
        let res = call(&app, "POST", &format!("/api/runs/{run_id}/cancel"), None).await;
        assert_eq!(res.status(), StatusCode::CONFLICT);

        // unknown run -> 404
        let res = call(&app, "POST", "/api/runs/999/cancel", None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // the agent_id given in the body was also assigned to the task
        let res = call(&app, "GET", &format!("/api/tasks/{task_id}"), None).await;
        assert_eq!(body_json(res).await["agent_id"], sleeper_id);
        let _ = agent_id;
    }

    #[tokio::test]
    async fn workdir_can_be_set_validated_and_cleared() {
        let app = app();
        let res = call(&app, "POST", "/api/tasks", Some(r#"{"title":"t"}"#)).await;
        let id = body_json(res).await["id"].as_i64().unwrap();

        // relative paths are rejected
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}/workdir"),
            Some(r#"{"workdir":"relative/path"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(body_json(res).await["error"].as_str().unwrap().contains("absolute"));

        // a valid absolute path is accepted
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}/workdir"),
            Some(r#"{"workdir":"/tmp"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["workdir"], "/tmp");

        // it is also accepted on create and update
        let res = call(
            &app,
            "POST",
            "/api/tasks",
            Some(r#"{"title":"t2","workdir":"/tmp"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        assert_eq!(body_json(res).await["workdir"], "/tmp");
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{id}"),
            Some(r#"{"workdir":"/var"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await["workdir"], "/var");

        // DELETE clears it
        let res = call(&app, "DELETE", &format!("/api/tasks/{id}/workdir"), None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(body_json(res).await["workdir"].is_null());

        // unknown task -> 404
        let res = call(&app, "PUT", "/api/tasks/999/workdir", Some(r#"{"workdir":"/tmp"}"#)).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn workdirs_endpoint_serves_the_config_file() {
        // The app is built with a known config path so the test can write
        // to the exact file the endpoint reads.
        let path = workdirs_test_path();
        let app = app_with(None, path.clone());

        // No config file -> an empty list (the feature is optional).
        let res = call(&app, "GET", "/api/workdirs", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await, serde_json::json!([]));

        // A config file is picked up without a restart (re-read per call).
        std::fs::write(&path, r#"["/tmp/alpha", "relative", "/tmp/alpha"]"#).unwrap();
        let res = call(&app, "GET", "/api/workdirs", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await, serde_json::json!(["/tmp/alpha"]));
        let _ = std::fs::remove_file(&path);

        // A malformed config is a 500 with the reason in the body (the
        // message names the file and the JSON error, so it is fixable).
        std::fs::write(&path, "{not json").unwrap();
        let res = call(&app, "GET", "/api/workdirs", None).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_json(res).await;
        assert!(body["error"].as_str().unwrap().contains("not valid JSON"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn workdirs_can_be_added_and_removed_via_the_api() {
        let path = workdirs_test_path();
        let app = app_with(None, path.clone());

        // POST adds an entry (and creates the config file on the way).
        let res = call(
            &app,
            "POST",
            "/api/workdirs",
            Some(r#"{"workdir":"/tmp/project-a"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        assert_eq!(body_json(res).await, serde_json::json!(["/tmp/project-a"]));

        // A duplicate add is a no-op that returns the unchanged list.
        let res = call(
            &app,
            "POST",
            "/api/workdirs",
            Some(r#"{"workdir":"/tmp/project-a"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        assert_eq!(body_json(res).await, serde_json::json!(["/tmp/project-a"]));

        // A relative path is rejected with 400 and nothing is written.
        let res = call(
            &app,
            "POST",
            "/api/workdirs",
            Some(r#"{"workdir":"relative"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(body_json(res).await["error"].as_str().unwrap().contains("absolute"));

        // The list endpoint serves the same state (the file is the truth).
        let res = call(&app, "GET", "/api/workdirs", None).await;
        assert_eq!(body_json(res).await, serde_json::json!(["/tmp/project-a"]));

        // DELETE removes an entry; the response is the updated list.
        let res = call(&app, "DELETE", "/api/workdirs?workdir=/tmp/project-a", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await, serde_json::json!([]));

        // Removing an entry that is not in the list is a no-op.
        let res = call(&app, "DELETE", "/api/workdirs?workdir=/tmp/gone", None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_json(res).await, serde_json::json!([]));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn workdir_mutations_survive_a_malformed_config_check() {
        let path = workdirs_test_path();
        let app = app_with(None, path.clone());

        // A malformed config blocks mutations (500 with the reason) and is
        // left untouched — the user must fix the file by hand.
        std::fs::write(&path, "{not json").unwrap();
        let res = call(
            &app,
            "POST",
            "/api/workdirs",
            Some(r#"{"workdir":"/tmp/x"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body_json(res).await["error"].as_str().unwrap().contains("not valid JSON"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{not json");

        let res = call(&app, "DELETE", "/api/workdirs?workdir=/tmp/x", None).await;
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body_json(res).await["error"].as_str().unwrap().contains("not valid JSON"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn second_run_of_an_agent_is_queued_and_runs_afterwards() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // A long sleep holds the slot; cancelling it frees the slot at a
        // known moment, which makes the pickup deterministic.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"sleep 30","timeout_secs":60}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // First run takes the slot.
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let first = body_json(res).await;
        assert_eq!(first["status"], "running");

        // Second run of the same agent is queued.
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let second = body_json(res).await;
        assert_eq!(second["status"], "queued");

        // Make the queued run fast: a queued run re-reads the agent config
        // when it is picked up, so it runs the *current* command.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"true"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // Cancelling the first run frees the slot; the queued run starts
        // and completes.
        let res = call(&app, "POST", &format!("/api/runs/{}/cancel", first["id"]), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let mut second_status = String::new();
        for _ in 0..200 {
            let res = call(&app, "GET", &format!("/api/runs/{}", second["id"]), None).await;
            second_status = body_json(res).await["status"].as_str().unwrap().to_string();
            if second_status == "completed" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(second_status, "completed");

        // The first run was cancelled.
        let res = call(&app, "GET", &format!("/api/runs/{}", first["id"]), None).await;
        assert_eq!(body_json(res).await["status"], "cancelled");

        // A queued run can be cancelled while waiting. The slot is free now,
        // so make the agent slow again and take the slot with a third run.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"sleep 30","timeout_secs":60}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let third = body_json(res).await;
        assert_eq!(third["status"], "running");

        // The fourth run queues behind it and can be cancelled while waiting.
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let fourth = body_json(res).await;
        assert_eq!(fourth["status"], "queued");
        let res = call(&app, "POST", &format!("/api/runs/{}/cancel", fourth["id"]), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/runs/{}", fourth["id"]), None).await;
        assert_eq!(body_json(res).await["status"], "cancelled");
    }

    #[tokio::test]
    async fn runs_can_be_deleted() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // A completed run can be deleted outright.
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let run_id = body_json(res).await["id"].as_i64().unwrap();
        for _ in 0..100 {
            let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
            if body_json(res).await["status"] != "running" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let res = call(&app, "DELETE", &format!("/api/runs/{run_id}"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // Deleting again -> 404 (the row is gone).
        let res = call(&app, "DELETE", &format!("/api/runs/{run_id}"), None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // A live run is stopped first, then its row is removed.
        let res = call(
            &app,
            "POST",
            "/api/agents",
            Some(r#"{"name":"sleeper","command":"sleep 60","timeout_secs":60}"#),
        )
        .await;
        let sleeper_id = body_json(res).await["id"].as_i64().unwrap();
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{sleeper_id}}}"#)),
        )
        .await;
        let live_id = body_json(res).await["id"].as_i64().unwrap();

        let res = call(&app, "DELETE", &format!("/api/runs/{live_id}"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/runs/{live_id}"), None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        // The slot freed by the deletion lets the agent run again. The
        // supervisor releases the slot asynchronously after the kill, so
        // the fresh run may briefly queue — it must start once the slot
        // frees (the queue picks it up automatically).
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{sleeper_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let fresh_id = body_json(res).await["id"].as_i64().unwrap();

        let mut status = String::new();
        for _ in 0..200 {
            let res = call(&app, "GET", &format!("/api/runs/{fresh_id}"), None).await;
            status = body_json(res).await["status"].as_str().unwrap().to_string();
            if status == "running" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(status, "running");

        // Clean up: stop the run so the test does not leave a process.
        let res = call(&app, "POST", &format!("/api/runs/{fresh_id}/cancel"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn run_executes_in_the_tasks_workdir() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // Point the task at /tmp and make the agent print its cwd.
        let res = call(
            &app,
            "PUT",
            &format!("/api/tasks/{task_id}/workdir"),
            Some(r#"{"workdir":"/tmp"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"pwd"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        assert_eq!(res.status(), StatusCode::CREATED);
        let run_id = body_json(res).await["id"].as_i64().unwrap();

        let mut output = String::new();
        for _ in 0..100 {
            let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
            let run = body_json(res).await;
            if run["status"] != "running" && run["status"] != "queued" {
                assert_eq!(run["status"], "completed");
                output = run["output"].as_str().unwrap().to_string();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(output.trim().ends_with("tmp"), "pwd output was: {output}");
    }

    // ---------- live run streaming (SSE) ----------

    /// Read an SSE response body to completion and return its frames:
    /// one `(event, data)` pair per frame.
    async fn sse_frames(res: Response) -> Vec<(String, String)> {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        let mut frames = Vec::new();
        for frame in text.split("\n\n") {
            if frame.trim().is_empty() {
                continue;
            }
            let mut event = "message".to_string();
            let mut data = Vec::new();
            for line in frame.split('\n') {
                if let Some(name) = line.strip_prefix("event:") {
                    event = name.trim().to_string();
                } else if let Some(payload) = line.strip_prefix("data:") {
                    data.push(payload.strip_prefix(' ').unwrap_or(payload));
                }
            }
            frames.push((event, data.join("\n")));
        }
        frames
    }

    #[tokio::test]
    async fn a_live_run_streams_its_conversation_over_sse() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // A slow command keeps the run alive while the stream is opened.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"echo part1; sleep 0.4; echo part2","timeout_secs":10}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let run_id = body_json(res).await["id"].as_i64().unwrap();

        // Attach while the run is live: the replay shows the prompt and
        // part1, then part2 arrives live, then the run ends.
        let res = call(&app, "GET", &format!("/api/runs/{run_id}/stream"), None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );
        let frames = sse_frames(res).await;

        let events: Vec<&str> = frames.iter().map(|(e, _)| e.as_str()).collect();
        assert_eq!(
            events,
            vec!["status", "prompt", "output", "output", "status", "end"]
        );
        assert!(frames[1].1.contains("work on to work on"), "prompt: {}", frames[1].1);
        assert!(frames[2].1.contains("part1"), "first chunk: {}", frames[2].1);
        assert!(frames[3].1.contains("part2"), "second chunk: {}", frames[3].1);
        assert_eq!(frames[4].1, "completed");
    }

    #[tokio::test]
    async fn a_finished_run_replays_its_stored_output_over_sse() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let run_id = body_json(res).await["id"].as_i64().unwrap();
        for _ in 0..100 {
            let res = call(&app, "GET", &format!("/api/runs/{run_id}"), None).await;
            if body_json(res).await["status"] != "running" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // The run is finished: the stream serves the stored output and
        // closes immediately.
        let res = call(&app, "GET", &format!("/api/runs/{run_id}/stream"), None).await;
        assert_eq!(res.status(), StatusCode::OK);
        let frames = sse_frames(res).await;
        let events: Vec<&str> = frames.iter().map(|(e, _)| e.as_str()).collect();
        assert_eq!(events, vec!["status", "output", "end"]);
        assert_eq!(frames[0].1, "completed");
        assert!(frames[1].1.contains("work on to work on"));
    }

    #[tokio::test]
    async fn streaming_an_unknown_run_is_404() {
        let app = app();
        let res = call(&app, "GET", "/api/runs/999/stream", None).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_queued_run_can_be_streamed_until_it_starts_and_ends() {
        let app = app();
        let (task_id, agent_id) = seed_task_agent(&app).await;

        // Hold the slot, then queue a second run behind it.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"sleep 30","timeout_secs":60}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let first = body_json(res).await["id"].as_i64().unwrap();
        let res = call(
            &app,
            "POST",
            &format!("/api/tasks/{task_id}/run"),
            Some(&format!(r#"{{"agent_id":{agent_id}}}"#)),
        )
        .await;
        let second = body_json(res).await["id"].as_i64().unwrap();

        // Make the queued run fast, then free the slot by cancelling the
        // first run — the queued run starts and completes.
        let res = call(
            &app,
            "PUT",
            &format!("/api/agents/{agent_id}"),
            Some(r#"{"command":"true"}"#),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // Open the stream while the run is still queued.
        let stream_res = call(&app, "GET", &format!("/api/runs/{second}/stream"), None).await;
        assert_eq!(stream_res.status(), StatusCode::OK);

        let res = call(&app, "POST", &format!("/api/runs/{first}/cancel"), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // The queued run's stream must reach the end after the pickup.
        let frames = sse_frames(stream_res).await;
        let events: Vec<&str> = frames.iter().map(|(e, _)| e.as_str()).collect();
        assert_eq!(
            events,
            vec!["status", "status", "prompt", "status", "end"]
        );
        assert_eq!(frames[0].1, "queued");
        assert_eq!(frames[1].1, "running");
        assert_eq!(frames[3].1, "completed");
    }
}

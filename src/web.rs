use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, MutexGuard};

use axum::{
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::db::{Db, SharedDb, UpdateError};
use crate::models::{
    check_enum, check_workdir, Agent, CreateAgent, CreateTask, Run, Stats, Task, UpdateAgent,
    UpdateTask, PRIORITIES, RUN_STATUSES, STATUSES,
};
use crate::runner::{self, AgentSlots, RunRegistry, SharedRegistry};

/// Router state: the database, the live-run registry and the per-agent
/// execution slots (one live run per agent).
#[derive(Clone)]
pub struct AppState {
    pub db: SharedDb,
    pub registry: SharedRegistry,
    pub slots: std::sync::Arc<AgentSlots>,
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
        .route("/api/runs/{id}", get(get_run).delete(cancel_run))
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
        let db: SharedDb = Arc::new(Mutex::new(Db::open(":memory:").unwrap()));
        let state = AppState {
            db,
            registry: Arc::new(RunRegistry::default()),
            slots: Arc::new(AgentSlots::default()),
        };
        router(state, token)
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
        let res = call(&app, "DELETE", &format!("/api/runs/{run_id}"), None).await;
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
        let res = call(&app, "DELETE", &format!("/api/runs/{run_id}"), None).await;
        assert_eq!(res.status(), StatusCode::CONFLICT);

        // unknown run -> 404
        let res = call(&app, "DELETE", "/api/runs/999", None).await;
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
        let res = call(&app, "DELETE", &format!("/api/runs/{}", first["id"]), None).await;
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
        let res = call(&app, "DELETE", &format!("/api/runs/{}", fourth["id"]), None).await;
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        let res = call(&app, "GET", &format!("/api/runs/{}", fourth["id"]), None).await;
        assert_eq!(body_json(res).await["status"], "cancelled");
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
}

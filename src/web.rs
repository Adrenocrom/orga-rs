use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::MutexGuard;

use axum::{
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::db::{Db, SharedDb, UpdateError};
use crate::models::{check_enum, CreateTask, Stats, Task, UpdateTask, PRIORITIES, STATUSES};

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
    axum::serve(listener, router(db, token))
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
pub fn router(db: SharedDb, token: Option<String>) -> Router {
    let api = Router::new()
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route(
            "/api/tasks/{id}",
            get(get_task).put(update_task).delete(delete_task),
        )
        .route("/api/stats", get(get_stats))
        .route_layer(middleware::from_fn_with_state(token, require_token))
        .with_state(db);
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
    State(db): State<SharedDb>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<Task>>, AppError> {
    // Reject unknown filter values instead of silently matching nothing —
    // a typo in a script (or an LLM tool call) would otherwise look like
    // "no tasks".
    check_enum("status", q.status.as_deref(), &STATUSES).map_err(AppError::BadRequest)?;
    check_enum("priority", q.priority.as_deref(), &PRIORITIES).map_err(AppError::BadRequest)?;
    let tasks = lock_db(&db)?.list(q.status.as_deref(), q.priority.as_deref())?;
    Ok(Json(tasks))
}

async fn create_task(
    State(db): State<SharedDb>,
    Json(input): Json<CreateTask>,
) -> Result<(StatusCode, Json<Task>), AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let task = lock_db(&db)?.create(&input)?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn get_task(State(db): State<SharedDb>, Path(id): Path<i64>) -> Result<Json<Task>, AppError> {
    lock_db(&db)?.get(id)?.map(Json).ok_or(AppError::NotFound)
}

async fn update_task(
    State(db): State<SharedDb>,
    Path(id): Path<i64>,
    Json(input): Json<UpdateTask>,
) -> Result<Json<Task>, AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let mut guard = lock_db(&db)?;
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
    State(db): State<SharedDb>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    if lock_db(&db)?.delete(id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

async fn get_stats(State(db): State<SharedDb>) -> Result<Json<Stats>, AppError> {
    Ok(Json(lock_db(&db)?.stats()?))
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

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "task not found".to_string()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
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
        router(db, token)
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
}

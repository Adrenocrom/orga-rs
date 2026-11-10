use std::net::SocketAddr;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::db::SharedDb;
use crate::models::{CreateTask, Stats, Task, UpdateTask};

pub async fn serve(db: SharedDb, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Dashboard listening on http://{addr}");
    println!("MCP clients: run `orga-rs mcp` with ORGA_URL=http://{addr}");
    axum::serve(listener, router(db)).await?;
    Ok(())
}

/// The full app: dashboard page + JSON API. Exposed for integration tests.
pub fn router(db: SharedDb) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route("/api/tasks/{id}", get(get_task).put(update_task).delete(delete_task))
        .route("/api/stats", get(get_stats))
        .with_state(db)
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
    let tasks = db.lock().unwrap().list(q.status.as_deref(), q.priority.as_deref())?;
    Ok(Json(tasks))
}

async fn create_task(
    State(db): State<SharedDb>,
    Json(input): Json<CreateTask>,
) -> Result<(StatusCode, Json<Task>), AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    let task = db.lock().unwrap().create(&input)?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn get_task(State(db): State<SharedDb>, Path(id): Path<i64>) -> Result<Json<Task>, AppError> {
    db.lock().unwrap().get(id)?.map(Json).ok_or(AppError::NotFound)
}

async fn update_task(
    State(db): State<SharedDb>,
    Path(id): Path<i64>,
    Json(input): Json<UpdateTask>,
) -> Result<Json<Task>, AppError> {
    input.validate().map_err(AppError::BadRequest)?;
    db.lock()
        .unwrap()
        .update(id, &input)?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn delete_task(
    State(db): State<SharedDb>,
    Path(id): Path<i64>,
) -> Result<StatusCode, AppError> {
    if db.lock().unwrap().delete(id)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

async fn get_stats(State(db): State<SharedDb>) -> Result<Json<Stats>, AppError> {
    Ok(Json(db.lock().unwrap().stats()?))
}

// ---------- errors ----------

enum AppError {
    NotFound,
    BadRequest(String),
    Internal(rusqlite::Error),
}

impl From<rusqlite::Error> for AppError {
    fn from(e: rusqlite::Error) -> Self {
        AppError::Internal(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "task not found".to_string()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
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
        let db: SharedDb = Arc::new(Mutex::new(Db::open(":memory:").unwrap()));
        router(db)
    }

    async fn call(app: &Router, method: &str, uri: &str, body: Option<&str>) -> Response {
        let mut req = Request::builder().method(method).uri(uri);
        if body.is_some() {
            req = req.header(CONTENT_TYPE, "application/json");
        }
        let req = match body {
            Some(json) => req.body(Body::from(json.to_string())).unwrap(),
            None => req.body(Body::empty()).unwrap(),
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
}

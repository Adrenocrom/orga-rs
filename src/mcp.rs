//! MCP server: exposes the task system as tools over the stdio transport.
//!
//! This is a thin bridge: it does **not** touch the database itself. Every
//! tool call is forwarded to the running web server's REST API (the same
//! server that serves the dashboard), so there is exactly one process that
//! owns the database and all clients — browser, scripts, LLM — see the same
//! data. The server's base URL comes from `ORGA_URL` (default
//! `http://127.0.0.1:3000`).
//!
//! Wire format: JSON-RPC 2.0, one message per line (Model Context Protocol,
//! revision 2025-06-18). Notifications (messages without an "id") are accepted
//! but never answered, as the spec requires. Tool-level failures (unknown id,
//! invalid enum value, server unreachable) are reported inside a successful
//! result with `isError: true` so the calling model can read and fix them;
//! JSON-RPC errors are reserved for protocol-level problems.

use std::io::{self, BufRead, Write};

use serde_json::{json, Value};

use crate::models::{check_enum, PRIORITIES, STATUSES};

const PROTOCOL_VERSION: &str = "2025-06-18";
const SERVER_NAME: &str = "orga-rs";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Text produced by a tool, or an error message aimed at the model.
type ToolResult = Result<String, String>;

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("ORGA_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());
    let base = base.trim_end_matches('/');
    let api = Api::new(base);
    eprintln!("{SERVER_NAME} MCP bridge ready (server: {base})");
    serve(io::stdin().lock(), io::stdout().lock(), &api)
}

/// The stdio loop, generic over reader/writer so tests can drive it in memory.
fn serve<R: BufRead, W: Write>(
    reader: R,
    mut writer: W,
    api: &Api,
) -> Result<(), Box<dyn std::error::Error>> {
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        // An unparseable line has no usable id, so it cannot be answered.
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        // Notifications carry no id and must not be answered.
        let Some(id) = msg.get("id").cloned() else {
            continue;
        };
        let method = msg.get("method").and_then(Value::as_str).unwrap_or_default();
        let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));

        let response = match method {
            "initialize" => respond(
                id,
                Ok(json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": { "tools": { "listChanged": false } },
                    "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
                })),
            ),
            "ping" => respond(id, Ok(json!({}))),
            "tools/list" => respond(id, Ok(json!({ "tools": tool_definitions() }))),
            "tools/call" => respond(id, call_tool(api, &params)),
            other => respond(
                id,
                Err(json!({
                    "code": -32601,
                    "message": format!("method not found: {other}"),
                })),
            ),
        };
        writeln!(writer, "{response}")?;
        writer.flush()?;
    }
    Ok(())
}

fn respond(id: Value, result: Result<Value, Value>) -> Value {
    match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
    }
}

fn call_tool(api: &Api, params: &Value) -> Result<Value, Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_params("missing tool name"))?;
    let args = match params.get("arguments") {
        None | Some(Value::Null) => json!({}), // absent or null -> no arguments
        Some(v) => v.clone(),
    };
    if !args.is_object() {
        return Err(invalid_params("arguments must be an object"));
    }

    let outcome = match name {
        "list_tasks" => tool_list_tasks(api, &args),
        "get_task" => tool_get_task(api, &args),
        "create_task" => tool_create_task(api, &args),
        "update_task" => tool_update_task(api, &args),
        "delete_task" => tool_delete_task(api, &args),
        "task_stats" => tool_task_stats(api),
        other => {
            return Err(json!({
                "code": -32602,
                "message": format!("unknown tool: {other}"),
            }))
        }
    };

    let (text, is_error) = match outcome {
        Ok(text) => (text, false),
        Err(msg) => (msg, true),
    };
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
    }))
}

// ---------- tool implementations (HTTP calls to the server) ----------

fn tool_list_tasks(api: &Api, args: &Value) -> ToolResult {
    let status = opt_string(args, "status")?;
    let priority = opt_string(args, "priority")?;
    let tasks = api.get_json(
        &format!(
            "/api/tasks{}",
            query(&[("status", status.as_deref()), ("priority", priority.as_deref())])
        ),
        "list tasks",
    )?;
    if tasks.as_array().is_none_or(|t| t.is_empty()) {
        return Ok("No tasks found.".to_string());
    }
    to_text(&tasks)
}

fn tool_get_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    let task = api.get_json(&format!("/api/tasks/{id}"), "fetch task")?;
    to_text(&task)
}

fn tool_create_task(api: &Api, args: &Value) -> ToolResult {
    let body = validate_task_body(args, false)?;
    let task = api.post_json("/api/tasks", &body, "create task")?;
    to_text(&task)
}

fn tool_update_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    let body = validate_task_body(args, true)?;
    let task = api.put_json(&format!("/api/tasks/{id}"), &body, "update task")?;
    to_text(&task)
}

fn tool_delete_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    api.delete(&format!("/api/tasks/{id}"), "delete task")?;
    Ok(format!("task {id} deleted"))
}

fn tool_task_stats(api: &Api) -> ToolResult {
    let stats = api.get_json("/api/stats", "fetch stats")?;
    to_text(&stats)
}

/// Validate the task fields of a create/update call before sending them to
/// the server, so the model gets a precise error instead of a generic 400.
/// `id` is stripped from the body (it travels in the URL path).
fn validate_task_body(args: &Value, is_update: bool) -> Result<Value, String> {
    let mut body = args.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.remove("id");
    }
    if !is_update {
        let title = args
            .get("title")
            .and_then(Value::as_str)
            .ok_or("missing required argument: title")?;
        if title.trim().is_empty() {
            return Err("title must not be empty".to_string());
        }
    } else if let Some(title) = args.get("title").and_then(Value::as_str) {
        if title.trim().is_empty() {
            return Err("title must not be empty".to_string());
        }
    }
    check_enum("status", opt_string(args, "status")?.as_deref(), &STATUSES)?;
    check_enum("priority", opt_string(args, "priority")?.as_deref(), &PRIORITIES)?;
    Ok(body)
}

// ---------- REST client ----------

/// Minimal HTTP client for the server's JSON API.
struct Api {
    base: String,
    agent: ureq::Agent,
}

impl Api {
    fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
            // Generous timeout: the model may think between calls, but a hung
            // server should not block the MCP client forever.
            agent: ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(10))
                .build(),
        }
    }

    fn request(&self, method: &str, path: &str, body: Option<&Value>, what: &str) -> Result<Value, String> {
        let url = format!("{}{}", self.base, path);
        let result = if let Some(json) = body {
            self.agent
                .request(method, &url)
                .set("Content-Type", "application/json")
                .send_json(json.clone())
        } else {
            self.agent.request(method, &url).call()
        };
        let response = match result {
            Ok(res) => HttpResponse {
                status: res.status(),
                body: res.into_string().unwrap_or_default(),
            },
            Err(ureq::Error::Status(code, res)) => HttpResponse {
                status: code,
                body: res.into_string().unwrap_or_default(),
            },
            Err(e) => {
                return Err(format!(
                    "cannot reach the task server at {url} (is `orga-rs serve` running?): {e}"
                ))
            }
        };
        if (200..300).contains(&response.status) {
            // 204 No Content (DELETE) has an empty body; treat it as null.
            if response.body.trim().is_empty() {
                Ok(Value::Null)
            } else {
                serde_json::from_str(&response.body)
                    .map_err(|e| format!("server returned invalid JSON: {e}"))
            }
        } else {
            Err(server_error(&response, what))
        }
    }

    fn get_json(&self, path: &str, what: &str) -> Result<Value, String> {
        self.request("GET", path, None, what)
    }

    fn post_json(&self, path: &str, body: &Value, what: &str) -> Result<Value, String> {
        self.request("POST", path, Some(body), what)
    }

    fn put_json(&self, path: &str, body: &Value, what: &str) -> Result<Value, String> {
        self.request("PUT", path, Some(body), what)
    }

    fn delete(&self, path: &str, what: &str) -> Result<Value, String> {
        self.request("DELETE", path, None, what)
    }
}

/// A finished HTTP response, already buffered.
struct HttpResponse {
    status: u16,
    body: String,
}

/// Extract the human-readable message from a `{"error": "..."}` body.
fn server_error(res: &HttpResponse, what: &str) -> String {
    let detail = serde_json::from_str::<Value>(&res.body)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| {
            if res.body.is_empty() {
                format!("HTTP {}", res.status)
            } else {
                res.body.clone()
            }
        });
    format!("cannot {what}: {detail}")
}

/// Build a query string from optional filters, e.g. `?status=todo`.
fn query(params: &[(&str, Option<&str>)]) -> String {
    let mut parts = Vec::new();
    for (key, value) in params {
        if let Some(v) = value {
            parts.push(format!("{key}={}", urlencode(v)));
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

/// Percent-encode everything outside unreserved characters.
fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// ---------- tool definitions (tools/list) ----------

fn tool_definitions() -> Value {
    let status_enum = json!(["todo", "in_progress", "done"]);
    let priority_enum = json!(["low", "medium", "high"]);
    let id_schema = json!({ "type": "integer", "description": "Task id." });
    json!([
        {
            "name": "list_tasks",
            "description": "List tasks, optionally filtered by status and priority. \
Ordered by priority (high first), then newest first.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": { "type": "string", "enum": status_enum },
                    "priority": { "type": "string", "enum": priority_enum }
                }
            }
        },
        {
            "name": "get_task",
            "description": "Fetch a single task by id.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": id_schema },
                "required": ["id"]
            }
        },
        {
            "name": "create_task",
            "description": "Create a new task. Only title is required; omitted fields \
default to status=todo, priority=medium, no due date.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Short task title." },
                    "description": { "type": "string", "description": "Optional details." },
                    "status": { "type": "string", "enum": status_enum },
                    "priority": { "type": "string", "enum": priority_enum },
                    "due_date": { "type": "string", "description": "Due date as YYYY-MM-DD." }
                },
                "required": ["title"]
            }
        },
        {
            "name": "update_task",
            "description": "Update a task by id. Only the fields to change need to be \
given; omitted fields keep their current value. Pass due_date as an empty string to clear it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": id_schema,
                    "title": { "type": "string" },
                    "description": { "type": "string" },
                    "status": { "type": "string", "enum": status_enum },
                    "priority": { "type": "string", "enum": priority_enum },
                    "due_date": { "type": "string", "description": "YYYY-MM-DD, or \"\" to clear." }
                },
                "required": ["id"]
            }
        },
        {
            "name": "delete_task",
            "description": "Delete a task by id. This cannot be undone.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": id_schema },
                "required": ["id"]
            }
        },
        {
            "name": "task_stats",
            "description": "Aggregate counters: total, todo, in_progress, done and overdue.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

// ---------- argument helpers ----------

fn to_text(value: &impl serde::Serialize) -> ToolResult {
    serde_json::to_string_pretty(value).map_err(|e| format!("serialization error: {e}"))
}

fn req_id(args: &Value) -> Result<i64, String> {
    match args.get("id") {
        Some(Value::Number(n)) => n
            .as_i64()
            .ok_or_else(|| "id must be an integer".to_string()),
        Some(_) => Err("id must be an integer".to_string()),
        None => Err("missing required argument: id".to_string()),
    }
}

fn opt_string(args: &Value, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("{key} must be a string")),
    }
}

fn invalid_params(message: &str) -> Value {
    json!({ "code": -32602, "message": message })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- pure helpers ----------

    #[test]
    fn query_string_encodes_and_skips_none() {
        assert_eq!(query(&[("status", None), ("priority", None)]), "");
        assert_eq!(query(&[("status", Some("todo"))]), "?status=todo");
        assert_eq!(
            query(&[("status", Some("in progress & more"))]),
            "?status=in%20progress%20%26%20more"
        );
    }

    #[test]
    fn server_error_extracts_error_field() {
        let res = HttpResponse {
            status: 400,
            body: r#"{"error":"invalid status 'bogus': expected one of todo, in_progress, done"}"#
                .to_string(),
        };
        let msg = server_error(&res, "create task");
        assert!(msg.starts_with("cannot create task: invalid status"));
    }

    #[test]
    fn server_error_falls_back_to_status_code() {
        let res = HttpResponse { status: 500, body: String::new() };
        assert_eq!(server_error(&res, "list tasks"), "cannot list tasks: HTTP 500");
    }

    #[test]
    fn validate_task_body_strips_id_and_checks_enums() {
        let body = validate_task_body(
            &json!({ "id": 5, "title": "ok", "status": "done", "priority": "high" }),
            false,
        )
        .unwrap();
        assert_eq!(body, json!({ "title": "ok", "status": "done", "priority": "high" }));

        assert!(validate_task_body(&json!({ "title": "x", "status": "bogus" }), false).is_err());
        assert!(validate_task_body(&json!({ "status": "done" }), false).is_err()); // no title
        assert!(validate_task_body(&json!({ "title": "  " }), false).is_err()); // blank title
        // updates may omit the title entirely
        assert!(validate_task_body(&json!({ "status": "done" }), true).is_ok());
    }

    // ---------- stdio loop against a real server ----------

    /// Spin up the real axum router on an ephemeral port and point the MCP
    /// bridge at it — a true end-to-end test of the HTTP path.
    fn spawn_server() -> (Api, u16) {
        let db: crate::db::SharedDb = std::sync::Arc::new(std::sync::Mutex::new(
            crate::db::Db::open(":memory:").unwrap(),
        ));
        let app = crate::web::router(db);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap(); // required by tokio's from_std
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                axum::serve(tokio::net::TcpListener::from_std(listener).unwrap(), app)
                    .await
                    .unwrap();
            });
        });
        (Api::new(&format!("http://{addr}")), addr.port())
    }

    fn session(api: &Api, input: &str) -> Vec<Value> {
        let mut out = Vec::new();
        serve(input.as_bytes(), &mut out, api).expect("stdio session");
        String::from_utf8(out).expect("utf-8 output")
            .lines()
            .map(|l| serde_json::from_str(l).expect("each line is one JSON response"))
            .collect()
    }

    #[test]
    fn stdio_end_to_end_against_running_server() {
        let (api, _port) = spawn_server();
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#, "\n",
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#, "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#, "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_task","arguments":{"title":"From stdio","priority":"high"}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_tasks","arguments":{}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"update_task","arguments":{"id":1,"status":"done"}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"task_stats","arguments":{}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"delete_task","arguments":{"id":1}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"get_task","arguments":{"id":1}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":9,"method":"ping"}"#, "\n",
        );
        let responses = session(&api, input);

        assert_eq!(responses.len(), 9); // the notification got no reply
        assert_eq!(responses[0]["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(responses[0]["result"]["serverInfo"]["name"], "orga-rs");

        let tools = responses[1]["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 6);

        let created = responses[2]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(created.contains("From stdio"));
        assert_eq!(responses[2]["result"]["isError"], false);

        let listed = responses[3]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(listed.contains("From stdio"));

        let updated = responses[4]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(updated.contains("\"status\": \"done\""));

        let stats = responses[5]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(stats.contains("\"done\": 1"));

        assert!(responses[6]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("deleted"));

        // get after delete -> tool error, not a JSON-RPC error
        assert_eq!(responses[7]["result"]["isError"], true);
        assert!(responses[7]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("not found"));

        assert_eq!(responses[8]["result"], json!({})); // ping
    }

    #[test]
    fn unreachable_server_is_a_tool_error_with_hint() {
        // Port 1 is reserved and nothing listens there.
        let api = Api::new("http://127.0.0.1:1");
        let responses = session(
            &api,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tasks","arguments":{}}}"#,
        );
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0]["result"]["isError"], true);
        let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("cannot reach the task server"));
        assert!(text.contains("orga-rs serve"));
    }

    #[test]
    fn stdio_ignores_noise_and_answers_only_requests() {
        let (api, _port) = spawn_server();
        let input = concat!(
            "\n",
            "this is not json\n",
            r#"{"jsonrpc":"2.0","id":7,"method":"no/such/method"}"#, "\n",
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}"#, "\n",
        );
        let responses = session(&api, input);

        assert_eq!(responses.len(), 1); // noise + notification produced nothing
        assert_eq!(responses[0]["id"], 7);
        assert_eq!(responses[0]["error"]["code"], -32601);
    }
}

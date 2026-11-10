//! MCP server: exposes the task system as tools over the stdio transport.
//!
//! This is a thin bridge: it does **not** touch the database itself. Every
//! tool call is forwarded to the running web server's REST API (the same
//! server that serves the dashboard), so there is exactly one process that
//! owns the database and all clients — browser, scripts, LLM — see the same
//! data. The server's base URL comes from `ORGA_URL` (default
//! `http://127.0.0.1:3000`). If the server was started with `ORGA_TOKEN`,
//! the same variable must be set for the bridge so its requests carry the
//! bearer token.
//!
//! Wire format: JSON-RPC 2.0, one message per line (Model Context Protocol,
//! revision 2025-06-18). Notifications (messages without an "id") are accepted
//! but never answered, as the spec requires. Tool-level failures (unknown id,
//! invalid enum value, server unreachable) are reported inside a successful
//! result with `isError: true` so the calling model can read and fix them;
//! JSON-RPC errors are reserved for protocol-level problems.

use std::io::{self, BufRead, Write};

use serde_json::{json, Value};

use crate::models::{
    check_due_date, check_enum, check_title, check_workdir, PRIORITIES, RUN_STATUSES, STATUSES,
};

const PROTOCOL_VERSION: &str = "2025-06-18";
const SERVER_NAME: &str = "orga-rs";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// What a successful tool call produces: text aimed at the model, plus — for
/// tools that declare an `outputSchema` — the same data as structured content.
struct ToolOutput {
    text: String,
    structured: Option<Value>,
}

impl ToolOutput {
    fn text(text: String) -> Self {
        Self { text, structured: None }
    }
}

/// A tool's output, or an error message aimed at the model.
type ToolResult = Result<ToolOutput, String>;

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("ORGA_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());
    let base = base.trim_end_matches('/');
    // Sent as a bearer token on every request when the server requires one.
    let token = std::env::var("ORGA_TOKEN").ok().filter(|t| !t.trim().is_empty());
    let api = Api::new(base, token);
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
            "initialize" => respond(id, Ok(initialize_result())),
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

/// The `initialize` result, including spec-optional `instructions` that tell
/// the model how to use the tools well (due-date format, update semantics).
fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
        "instructions": "Tools manage a task list. Statuses are todo/in_progress/done, \
priorities low/medium/high. due_date must be a real calendar date formatted YYYY-MM-DD. \
On update_task, omit fields you do not want to change and pass due_date as an empty \
string to clear it. Use task_stats for a quick summary. Configured agents (list_agents) \
can be spawned on a task with run_task; they work in the background — poll get_run \
for the outcome. Each agent runs one task at a time; further runs are queued and start \
automatically. A task's optional workdir is the directory the agent executes in; \
list_workdirs returns the user-configured directories to pick from."
    })
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
        "list_agents" => tool_list_agents(api),
        "list_workdirs" => tool_list_workdirs(api),
        "run_task" => tool_run_task(api, &args),
        "get_run" => tool_get_run(api, &args),
        "list_runs" => tool_list_runs(api, &args),
        "delete_run" => tool_delete_run(api, &args),
        other => {
            return Err(json!({
                "code": -32602,
                "message": format!("unknown tool: {other}"),
            }))
        }
    };

    let (output, is_error) = match outcome {
        Ok(output) => (output, false),
        Err(msg) => (ToolOutput::text(msg), true),
    };
    let mut result = json!({
        "content": [{ "type": "text", "text": output.text }],
        "isError": is_error,
    });
    // Structured content is only meaningful for successful calls.
    if let Some(structured) = output.structured {
        result["structuredContent"] = structured;
    }
    Ok(result)
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
    let Some(arr) = tasks.as_array() else {
        return Err("server returned an unexpected response".to_string());
    };
    if arr.is_empty() {
        return Ok(ToolOutput::text("No tasks found.".to_string()));
    }
    // One line per task keeps long lists compact for the model;
    // get_task returns the full record.
    let mut lines = vec![format!(
        "{} task{}:",
        arr.len(),
        if arr.len() == 1 { "" } else { "s" }
    )];
    for task in arr {
        lines.push(task_line(task));
    }
    Ok(ToolOutput::text(lines.join("\n")))
}

/// One-line task summary, e.g. `#3 [high] [todo] Ship it (due 2026-11-10)`.
fn task_line(task: &Value) -> String {
    let field = |key: &str| task.get(key).and_then(Value::as_str).unwrap_or("");
    let id = task.get("id").and_then(Value::as_i64).unwrap_or_default();
    let mut line = format!(
        "#{id} [{}] [{}] {}",
        field("priority"),
        field("status"),
        field("title")
    );
    if let Some(due) = task.get("due_date").and_then(Value::as_str) {
        line.push_str(&format!(" (due {due})"));
    }
    line
}

fn tool_get_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    let task = api.get_json(&format!("/api/tasks/{id}"), "fetch task")?;
    to_text(&task).map(ToolOutput::text)
}

fn tool_create_task(api: &Api, args: &Value) -> ToolResult {
    let body = validate_task_body(args, false)?;
    let task = api.post_json("/api/tasks", &body, "create task")?;
    to_text(&task).map(ToolOutput::text)
}

fn tool_update_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    let body = validate_task_body(args, true)?;
    let task = api.put_json(&format!("/api/tasks/{id}"), &body, "update task")?;
    to_text(&task).map(ToolOutput::text)
}

fn tool_delete_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    api.delete(&format!("/api/tasks/{id}"), "delete task")?;
    Ok(ToolOutput::text(format!("task {id} deleted")))
}

fn tool_task_stats(api: &Api) -> ToolResult {
    let stats = api.get_json("/api/stats", "fetch stats")?;
    // Compact text for the model, plus structuredContent matching the tool's
    // declared outputSchema (MCP 2025-06-18 structured outputs).
    let text = serde_json::to_string(&stats).map_err(|e| format!("serialization error: {e}"))?;
    Ok(ToolOutput { text, structured: Some(stats) })
}

// ---------- agent tools ----------

fn tool_list_agents(api: &Api) -> ToolResult {
    let agents = api.get_json("/api/agents", "list agents")?;
    let Some(arr) = agents.as_array() else {
        return Err("server returned an unexpected response".to_string());
    };
    if arr.is_empty() {
        return Ok(ToolOutput::text(
            "No agents configured. Agents are configured via the dashboard or \
the REST API (POST /api/agents)."
                .to_string(),
        ));
    }
    let mut lines = vec![format!("{} agent{}:", arr.len(), if arr.len() == 1 { "" } else { "s" })];
    for agent in arr {
        let field = |key: &str| agent.get(key).and_then(Value::as_str).unwrap_or("");
        let id = agent.get("id").and_then(Value::as_i64).unwrap_or_default();
        lines.push(format!(
            "#{} {} — `{}`",
            id,
            field("name"),
            field("command")
        ));
    }
    Ok(ToolOutput::text(lines.join("\n")))
}

/// List the configured working directories (the user-maintained config file
/// the dashboard's workdir dropdown is built from). Read-only: the model
/// can pick from it for a task's workdir but not edit it.
fn tool_list_workdirs(api: &Api) -> ToolResult {
    let dirs = api.get_json("/api/workdirs", "list workdirs")?;
    let Some(arr) = dirs.as_array() else {
        return Err("server returned an unexpected response".to_string());
    };
    if arr.is_empty() {
        return Ok(ToolOutput::text(
            "No working directories configured. The list lives in the workdirs \
config file (~/.config/orga/workdirs.json, a JSON array of absolute paths) \
maintained by the user; a task's workdir can also be set directly."
                .to_string(),
        ));
    }
    let mut lines = vec![format!(
        "{} configured working director{}:",
        arr.len(),
        if arr.len() == 1 { "y" } else { "ies" }
    )];
    for dir in arr {
        lines.push(dir.as_str().unwrap_or_default().to_string());
    }
    Ok(ToolOutput::text(lines.join("\n")))
}

/// Run an agent on a task. The agent is spawned by the server and works in
/// the background; the tool returns the run record immediately.
fn tool_run_task(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    // The task id travels in the URL path; only an optional agent_id goes
    // in the body (without it, the server uses the task's assigned agent).
    let mut body = json!({});
    if let Some(agent_id) = args.get("agent_id") {
        body["agent_id"] = agent_id.clone();
    }
    let run = api.post_json(&format!("/api/tasks/{id}/run"), &body, "run agent")?;
    let status = run.get("status").and_then(Value::as_str).unwrap_or("");
    let hint = if status == "queued" {
        "The agent is busy with another run; this run is queued and will start \
automatically. Poll get_run for the outcome."
    } else {
        "Agent started. Poll get_run for the outcome."
    };
    let text = serde_json::to_string_pretty(&run)
        .map_err(|e| format!("serialization error: {e}"))?;
    Ok(ToolOutput::text(format!(
        "{hint}\n{text}",
    )))
}

fn tool_get_run(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    let run = api.get_json(&format!("/api/runs/{id}"), "fetch run")?;
    to_text(&run).map(ToolOutput::text)
}

fn tool_list_runs(api: &Api, args: &Value) -> ToolResult {
    let task_id = opt_i64(args, "task_id")?;
    let status = opt_string(args, "status")?;
    if let Some(s) = status.as_deref() {
        check_enum("status", Some(s), &RUN_STATUSES)?;
    }
    let mut path = String::from("/api/runs?");
    if let Some(t) = task_id {
        path.push_str(&format!("task_id={t}&"));
    }
    if let Some(s) = &status {
        path.push_str(&format!("status={}&", urlencode(s)));
    }
    path.push_str("limit=50");
    let runs = api.get_json(&path, "list runs")?;
    let Some(arr) = runs.as_array() else {
        return Err("server returned an unexpected response".to_string());
    };
    if arr.is_empty() {
        return Ok(ToolOutput::text("No runs found.".to_string()));
    }
    let mut lines = vec![format!("{} run{}:", arr.len(), if arr.len() == 1 { "" } else { "s" })];
    for run in arr {
        lines.push(run_line(run));
    }
    Ok(ToolOutput::text(lines.join("\n")))
}

/// One-line run summary, e.g. `#12 task #3 by coder: completed (exit 0)`.
fn run_line(run: &Value) -> String {
    let field = |key: &str| run.get(key).and_then(Value::as_str).unwrap_or("");
    let id = run.get("id").and_then(Value::as_i64).unwrap_or_default();
    let task_id = run.get("task_id").and_then(Value::as_i64).unwrap_or_default();
    let exit = run
        .get("exit_code")
        .and_then(Value::as_i64)
        .map(|c| format!("exit {c}"))
        .unwrap_or_default();
    format!(
        "#{id} task #{task_id} by {}: {}{}",
        field("agent_name"),
        field("status"),
        if exit.is_empty() { String::new() } else { format!(" ({exit})") }
    )
}

/// Delete a run record. A live run is stopped first, then removed from
/// history entirely (unlike cancelling, which keeps the record).
fn tool_delete_run(api: &Api, args: &Value) -> ToolResult {
    let id = req_id(args)?;
    api.delete(&format!("/api/runs/{id}"), "delete run")?;
    Ok(ToolOutput::text(format!("run {id} deleted")))
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
        args.get("title")
            .and_then(Value::as_str)
            .ok_or("missing required argument: title")?;
    }
    if let Some(title) = args.get("title").and_then(Value::as_str) {
        check_title(title)?;
    }
    check_enum("status", opt_string(args, "status")?.as_deref(), &STATUSES)?;
    check_enum("priority", opt_string(args, "priority")?.as_deref(), &PRIORITIES)?;
    check_due_date(opt_string(args, "due_date")?.as_deref())?;
    check_workdir(opt_string(args, "workdir")?.as_deref())?;
    Ok(body)
}

// ---------- REST client ----------

/// Minimal HTTP client for the server's JSON API.
struct Api {
    base: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl Api {
    fn new(base: &str, token: Option<String>) -> Self {
        Self {
            base: base.to_string(),
            token,
            // Generous timeout: the model may think between calls, but a hung
            // server should not block the MCP client forever.
            agent: ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(10))
                .build(),
        }
    }

    fn request(&self, method: &str, path: &str, body: Option<&Value>, what: &str) -> Result<Value, String> {
        let url = format!("{}{}", self.base, path);
        let mut request = self.agent.request(method, &url);
        if let Some(token) = &self.token {
            request = request.set("Authorization", &format!("Bearer {token}"));
        }
        let result = if let Some(json) = body {
            request
                .set("Content-Type", "application/json")
                .send_json(json) // &Value implements Serialize; no clone needed
        } else {
            request.call()
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
    // The enums come straight from the models so the schema cannot drift
    // from what the server accepts.
    let status_enum = json!(STATUSES);
    let priority_enum = json!(PRIORITIES);
    let id_schema = json!({ "type": "integer", "description": "Task id." });
    let due_date_schema = json!({
        "type": "string",
        "description": "Due date as YYYY-MM-DD, or an empty string for no due date.",
        // The empty alternative matters: "" is the documented way to clear a
        // due date on update, and a strict client would otherwise reject it.
        "pattern": "^(\\d{4}-\\d{2}-\\d{2})?$"
    });
    let workdir_schema = json!({
        "type": "string",
        "description": "Absolute path the agent process executes in, or an empty string for none.",
        "pattern": "^(/.*)?$"
    });
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
                    "title": {
                        "type": "string",
                        "description": "Short task title.",
                        "maxLength": crate::models::MAX_TITLE_LEN
                    },
                    "description": { "type": "string", "description": "Optional details." },
                    "status": { "type": "string", "enum": status_enum },
                    "priority": { "type": "string", "enum": priority_enum },
                    "due_date": due_date_schema,
                    "workdir": workdir_schema
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
                    "title": { "type": "string", "maxLength": crate::models::MAX_TITLE_LEN },
                    "description": { "type": "string" },
                    "status": { "type": "string", "enum": status_enum },
                    "priority": { "type": "string", "enum": priority_enum },
                    "due_date": {
                        "type": "string",
                        "description": "YYYY-MM-DD, or \"\" to clear.",
                        "pattern": "^(\\d{4}-\\d{2}-\\d{2})?$"
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Absolute path, or \"\" to clear.",
                        "pattern": "^(/.*)?$"
                    }
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
            "inputSchema": { "type": "object", "properties": {} },
            "outputSchema": {
                "type": "object",
                "properties": {
                    "total": { "type": "integer" },
                    "todo": { "type": "integer" },
                    "in_progress": { "type": "integer" },
                    "done": { "type": "integer" },
                    "overdue": { "type": "integer" }
                },
                "required": ["total", "todo", "in_progress", "done", "overdue"]
            }
        },
        {
            "name": "list_agents",
            "description": "List the configured agents (name + command). Agents are \
configured by the user via the dashboard or the REST API; they cannot be created \
through this tool.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "list_workdirs",
            "description": "List the configured working directories the user \
maintains in the workdirs config file (~/.config/orga/workdirs.json). Use one \
of these paths when setting a task's workdir (create_task/update_task) so the \
agent executes in the right project directory. The list is user-maintained and \
read-only for the model.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "run_task",
            "description": "Spawn a configured agent to work on a task. The agent \
process is started by the server and runs in the background; this returns \
immediately with a run record. Each agent runs one task at a time — when the \
agent is busy, the run is queued (status \"queued\") and starts automatically \
when the current run ends. Poll get_run (or list_runs) for the outcome. \
Without agent_id, the task's assigned agent is used. The agent process \
executes in the task's workdir when one is set.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": id_schema,
                    "agent_id": { "type": "integer", "description": "Agent id; defaults to the task's assigned agent." }
                },
                "required": ["id"]
            }
        },
        {
            "name": "get_run",
            "description": "Fetch one agent run by id, including its captured output.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": id_schema },
                "required": ["id"]
            }
        },
        {
            "name": "list_runs",
            "description": "List agent runs, newest first, optionally filtered by \
task_id and status (queued/running/completed/failed/timed_out/cancelled).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": { "type": "integer" },
                    "status": { "type": "string", "enum": json!(RUN_STATUSES) }
                }
            }
        },
        {
            "name": "delete_run",
            "description": "Delete a run record by id. A live run is stopped first, \
then removed from history entirely (unlike cancelling, which keeps the record). \
Use this to clean up runs you no longer want.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": id_schema },
                "required": ["id"]
            }
        }
    ])
}

// ---------- argument helpers ----------

fn to_text(value: &impl serde::Serialize) -> Result<String, String> {
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

fn opt_i64(args: &Value, key: &str) -> Result<Option<i64>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("{key} must be an integer")),
        Some(_) => Err(format!("{key} must be an integer")),
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
    fn validate_task_body_strips_id_and_checks_fields() {
        let body = validate_task_body(
            &json!({ "id": 5, "title": "ok", "status": "done", "priority": "high" }),
            false,
        )
        .unwrap();
        assert_eq!(body, json!({ "title": "ok", "status": "done", "priority": "high" }));

        assert!(validate_task_body(&json!({ "title": "x", "status": "bogus" }), false).is_err());
        assert!(validate_task_body(&json!({ "status": "done" }), false).is_err()); // no title
        assert!(validate_task_body(&json!({ "title": "  " }), false).is_err()); // blank title
        assert!(validate_task_body(&json!({ "title": "x", "due_date": "tomorrow" }), false).is_err());
        assert!(validate_task_body(&json!({ "title": "x", "due_date": "2026-1-5" }), false).is_err());
        assert!(validate_task_body(&json!({ "title": "x", "due_date": "2026-11-10" }), false).is_ok());
        // updates may omit the title entirely; "" clears the due date
        assert!(validate_task_body(&json!({ "status": "done" }), true).is_ok());
        assert!(validate_task_body(&json!({ "due_date": "" }), true).is_ok());
    }

    #[test]
    fn task_line_summarizes_a_task() {
        let line = task_line(&json!({
            "id": 3, "title": "Ship it", "status": "todo", "priority": "high",
            "due_date": "2026-11-10"
        }));
        assert_eq!(line, "#3 [high] [todo] Ship it (due 2026-11-10)");

        let line = task_line(&json!({ "id": 1, "title": "Bare", "status": "done", "priority": "low" }));
        assert_eq!(line, "#1 [low] [done] Bare");
    }

    // ---------- stdio loop against a real server ----------

    /// Spin up the real axum router on an ephemeral port and point the MCP
    /// bridge at it — a true end-to-end test of the HTTP path.
    fn spawn_server() -> (Api, u16) {
        let db: crate::db::SharedDb = std::sync::Arc::new(std::sync::Mutex::new(
            crate::db::Db::open(":memory:").unwrap(),
        ));
        let state = crate::web::AppState {
            db,
            registry: std::sync::Arc::new(crate::runner::RunRegistry::default()),
            slots: std::sync::Arc::new(crate::runner::AgentSlots::default()),
            // No config file exists at this path -> an empty workdirs list.
            workdirs: std::env::temp_dir().join("orga-rs-workdirs-mcptest.json"),
        };
        let app = crate::web::router(state, None);
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
        (Api::new(&format!("http://{addr}"), None), addr.port())
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
        assert!(responses[0]["result"]["instructions"].as_str().unwrap().contains("YYYY-MM-DD"));

        let tools = responses[1]["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 12);
        let stats_tool = tools.iter().find(|t| t["name"] == "task_stats").unwrap();
        assert!(stats_tool["outputSchema"].is_object());

        let created = responses[2]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(created.contains("From stdio"));
        assert_eq!(responses[2]["result"]["isError"], false);

        let listed = responses[3]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(listed.contains("From stdio"));
        assert!(listed.contains("#1 [high] [todo]"));

        let updated = responses[4]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(updated.contains("\"status\": \"done\""));

        let stats = responses[5]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(stats.contains("\"done\":1")); // compact, one line
        assert_eq!(responses[5]["result"]["structuredContent"]["done"], 1);

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
    fn stdio_due_date_roundtrip_and_validation() {
        let (api, _port) = spawn_server();
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_task","arguments":{"title":"dated","due_date":"2999-01-01"}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"update_task","arguments":{"id":1,"due_date":""}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_task","arguments":{"title":"bad","due_date":"tomorrow"}}}"#, "\n",
        );
        let responses = session(&api, input);

        // create with a valid date
        assert_eq!(responses[0]["result"]["isError"], false);
        assert!(responses[0]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("2999-01-01"));

        // an empty string clears the due date end-to-end
        assert_eq!(responses[1]["result"]["isError"], false);
        assert!(responses[1]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("\"due_date\": null"));

        // an invalid date is rejected before the round-trip, with a precise message
        assert_eq!(responses[2]["result"]["isError"], true);
        assert!(responses[2]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("invalid due_date 'tomorrow'"));
    }

    #[test]
    fn unreachable_server_is_a_tool_error_with_hint() {
        // Port 1 is reserved and nothing listens there.
        let api = Api::new("http://127.0.0.1:1", None);
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
    fn stdio_agent_tools_end_to_end() {
        let (api, _port) = spawn_server();
        // Configure an agent directly through the REST API (agents are
        // user-configured, not model-created), then drive it over MCP.
        let agent = api
            .post_json(
                "/api/agents",
                &json!({
                    "name": "echoer",
                    "command": "cat",
                    "prompt_template": "work on: {title}"
                }),
                "create agent",
            )
            .unwrap();
        let agent_id = agent["id"].as_i64().unwrap();

        let run_line = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": { "name": "run_task", "arguments": { "id": 1, "agent_id": agent_id } }
        })
        .to_string();
        let input = [
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_agents","arguments":{}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_task","arguments":{"title":"MCP agent task"}}}"#,
            &run_line,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_runs","arguments":{}}}"#,
        ]
        .join("\n");
        let responses = session(&api, &input);

        // list_agents shows the configured agent
        let listed = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(listed.contains("echoer"));
        assert!(listed.contains("cat"));

        // run_task spawns and returns the run record (usually still
        // "running"; a fast agent may already have finished)
        let run_text = responses[2]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(run_text.contains("Poll get_run"));
        assert!(run_text.contains("running") || run_text.contains("completed"));
        // list_runs shows the run; give the supervisor a moment to finish
        std::thread::sleep(std::time::Duration::from_millis(300));
        let responses = session(
            &api,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_runs","arguments":{}}}"#,
        );
        let runs = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(runs.contains("echoer"));
        assert!(runs.contains("completed") || runs.contains("running"));
    }

    #[test]
    fn stdio_workdirs_tool_lists_the_configured_directories() {
        let (api, _port) = spawn_server();
        // The spawn_server helper points at a config path that does not
        // exist, so the tool reports the empty list with a hint.
        let responses = session(
            &api,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_workdirs","arguments":{}}}"#,
        );
        assert_eq!(responses[0]["result"]["isError"], false);
        let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("No working directories configured"), "{text}");
        assert!(text.contains("workdirs.json"), "{text}");
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

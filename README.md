# orga-rs

A small task management system backed by SQLite, with a web dashboard, MCP
(Model Context Protocol) tool support and **configurable agents** that can be
spawned to work on a task. One binary, two modes, one database file.

## Architecture

```
        browser ──► HTTP ──┐
                          ┌┴──────────────┐
   scripts / apps ──► HTTP │ orga-rs serve │──► ~/.config/orga/tasks.db
                          └┬──────────────┘
   LLM client ──► stdio ──►│ orga-rs mcp   │
   (MCP)                   └─── HTTP ─────┘
                                │
                                ▼ spawns `sh -c <command>` per run
                          configured agents
```

`orga-rs serve` is the single process that owns the database. It serves the
dashboard and the REST API, and it spawns agent processes. `orga-rs mcp` is a
thin stdio→HTTP bridge: it forwards every tool call to the running server's
REST API, so browser, scripts and LLM clients all see the same data through
one process. The MCP bridge never touches the database itself.

## Run

```sh
cargo run                # web dashboard on http://127.0.0.1:3000
cargo run -- serve 0.0.0.0:8080   # custom address
cargo run -- mcp         # MCP stdio bridge (needs a running server)
```

The database file defaults to `~/.config/orga/tasks.db` (the directory is
created on first start); override with the `ORGA_DB` environment variable.
The MCP bridge connects to
`http://127.0.0.1:3000` by default; override with `ORGA_URL` (e.g. when the
server runs on another machine or port).

### Security

**The API has no authentication by default.** It is designed for a single
user on a trusted machine, and the default bind address (`127.0.0.1`) keeps
it local. If you bind to `0.0.0.0` or another reachable address, anyone on
that network can read, modify and delete your tasks. For anything beyond a
trusted network, set a shared token:

```sh
ORGA_TOKEN=some-secret cargo run -- serve 0.0.0.0:8080
```

With `ORGA_TOKEN` set, every `/api/*` request must carry
`Authorization: Bearer some-secret` (the dashboard page itself stays open —
it contains no data). The MCP bridge reads the same variable and sends the
token along; set it in the client's `env` block too. On Ctrl-C or SIGTERM the
server stops accepting connections and lets in-flight requests finish.

## Web dashboard

Open http://127.0.0.1:3000 — a single-page dashboard (no build step, no
JavaScript framework) that talks to the REST API:

- slim one-line header: title, stat pills (total / to do / in progress /
  done / overdue), **+ Add task**, status and priority filters, task count
- **two-pane layout**: the task list (with task ids) on the left, the
  details of the selected task on the right — description, status badge
  (click to cycle todo → in_progress → done), priority, due date, workdir,
  agent assignment, **▶ Run agent**, edit and delete
- create / edit / delete tasks, filter by status and priority; the
  description field is generously sized by default
- optional **workdir** per task — the directory its agent runs execute in
- **Agents section**: configure agents (name, command, optional prompt
  template, optional timeout), edit and delete them
- per task: pick an agent from the dropdown and press **▶ Run agent** — the
  agent process is spawned by the server and works in the background; if
  the agent is already busy, the run is queued and starts automatically
- **Agent runs** section: live status of every run (queued/running/…),
  captured output, cancel button for queued and running runs, delete button
  for any run (stops it first if still live); the view polls while runs
  are live

## Agents

An agent is a shell command you configure (e.g. a CLI coding agent, a script,
`claude -p`, `aider --yes-always`, …). When you run an agent on a task, the
server:

1. records a **run** (status `running`) and assigns the agent to the task,
2. spawns `sh -c <command>` — **in the task's working directory** when one
   is set (see below),
3. writes the rendered prompt to the process's **stdin**, followed by a
   final `###END_OF_INPUT###` line, then closes it,
4. supervises the process: exit 0 → `completed`, non-zero exit → `failed`,
   over the configured timeout (when one is set) → `timed_out`, cancelled
   via the API → `cancelled`,
5. captures stdout + stderr (last 64 KiB) into the run record.

The prompt is rendered from the agent's optional **prompt template** with
the placeholders `{id}`, `{title}`, `{description}`, `{status}`,
`{priority}`, `{due_date}` and `{workdir}` (`none` when unset). Without a
template a sensible default prompt is used. The prompt is always terminated
with a `###END_OF_INPUT###` line (not duplicated when the template already
ends with it), so an agent can detect the end of the task text without
waiting for stdin EOF.

### Per-agent queue

**Each agent runs one task at a time.** When you run an agent that already
has a live run, the new run is recorded with status `queued` and starts
automatically when the current run ends (FIFO). Queued runs can be
cancelled while they wait (`POST /api/runs/{id}/cancel`). Different agents
are independent — a busy agent never blocks another.

### Working directory

A task can have an optional **workdir**: the directory the agent process
executes in (spawned with it as `current_dir`). This is where a coding
agent should make its changes. Set it per task via the dashboard, on
create/update, or with `PUT /api/tasks/{id}/workdir` (`{"workdir": "/abs/path"}`;
`DELETE` clears it). Rules:

- must be an **absolute path** (relative paths are rejected — they would
  silently resolve against wherever `serve` was started)
- must **exist at run time**; a missing directory fails the run with a
  clear message instead of spawning in the wrong place
- unset → the agent runs in the server's working directory

**Security note:** the command runs through `sh -c`, so you can use pipes
and arguments — but the task content travels only via stdin, never on the
command line. A task title can never inject shell syntax. The command
itself is trusted configuration: anyone who can reach the API can run any
command you configured. Keep the default loopback binding (or set
`ORGA_TOKEN`) unless you trust your network.

Runs whose server died mid-flight are marked `failed` at the next startup
("server restarted while this run was waiting or active").

## REST API

| Method   | Path                          | Description                              |
| -------- | ----------------------------- | ---------------------------------------- |
| `GET`    | `/api/tasks`                  | list, optional `?status=&priority=`     |
| `POST`   | `/api/tasks`                  | create (`title` required)                |
| `GET`    | `/api/tasks/{id}`             | fetch one                                |
| `PUT`    | `/api/tasks/{id}`             | partial update (omitted fields are kept) |
| `DELETE` | `/api/tasks/{id}`             | delete                                   |
| `PUT`    | `/api/tasks/{id}/agent`       | assign an agent (`{"agent_id": n}`)      |
| `DELETE` | `/api/tasks/{id}/agent`       | remove the assignment                    |
| `PUT`    | `/api/tasks/{id}/workdir`     | set the workdir (`{"workdir": "/abs/path"}`) |
| `DELETE` | `/api/tasks/{id}/workdir`     | clear the workdir                        |
| `POST`   | `/api/tasks/{id}/run`          | spawn the agent (`{}` or `{"agent_id": n}`), returns the `running` or `queued` run |
| `GET`    | `/api/agents`                 | list configured agents                   |
| `POST`   | `/api/agents`                 | create an agent                          |
| `GET`    | `/api/agents/{id}`            | fetch one                                |
| `PUT`    | `/api/agents/{id}`            | partial update                           |
| `DELETE` | `/api/agents/{id}`            | delete (assignments are cleared)         |
| `GET`    | `/api/runs`                   | list runs, `?task_id=&status=&limit=`    |
| `GET`    | `/api/runs/{id}`              | fetch one run incl. output               |
| `POST`   | `/api/runs/{id}/cancel`       | cancel a live run (409 if not live)      |
| `DELETE` | `/api/runs/{id}`              | delete a run (stops it first if live)    |
| `GET`    | `/api/stats`                  | counters incl. overdue                   |

Task fields: `title` (required, 1–200 characters), `description`, `status`
(`todo`/`in_progress`/`done`), `priority` (`low`/`medium`/`high`), `due_date`
(`YYYY-MM-DD` — a real calendar date — or null), `workdir` (optional absolute
path, ≤ 1024 characters; the directory agents execute in), `agent_id` (set
via the assignment endpoints). On `PUT`, an empty string for `due_date` or
`workdir` clears it; omitting the field keeps it. Invalid values (unknown
status/priority, malformed dates, relative workdirs, blank or over-long
titles, unknown filter values on the list endpoint) are rejected with `400`
and a message in `{"error": "..."}`.

Agent fields: `name` (1–100 characters), `command` (1–2000 characters, run
via `sh -c`), `prompt_template` (optional, ≤ 2000 characters, placeholders
above), `timeout_secs` (optional; absent or `0` → no timeout, max 86400).
On `PUT`, omitted fields keep their value.

Run fields: `id`, `task_id`, `agent_id`, `agent_name` (snapshot), `status`
(`queued`/`running`/`completed`/`failed`/`timed_out`/`cancelled`), `exit_code`,
`output`, `started_at`, `ended_at`. `queued` means the agent's single
execution slot is taken; the run starts automatically when the slot frees.

## MCP tools

The `mcp` subcommand speaks JSON-RPC 2.0 over stdio (MCP revision 2025-06-18)
and exposes eleven tools: `list_tasks`, `get_task`, `create_task`,
`update_task`, `delete_task`, `task_stats`, `list_agents`, `run_task`,
`get_run`, `list_runs`, `delete_run`.
Tool-level failures (unknown id, invalid enum, server unreachable) are
returned as `isError: true` results so the model can read and correct them.

Agents are **user-configured** (dashboard or REST API) — the model can list
and spawn them, but not create or edit them. `run_task` returns immediately
with the run record; the model polls `get_run` for the outcome.

**The MCP bridge requires a running server.** Start `orga-rs serve` first,
then register the bridge in any stdio MCP client (Claude Desktop, etc.):

```json
{
  "mcpServers": {
    "orga": {
      "command": "/absolute/path/to/orga-rs",
      "args": ["mcp"],
      "env": { "ORGA_URL": "http://127.0.0.1:3000" }
    }
  }
}
```

If the server runs with `ORGA_TOKEN`, add it to the same `env` block so the
bridge can authenticate.

Use an **absolute path** to the binary — MCP clients often don't inherit your
working directory. If the server is not running, tool calls fail with a clear
`isError` message telling the model to start `orga-rs serve`.

## Tests

```sh
cargo test
```

Covers validation, the SQLite layer, the REST API end-to-end via axum's
`oneshot` (no network needed), the agent runner against real commands
(`cat`, failing commands, timeouts, cancellation), and the MCP bridge
end-to-end against a real axum server on an ephemeral port (stdio in →
HTTP → SQLite → back), including the agent tools.

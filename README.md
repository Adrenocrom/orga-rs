# orga-rs

A small task management system backed by SQLite, with a web dashboard and MCP
(Model Context Protocol) tool support. One binary, two modes, one database file.

## Architecture

```
        browser ──► HTTP ──┐
                          ┌┴──────────────┐
   scripts / apps ──► HTTP │ orga-rs serve │──► tasks.db (SQLite)
                          └┬──────────────┘
   LLM client ──► stdio ──►│ orga-rs mcp   │
   (MCP)                   └─── HTTP ─────┘
```

`orga-rs serve` is the single process that owns the database. It serves the
dashboard and the REST API. `orga-rs mcp` is a thin stdio→HTTP bridge: it
forwards every tool call to the running server's REST API, so browser, scripts
and LLM clients all see the same data through one process. The MCP bridge
never touches the database itself.

## Run

```sh
cargo run                # web dashboard on http://127.0.0.1:3000
cargo run -- serve 0.0.0.0:8080   # custom address
cargo run -- mcp         # MCP stdio bridge (needs a running server)
```

The database file defaults to `tasks.db` in the working directory; override
with the `ORGA_DB` environment variable. The MCP bridge connects to
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

- create / edit / delete tasks, filter by status and priority
- click a task's status badge to cycle todo → in_progress → done
- stat cards for total / todo / in progress / done / overdue

## REST API

| Method   | Path             | Description                              |
| -------- | ---------------- | ---------------------------------------- |
| `GET`    | `/api/tasks`     | list, optional `?status=&priority=`     |
| `POST`   | `/api/tasks`     | create (`title` required)                |
| `GET`    | `/api/tasks/{id}`| fetch one                                |
| `PUT`    | `/api/tasks/{id}`| partial update (omitted fields are kept) |
| `DELETE` | `/api/tasks/{id}`| delete                                   |
| `GET`    | `/api/stats`     | counters incl. overdue                   |

Task fields: `title` (required, 1–200 characters), `description`, `status`
(`todo`/`in_progress`/`done`), `priority` (`low`/`medium`/`high`), `due_date`
(`YYYY-MM-DD` — a real calendar date — or null). On `PUT`, an empty string for
`due_date` clears it; omitting the field keeps it. Invalid values (unknown
status/priority, malformed dates, blank or over-long titles, unknown filter
values on the list endpoint) are rejected with `400` and a message in
`{"error": "..."}`.

## MCP tools

The `mcp` subcommand speaks JSON-RPC 2.0 over stdio (MCP revision 2025-06-18)
and exposes six tools: `list_tasks`, `get_task`, `create_task`, `update_task`,
`delete_task`, `task_stats`. Tool-level failures (unknown id, invalid enum,
server unreachable) are returned as `isError: true` results so the model can
read and correct them.

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
`oneshot` (no network needed), and the MCP bridge end-to-end against a real
axum server on an ephemeral port (stdio in → HTTP → SQLite → back).

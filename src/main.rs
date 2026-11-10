mod db;
mod mcp;
mod models;
mod runner;
mod web;
mod workdirs;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use db::{Db, SharedDb};

/// Database file, overridable so several instances can coexist.
///
/// Defaults to `~/.config/orga/tasks.db` — one stable location regardless of
/// the working directory `serve` was started from; the directory is created
/// on first start. An empty `ORGA_DB` counts as unset: an empty filename
/// would make SQLite silently use a throwaway temporary database.
fn db_path() -> String {
    match std::env::var("ORGA_DB") {
        Ok(path) if !path.trim().is_empty() => path,
        _ => {
            let home = std::env::var("HOME").unwrap_or_else(|_| {
                panic!("ORGA_DB is unset and $HOME is not set: cannot locate the default database")
            });
            let dir = std::path::Path::new(&home).join(".config").join("orga");
            std::fs::create_dir_all(&dir)
                .unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
            dir.join("tasks.db").to_string_lossy().into_owned()
        }
    }
}

/// Path of the workdirs config file, overridable like the database path.
///
/// Defaults to `~/.config/orga/workdirs.json` — next to the database, one
/// stable location regardless of the working directory `serve` was started
/// from. An empty `ORGA_WORKDIRS` counts as unset (an empty filename would
/// silently point at the current directory).
fn workdirs_path() -> std::path::PathBuf {
    match std::env::var("ORGA_WORKDIRS") {
        Ok(path) if !path.trim().is_empty() => std::path::PathBuf::from(path),
        _ => {
            let home = std::env::var("HOME").unwrap_or_else(|_| {
                panic!("ORGA_WORKDIRS is unset and $HOME is not set: cannot locate the workdirs config")
            });
            std::path::Path::new(&home).join(".config").join("orga").join("workdirs.json")
        }
    }
}

/// Optional bearer token: when set, the API only answers requests that
/// present it. Read once at startup.
fn api_token() -> Option<String> {
    std::env::var("ORGA_TOKEN")
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

const USAGE: &str = "\
usage: orga-rs [serve [ADDR]] | mcp

  serve: web dashboard + REST API (owns the database; ORGA_DB, default ~/.config/orga/tasks.db)
  mcp:    MCP stdio bridge to a running server (ORGA_URL, default http://127.0.0.1:3000)

environment:
  ORGA_DB       database file for `serve` (default: ~/.config/orga/tasks.db)
  ORGA_URL      base URL of the server for `mcp` (default: http://127.0.0.1:3000)
  ORGA_TOKEN    when set, `serve` requires 'Authorization: Bearer <token>' on
                every API request and `mcp` sends it along
  ORGA_WORKDIRS workdirs config file for `serve` (default: ~/.config/orga/workdirs.json)";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("serve") => serve(args.get(1).map(String::as_str)).await,
        Some("mcp") => {
            if let Err(e) = mcp::run() {
                eprintln!("mcp server error: {e}");
                std::process::exit(1);
            }
        }
        Some("--help") | Some("-h") | Some("help") => println!("{USAGE}"),
        Some("--version") | Some("-V") => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        }
        Some(other) => {
            eprintln!("unknown command '{other}'");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

async fn serve(addr_arg: Option<&str>) {
    let addr: SocketAddr = addr_arg
        .map(|s| s.parse().expect("invalid listen address"))
        .unwrap_or_else(|| "127.0.0.1:3000".parse().unwrap());
    let db: SharedDb = Arc::new(Mutex::new(Db::open(&db_path()).expect("cannot open database")));
    // Runs marked `running` belong to processes of a previous server
    // process; they cannot still be alive. Flip them to `failed` before
    // accepting requests so the history is honest.
    runner::fail_orphaned_runs(&db.lock().expect("db mutex poisoned"));
    if let Err(e) = web::serve(db, addr, api_token()).await {
        eprintln!("web server error: {e}");
        std::process::exit(1);
    }
}

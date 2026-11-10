mod db;
mod mcp;
mod models;
mod runner;
mod web;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use db::{Db, SharedDb};

/// Database file, overridable so several instances can coexist.
fn db_path() -> String {
    std::env::var("ORGA_DB").unwrap_or_else(|_| "tasks.db".to_string())
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

  serve: web dashboard + REST API (owns the database; ORGA_DB, default tasks.db)
  mcp:    MCP stdio bridge to a running server (ORGA_URL, default http://127.0.0.1:3000)

environment:
  ORGA_DB     database file for `serve` (default: tasks.db)
  ORGA_URL    base URL of the server for `mcp` (default: http://127.0.0.1:3000)
  ORGA_TOKEN  when set, `serve` requires 'Authorization: Bearer <token>' on
              every API request and `mcp` sends it along";

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

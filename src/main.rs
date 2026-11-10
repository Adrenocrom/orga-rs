mod db;
mod mcp;
mod models;
mod web;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use db::{Db, SharedDb};

/// Database file, overridable so several instances can coexist.
fn db_path() -> String {
    std::env::var("ORGA_DB").unwrap_or_else(|_| "tasks.db".to_string())
}

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
        Some(other) => {
            eprintln!("unknown command '{other}'");
            eprintln!(
                "usage: orga-rs [serve [ADDR]] | mcp\n\
                 \x20 serve: web dashboard + REST API (owns the database; ORGA_DB, default tasks.db)\n\
                 \x20 mcp:    MCP stdio bridge to a running server (ORGA_URL, default http://127.0.0.1:3000)"
            );
            std::process::exit(2);
        }
    }
}

async fn serve(addr_arg: Option<&str>) {
    let addr: SocketAddr = addr_arg
        .map(|s| s.parse().expect("invalid listen address"))
        .unwrap_or_else(|| "127.0.0.1:3000".parse().unwrap());
    let db: SharedDb = Arc::new(Mutex::new(Db::open(&db_path()).expect("cannot open database")));
    if let Err(e) = web::serve(db, addr).await {
        eprintln!("web server error: {e}");
        std::process::exit(1);
    }
}

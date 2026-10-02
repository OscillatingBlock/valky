use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use config::{Environment, File, FileFormat};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};

use valky::{
    client::{ClientCache, ClientId},
    clock::SystemClock,
    lease::LeaseTable,
    net::{IncomingRouter, NetworkManager, NetworkReceiver},
    protocol::{Codec, Message, NodeId, ServerMessage},
    server::{Server, ServerId},
    store::{Key, ServerStore, Store, Value},
};

/// Everything in ./config.toml (see the repo root). Missing keys fall back
/// to the defaults below, so a minimal file with just `mode` still starts.
#[derive(Debug, Deserialize)]
struct AppConfig {
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    client: ClientConfig,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default = "default_server_id")]
    id: u64,
    #[serde(default = "default_skew_ms")]
    skew_ms: u64,
    #[serde(default = "default_lease_ms")]
    lease_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ClientConfig {
    #[serde(default = "default_server_addr")]
    server: String,
    #[serde(default = "default_server_id")]
    server_id: u64,
    #[serde(default = "default_client_id")]
    id: u64,
    #[serde(default = "default_client_listen")]
    listen: String,
    #[serde(default = "default_skew_ms")]
    skew_ms: u64,
}

fn default_mode() -> String {
    "server".to_string()
}
fn default_listen() -> String {
    "127.0.0.1:7000".to_string()
}
fn default_server_id() -> u64 {
    1
}
fn default_client_id() -> u64 {
    7
}
fn default_server_addr() -> String {
    "127.0.0.1:7000".to_string()
}
fn default_client_listen() -> String {
    "127.0.0.1:0".to_string()
}
fn default_skew_ms() -> u64 {
    100
}
fn default_lease_ms() -> u64 {
    10_000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            id: default_server_id(),
            skew_ms: default_skew_ms(),
            lease_ms: default_lease_ms(),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server: default_server_addr(),
            server_id: default_server_id(),
            id: default_client_id(),
            listen: default_client_listen(),
            skew_ms: default_skew_ms(),
        }
    }
}

/// Load config.toml (or --config=PATH). File values win over the struct
/// defaults above; VALKY_* env vars win over the file.
fn load_config(config_path: &str) -> anyhow::Result<AppConfig> {
    if !std::path::Path::new(config_path).exists() {
        anyhow::bail!(
            "config file not found: {config_path} (run from the project root, \
             or pass --config=PATH; see config.toml in the repo)"
        );
    }
    let settings = config::Config::builder()
        .add_source(File::new(config_path, FileFormat::Toml))
        // VALKY_MODE, VALKY_SERVER_LISTEN, VALKY_CLIENT_SERVER, ...
        .add_source(Environment::with_prefix("VALKY").separator("_"))
        .build()
        .context("failed to load configuration")?;
    settings
        .try_deserialize()
        .context("invalid configuration values")
}

fn main() -> anyhow::Result<()> {
    // No CLI framework in this project; subcommands are hand-rolled.
    // Usage:
    //   valky server [--listen=ADDR] [--id=N] [--skew-ms=N] [--lease-ms=N]
    //   valky client --server=ADDR [--server-id=N] [--id=N] [--listen=ADDR] [--skew-ms=N]
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return Ok(());
    }
    let config_path = args
        .iter()
        .find_map(|a| a.strip_prefix("--config="))
        .unwrap_or("config.toml");
    let cfg = load_config(config_path)?;

    // Optional positional override: `valky server` / `valky client`.
    // Otherwise the `mode` key in the config file decides.
    let mode = match args.first().map(String::as_str) {
        Some("server") | Some("client") => args[0].clone(),
        _ => cfg.mode.clone(),
    };
    match mode.as_str() {
        "server" => run_server(&cfg.server).await,
        "client" => run_client(&cfg.client).await,
        other => anyhow::bail!("unknown mode {other:?}: expected \"server\" or \"client\""),
    }
}

fn print_usage() {
    println!("valky — lease-based read-cache coherence demo");
    println!();
    println!("Configuration lives in ./config.toml (override with --config=PATH);");
    println!("VALKY_* env vars override file values. `mode` picks the half to run,");
    println!("optionally overridden by a first argument:");
    println!();
    println!("  valky [--config=PATH] [server|client]");
    println!();
    println!("Client commands (stdin):  get <key> | put <key> <value> | quit");
}

async fn run_server(cfg: &ServerConfig) -> anyhow::Result<()> {
    let id = cfg.id;
    let listen = cfg.listen.clone();

    let store: Arc<dyn Store> = Arc::new(ServerStore::default());
    let leases = LeaseTable::new(Arc::new(SystemClock), cfg.skew_ms, cfg.lease_ms);

    // Server <-> network plumbing: inbound client messages go to the
    // server loop; the server's outgoing invalidates/replies go to
    // the network dispatcher.
    let (from_client_tx, from_client_rx) = tokio::sync::mpsc::channel(512);
    let (to_client_tx, to_client_rx) = tokio::sync::mpsc::channel(512);

    let mut server = Server::new(
        store,
        leases,
        from_client_rx,
        to_client_tx,
        ServerId::new(id),
    );
    tokio::spawn(async move { server.run().await });

    let net = NetworkManager::new(
        NodeId::Server(ServerId::new(id)),
        Codec::new(),
        IncomingRouter::Server(from_client_tx),
        listen.clone(),
        String::new(), // server dials nobody
        ServerId::new(id),
    );
    println!(
        "valky server {id} listening on {listen} (skew={}ms lease={}ms)",
        cfg.skew_ms, cfg.lease_ms
    );
    net.run(NetworkReceiver::Server(to_client_rx)).await
}

async fn run_client(cfg: &ClientConfig) -> anyhow::Result<()> {
    let id = cfg.id;
    let server_addr = cfg.server.clone();
    let listen = cfg.listen.clone();

    let (to_server_tx, to_server_rx) = tokio::sync::mpsc::channel::<Message>(512);
    let (from_server_tx, from_server_rx) =
        tokio::sync::mpsc::channel::<ServerMessage>(512);

    let cache = Arc::new(ClientCache::new(
        Arc::new(SystemClock),
        ClientId::new(id),
        cfg.skew_ms,
        to_server_tx,
        from_server_rx,
    ));
    valky::client::start_client(Arc::clone(&cache));

    let net = NetworkManager::new(
        NodeId::Client(ClientId::new(id)),
        Codec::new(),
        IncomingRouter::Client(from_server_tx),
        listen.clone(),
        server_addr.clone(),
        ServerId::new(cfg.server_id),
    );
    let net_task = tokio::spawn(async move {
        if let Err(e) = net.run(NetworkReceiver::Client(to_server_rx)).await {
            eprintln!("network exited: {e:#}");
        }
    });

    println!("valky client {id} -> server {server_addr} (local {listen})");
    repl(&cache).await?;

    net_task.abort();
    Ok(())
}

async fn repl(cache: &Arc<ClientCache>) -> anyhow::Result<()> {
    println!("commands: get <key> | put <key> <value> | quit");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(3, ' ');
        match parts.next() {
            Some("get") => {
                let Some(k) = parts.next() else {
                    println!("usage: get <key>");
                    continue;
                };
                let key = Key::from_string(k.to_string());
                // Bound the wait: with no server around a miss would park forever.
                match tokio::time::timeout(Duration::from_secs(5), cache.get(&key)).await {
                    Err(_) => println!("{k}: (timed out — server unreachable?)"),
                    Ok(Err(e)) => println!("{k}: (error: {e:#})"),
                    Ok(Ok(None)) => println!("{k}: (missing)"),
                    Ok(Ok(Some(v))) => {
                        println!("{k} = {}", String::from_utf8_lossy(v.as_bytes()))
                    }
                }
            }
            Some("put") => {
                let (Some(k), Some(v)) = (parts.next(), parts.next()) else {
                    println!("usage: put <key> <value>");
                    continue;
                };
                cache
                    .write(
                        Key::from_string(k.to_string()),
                        Value::from_bytes(Bytes::copy_from_slice(v.as_bytes())),
                    )
                    .await?;
                // `write` is fire-and-forget: the server applies it after
                // invalidates/acks settle, so it may not read back instantly.
                println!("{k}: write sent (applies async)");
            }
            Some("quit" | "exit") => break,
            _ => println!("commands: get <key> | put <key> <value> | quit"),
        }
    }
    Ok(())
}

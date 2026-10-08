use anyhow::Context;
use config::{Environment, File, FileFormat};
use serde::Deserialize;

/// Everything in ./config.toml (see the repo root). Missing keys fall back
/// to the defaults below, so a minimal file with just `mode` still starts.
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub client: ClientConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_server_id")]
    pub id: u64,
    #[serde(default = "default_skew_ms")]
    pub skew_ms: u64,
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct ClientConfig {
    #[serde(default = "default_server_addr")]
    pub server: String,
    #[serde(default = "default_server_id")]
    pub server_id: u64,
    #[serde(default = "default_client_id")]
    pub id: u64,
    #[serde(default = "default_client_listen")]
    pub listen: String,
    #[serde(default = "default_skew_ms")]
    pub skew_ms: u64,
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
pub fn load_config(config_path: &str) -> anyhow::Result<AppConfig> {
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

//! Management adapters. Trading facts and lifecycle policy remain in the existing engines.
mod app;
mod catalog;
mod config;
pub mod contract;
mod control;
mod http;
mod legacy;
mod live;
mod node;
mod store;
mod v2;

pub use config::{ConsoleConfig, ManagerConfig, NodeConfig};
pub use http::{manager_router, serve_manager};
pub use node::serve_node;
mod cli_database;
mod daemon;
mod frontend;
mod frontend_config;
mod frontend_entry;
mod frontend_research;
mod frontend_targets;
mod human;
mod shell;
pub use frontend_entry::run as run_frontend;
mod client;
mod command;
mod managed_observe;
mod managed_operation;
mod managed_run;
mod managed_strategy;
pub use client::{
    ClientError as ClientUsageError, ClientOptions, exit_code as client_exit_code,
    render_error as render_client_error,
};
#[cfg(test)]
mod tests;
pub use command::{ConsoleCommand, run};

use anyhow::{Result, bail};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn now_ns() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

pub(crate) fn nonce() -> String {
    let mut bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex(&bytes)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
/// Opaque instance identity. Callers load relative database paths before hashing.
pub(crate) fn manager_identity(config: &ManagerConfig) -> String {
    let mut hash = Sha256::new();
    hash.update(b"titan-manager-identity-v1\0");
    for component in [
        config.database.as_os_str().as_encoded_bytes(),
        config.public_origin.as_bytes(),
    ] {
        hash.update((component.len() as u64).to_be_bytes());
        hash.update(component);
    }
    hex(&hash.finalize())
}

pub(crate) fn build_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        std::env::current_exe()
            .and_then(std::fs::read)
            .map(|bytes| digest(&bytes))
            .expect("cannot identify management executable")
    })
}

pub(crate) fn checked_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        bail!("invalid resource identity");
    }
    Ok(())
}
pub(crate) fn require(condition: bool, message: &str) -> Result<()> {
    anyhow::ensure!(condition, "{message}");
    Ok(())
}

pub(crate) fn inside(root: &Path, relative: &str) -> Result<std::path::PathBuf> {
    let root = root.canonicalize()?;
    let file = root.join(relative).canonicalize()?;
    if !file.starts_with(&root) || !file.is_file() {
        bail!("file is outside the resource directory");
    }
    Ok(file)
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct Fault {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}
pub(crate) fn fault(status: u16, code: &'static str, message: impl Into<String>) -> anyhow::Error {
    Fault {
        status,
        code,
        message: message.into(),
    }
    .into()
}

#[cfg(test)]
mod manager_identity_tests {
    use super::*;

    #[test]
    fn manager_identity_is_stable_and_separates_databases_and_origins() {
        let mut config = ManagerConfig {
            listen: "127.0.0.1:3200".into(),
            public_origin: "http://127.0.0.1:3200".into(),
            database: "/workspace/instance-a/manager.sqlite".into(),
            web_root: "/workspace/web".into(),
            secure_cookie: false,
            session_seconds: 3600,
            nodes: Default::default(),
            users: vec![],
        };
        let original = manager_identity(&config);
        assert_eq!(original, manager_identity(&config.clone()));
        assert_eq!(original.len(), 64);
        assert!(original.bytes().all(|byte| byte.is_ascii_hexdigit()));
        config.database = "/workspace/instance-b/manager.sqlite".into();
        assert_ne!(original, manager_identity(&config));
        config.database = "/workspace/instance-a/manager.sqlite".into();
        config.public_origin = "http://127.0.0.1:3201".into();
        assert_ne!(original, manager_identity(&config));
        config.public_origin = "http://127.0.0.1:3200".into();
        config.web_root = "/workspace/different-web".into();
        config.session_seconds = 1800;
        assert_eq!(original, manager_identity(&config));
    }
}

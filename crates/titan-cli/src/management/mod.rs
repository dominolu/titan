//! Management adapters. Trading facts and lifecycle policy remain in the existing engines.
mod app;
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
mod command;
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

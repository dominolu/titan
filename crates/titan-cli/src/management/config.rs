use super::contract::{Identity, Role};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleConfig {
    pub schema_version: u32,
    pub manager: Option<ManagerConfig>,
    pub node: Option<NodeConfig>,
}
impl ConsoleConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let path = path.canonicalize()?;
        let parent = path.parent().context("configuration has no parent")?;
        let mut value: Self = toml::from_str(&std::fs::read_to_string(&path)?)?;
        ensure!(
            value.schema_version == 1,
            "unsupported console configuration schema"
        );
        let resolve = |path: &mut PathBuf| {
            if path.is_relative() {
                *path = parent.join(&*path);
            }
        };
        if let Some(manager) = &mut value.manager {
            resolve(&mut manager.database);
            resolve(&mut manager.web_root);
        }
        if let Some(node) = &mut value.node {
            resolve(&mut node.work_root);
            resolve(&mut node.titan_program);
            if let Some(path) = &mut node.history_program {
                resolve(path);
            }
            if let Some(path) = &mut node.signing_key {
                resolve(path);
            }
            for template in node.templates.values_mut() {
                resolve(&mut template.source);
                resolve(&mut template.parameters);
                if let Some(path) = &mut template.layout {
                    resolve(path);
                }
            }
            for dataset in node.datasets.values_mut() {
                resolve(&mut dataset.trades);
                resolve(&mut dataset.quotes);
                resolve(&mut dataset.funding);
            }
            for binding in node.bindings.values_mut() {
                resolve(&mut binding.runtime_config);
            }
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerConfig {
    pub listen: String,
    pub public_origin: String,
    pub database: PathBuf,
    pub web_root: PathBuf,
    #[serde(default = "yes")]
    pub secure_cookie: bool,
    #[serde(default = "session_seconds")]
    pub session_seconds: u64,
    #[serde(default)]
    pub nodes: BTreeMap<String, RemoteNode>,
    #[serde(default)]
    pub users: Vec<User>,
}
fn yes() -> bool {
    true
}
fn session_seconds() -> u64 {
    3600
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteNode {
    #[serde(default)]
    pub live_control: bool,
    pub endpoint: String,
    pub token_env: String,
    pub environments: Vec<String>,
}
impl RemoteNode {
    pub(crate) fn token(&self) -> Result<String> {
        std::env::var(&self.token_env).context("missing node credential reference")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
    pub nodes: Vec<String>,
    #[serde(default)]
    pub accounts: Vec<String>,
    pub environments: Vec<String>,
}
impl User {
    pub(crate) fn identity(&self) -> Identity {
        Identity {
            username: self.username.clone(),
            role: self.role.clone(),
            nodes: self.nodes.clone(),
            accounts: self.accounts.clone(),
            environments: self.environments.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[serde(default)]
    pub artifact_sources: BTreeMap<String, RemoteNode>,
    #[serde(default)]
    pub live_control: bool,
    pub id: String,
    pub listen: String,
    pub token_env: String,
    pub work_root: PathBuf,
    pub titan_program: PathBuf,
    pub environments: Vec<String>,
    pub history_program: Option<PathBuf>,
    pub signing_key: Option<PathBuf>,
    pub signing_key_id: Option<String>,
    #[serde(default)]
    pub templates: BTreeMap<String, Template>,
    #[serde(default)]
    pub datasets: BTreeMap<String, Dataset>,
    #[serde(default)]
    pub bindings: BTreeMap<String, Binding>,
    #[serde(default)]
    pub observe_bindings: Vec<String>,
    #[serde(default = "task_seconds")]
    pub task_seconds: u64,
}
fn task_seconds() -> u64 {
    1800
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub source: PathBuf,
    pub parameters: PathBuf,
    pub layout: Option<PathBuf>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub trades: PathBuf,
    pub quotes: PathBuf,
    pub funding: PathBuf,
    pub regime: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    #[serde(default)]
    pub account_identity: Option<String>,
    pub account: String,
    pub instrument: String,
    pub runtime_config: PathBuf,
    pub strategy_key: String,
    pub max_exposure_quote: String,
    pub max_price_deviation_bps: u32,
    pub max_duration_seconds: u64,
}

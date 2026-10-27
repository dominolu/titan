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
            for template in node.parameter_presets.values_mut() {
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
    #[serde(default, alias = "artifact_sources")]
    pub strategy_sources: BTreeMap<String, RemoteNode>,
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
    #[serde(default, alias = "templates")]
    pub parameter_presets: BTreeMap<String, StrategyParameterPreset>,
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
pub struct StrategyParameterPreset {
    /// Logical strategy workspace, independent of template aliases and runtime strategy IDs.
    #[serde(default)]
    pub strategy_id: Option<String>,
    #[serde(default, rename = "research_id")]
    pub legacy_research_id: Option<String>,
    #[serde(default, alias = "research_name")]
    pub strategy_name: Option<String>,
    pub source: PathBuf,
    pub parameters: PathBuf,
    pub layout: Option<PathBuf>,
}

impl StrategyParameterPreset {
    pub(crate) fn strategy_identity(&self, node_id: &str) -> Result<(String, String)> {
        let source = self.source.canonicalize()?;
        ensure!(
            self.strategy_id.is_none() || self.legacy_research_id.is_none(),
            "strategy identity supplied twice"
        );
        let id = if let Some(id) = &self.strategy_id {
            super::checked_id(id)?;
            id.clone()
        } else if let Some(id) = &self.legacy_research_id {
            super::checked_id(id)?;
            format!("research-{id}")
        } else {
            // Aliases of the same source share a workspace; editing bytes keeps its identity.
            format!(
                "source-{}",
                super::digest(
                    &[
                        node_id.as_bytes(),
                        b"\0",
                        source.as_os_str().as_encoded_bytes(),
                    ]
                    .concat()
                )
            )
        };
        super::checked_id(&id)?;
        let name = self.strategy_name.clone().unwrap_or_else(|| {
            self.strategy_id.clone().unwrap_or_else(|| {
                source
                    .parent()
                    .and_then(Path::file_name)
                    .map(|v| v.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "策略研究".into())
            })
        });
        ensure!(
            !name.trim().is_empty() && name.len() <= 256,
            "invalid strategy name"
        );
        Ok((id, name))
    }
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

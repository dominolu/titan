use std::{
    collections::{BTreeSet, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use titan_account_plugin::{
    ACCOUNT_EVENT_SCHEMA_VERSION, ACCOUNT_EVENT_TYPES, AccountAdminApi, AccountAdminRequest,
    AccountAdminResponse, AccountDefinition, FILL_EVENT, FILL_EVENT_SCHEMA_VERSION,
};
use titan_connector_loader::{
    LoadedConnectorPlugin, account_plugin_factory, load_connector_plugins, market_plugin_factory,
};
use titan_event_engine::{EventClass, EventEngineConfig, PoolKind, TitanCoreRuntime};
use titan_market_plugin::{
    MARKET_EVENT_SCHEMA_VERSION, MARKET_EVENT_TYPES, MarketAdminApi, MarketAdminRequest,
    MarketAdminResponse, MarketSourceDefinition,
};
use titan_plugin_engine::{
    ApiVersion, CallbackBudget, ConfigSnapshot, EventQos, ExecutionModel, ExecutionSpec,
    PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION, PLUGIN_RUNTIME_EVENT_TYPES, PluginError, PluginSpec,
    ServiceId, ServiceKey, ServiceScope, StopReason, SubscriptionLimits, TraceContext,
};
use titan_python_host::EmbeddedPythonCompiler;
use titan_strategy_plugin::{
    InProcessNumbaLoaderFactory, NativeStrategyRuntimeFactory, STRATEGY_PLUGIN_MANIFEST,
    STRATEGY_PLUGIN_TYPE, StrategyAdminApi, StrategyAdminRequest, StrategyAdminResponse,
    StrategyDefinition, StrategyOperationState, StrategyPluginConfig, StrategyPluginFactory,
    StrategyRecoveryPolicy,
};

pub const APPLICATION_CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub event_engine: EventEngineConfig,
    #[serde(default)]
    pub connector_plugin_packages: Vec<PathBuf>,
    pub plugins: Vec<PluginConfig>,
    #[serde(default)]
    pub market_sources: Vec<MarketSourceDefinition>,
    #[serde(default)]
    pub accounts: Vec<AccountDefinition>,
    #[serde(default)]
    pub strategies: Vec<StrategyDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub instance_id: String,
    pub plugin_type: String,
    #[serde(default = "one")]
    pub config_schema_version: u32,
    #[serde(default = "one_u64")]
    pub config_version: u64,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub execution: PluginExecutionConfig,
    #[serde(default)]
    pub subscription: PluginSubscriptionConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginExecutionConfig {
    #[serde(default = "passive_execution")]
    pub model: ExecutionModel,
    #[serde(default)]
    pub cpu_affinity: Option<usize>,
    #[serde(default)]
    pub callback_budget: Option<CallbackBudgetConfig>,
}

impl Default for PluginExecutionConfig {
    fn default() -> Self {
        Self {
            model: ExecutionModel::Passive,
            cpu_affinity: None,
            callback_budget: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackBudgetConfig {
    pub soft_budget_us: u64,
    pub stall_threshold_us: u64,
    pub max_consecutive_violations: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginSubscriptionConfig {
    #[serde(default = "default_subscription_capacity")]
    pub max_capacity: usize,
    #[serde(default = "default_qos")]
    pub allowed_qos: BTreeSet<EventQos>,
}

impl Default for PluginSubscriptionConfig {
    fn default() -> Self {
        Self {
            max_capacity: default_subscription_capacity(),
            allowed_qos: default_qos(),
        }
    }
}

#[derive(Clone)]
pub struct AdaptedConfiguration {
    pub event_engine: EventEngineConfig,
    pub connector_plugin_packages: Vec<PathBuf>,
    pub plugin_specs: Vec<PluginSpec>,
    pub market_sources: Vec<MarketSourceDefinition>,
    pub accounts: Vec<AccountDefinition>,
    pub strategies: Vec<StrategyDefinition>,
    strategy_bootstrap: Option<StrategyBootstrap>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyPluginBootstrapConfig {
    #[serde(default)]
    allowed_artifact_roots: Vec<PathBuf>,
    #[serde(default)]
    python_paths: Vec<PathBuf>,
}

#[derive(Clone)]
struct StrategyBootstrap {
    config: StrategyPluginConfig,
    python_paths: Vec<PathBuf>,
    strategy_types: BTreeSet<Arc<str>>,
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("cannot read application config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid TOML in application config {path}: {source}")]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("unsupported application config schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid application configuration: {0}")]
    Invalid(String),
    #[error("cannot resolve plugin package {path}: {source}")]
    PackagePath {
        path: PathBuf,
        source: std::io::Error,
    },
}

pub struct ConfigurationAdapter;

impl ConfigurationAdapter {
    pub fn load_toml(path: impl AsRef<Path>) -> Result<AdaptedConfiguration, ConfigurationError> {
        let path = path.as_ref();
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigurationError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let config = toml::from_str::<ApplicationConfig>(&contents).map_err(|source| {
            ConfigurationError::Toml {
                path: path.to_path_buf(),
                source,
            }
        })?;
        Self::adapt(config, path.parent().unwrap_or_else(|| Path::new(".")))
    }

    pub fn adapt(
        mut config: ApplicationConfig,
        config_directory: &Path,
    ) -> Result<AdaptedConfiguration, ConfigurationError> {
        if config.schema_version != APPLICATION_CONFIG_SCHEMA_VERSION {
            return Err(ConfigurationError::UnsupportedSchema(config.schema_version));
        }
        config
            .event_engine
            .validate()
            .map_err(|error| ConfigurationError::Invalid(error.to_string()))?;

        let mut package_paths = Vec::with_capacity(config.connector_plugin_packages.len());
        let mut unique_packages = HashSet::new();
        for path in config.connector_plugin_packages {
            let path = if path.is_absolute() {
                path
            } else {
                config_directory.join(path)
            };
            let path =
                std::fs::canonicalize(&path).map_err(|source| ConfigurationError::PackagePath {
                    path: path.clone(),
                    source,
                })?;
            if !unique_packages.insert(path.clone()) {
                return Err(ConfigurationError::Invalid(format!(
                    "duplicate connector plugin package {}",
                    path.display()
                )));
            }
            package_paths.push(path);
        }

        let mut instance_ids = HashSet::new();
        let mut plugin_specs = Vec::with_capacity(config.plugins.len());
        let mut strategy_bootstrap_config = None;
        for plugin in config.plugins {
            if plugin.instance_id.trim().is_empty() || plugin.plugin_type.trim().is_empty() {
                return Err(ConfigurationError::Invalid(
                    "plugin instance_id and plugin_type must not be empty".into(),
                ));
            }
            if !instance_ids.insert(plugin.instance_id.clone()) {
                return Err(ConfigurationError::Invalid(format!(
                    "duplicate plugin instance_id {}",
                    plugin.instance_id
                )));
            }
            if plugin.config_schema_version == 0 || plugin.config_version == 0 {
                return Err(ConfigurationError::Invalid(format!(
                    "plugin {} has a zero configuration version",
                    plugin.instance_id
                )));
            }
            if plugin.plugin_type == STRATEGY_PLUGIN_TYPE {
                if strategy_bootstrap_config.is_some() {
                    return Err(ConfigurationError::Invalid(
                        "only one titan.strategy plugin instance is supported".into(),
                    ));
                }
                strategy_bootstrap_config = Some(
                    serde_json::from_value::<StrategyPluginBootstrapConfig>(plugin.config.clone())
                        .map_err(|error| {
                            ConfigurationError::Invalid(format!(
                                "invalid titan.strategy plugin config: {error}"
                            ))
                        })?,
                );
            }
            plugin_specs.push(PluginSpec {
                instance_id: Arc::from(plugin.instance_id),
                plugin_type: Arc::from(plugin.plugin_type),
                config: Arc::new(
                    ConfigSnapshot::new(plugin.config_version, plugin.config)
                        .with_schema_version(plugin.config_schema_version),
                ),
                enabled: plugin.enabled,
                execution: ExecutionSpec {
                    model: plugin.execution.model,
                    cpu_affinity: plugin.execution.cpu_affinity,
                    callback_budget: plugin.execution.callback_budget.map(|budget| {
                        CallbackBudget {
                            soft_budget_us: budget.soft_budget_us,
                            stall_threshold_us: budget.stall_threshold_us,
                            max_consecutive_violations: budget.max_consecutive_violations,
                        }
                    }),
                },
                subscription_limits: SubscriptionLimits {
                    max_capacity: plugin.subscription.max_capacity,
                    allowed_qos: plugin.subscription.allowed_qos,
                },
                service_scopes: vec![],
                required_service_scopes: vec![],
            });
        }

        validate_runtime_definitions(&config.market_sources, &config.accounts)?;
        let strategy_bootstrap = adapt_strategies(
            &mut config.strategies,
            config_directory,
            strategy_bootstrap_config,
            plugin_specs
                .iter()
                .any(|spec| spec.enabled && spec.plugin_type.as_ref() == STRATEGY_PLUGIN_TYPE),
        )?;
        Ok(AdaptedConfiguration {
            event_engine: config.event_engine,
            connector_plugin_packages: package_paths,
            plugin_specs,
            market_sources: config.market_sources,
            accounts: config.accounts,
            strategies: config.strategies,
            strategy_bootstrap,
        })
    }
}

#[derive(Debug, Error)]
pub enum ApplicationRuntimeError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Plugin(#[from] PluginError),
    #[error(transparent)]
    Core(#[from] titan_event_engine::CoreRuntimeError),
    #[error("market runtime definition failed: {0}")]
    Market(String),
    #[error("account runtime definition failed: {0}")]
    Account(String),
    #[error("strategy runtime definition failed: {0}")]
    Strategy(String),
}

pub struct ConfiguredCoreRuntime {
    core: TitanCoreRuntime,
    // Sessions must remain alive for every factory and connector created from foreign code.
    _connector_plugins: Vec<LoadedConnectorPlugin>,
}

impl ConfiguredCoreRuntime {
    pub fn start_from_toml(path: impl AsRef<Path>) -> Result<Self, ApplicationRuntimeError> {
        Self::start(ConfigurationAdapter::load_toml(path)?)
    }

    pub fn start(config: AdaptedConfiguration) -> Result<Self, ApplicationRuntimeError> {
        let AdaptedConfiguration {
            event_engine,
            connector_plugin_packages,
            plugin_specs,
            market_sources,
            accounts,
            strategies,
            strategy_bootstrap,
        } = config;
        let connector_plugins = load_connector_plugins(&connector_plugin_packages)?;
        let mut core = TitanCoreRuntime::new(event_engine, ApiVersion::new(1, 0))?;
        register_event_catalog(&core)?;
        core.plugins_mut().register(
            Arc::new(market_plugin_factory(&connector_plugins)),
            semver::Version::new(1, 0, 0),
            "builtin:titan-market-plugin",
        )?;
        core.plugins_mut().register(
            Arc::new(account_plugin_factory(&connector_plugins)),
            semver::Version::new(1, 0, 0),
            "builtin:titan-account-plugin",
        )?;
        if let Some(bootstrap) = strategy_bootstrap {
            let mut compiler = EmbeddedPythonCompiler::default();
            for path in bootstrap.python_paths {
                compiler = compiler.with_python_path(path);
            }
            let mut factory = StrategyPluginFactory::from_plugin_services(
                bootstrap.config,
                core.event_handle().as_ref().clone(),
            )
            .with_loader(Arc::new(InProcessNumbaLoaderFactory::new(Arc::new(
                compiler,
            ))))
            .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            for strategy_type in bootstrap.strategy_types {
                factory = factory
                    .with_runtime_factory(Arc::new(NativeStrategyRuntimeFactory::new(
                        strategy_type,
                    )))
                    .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            }
            core.plugins_mut().register(
                Arc::new(factory),
                semver::Version::new(1, 0, 0),
                "builtin:titan-strategy-plugin",
            )?;
        }
        core.start_with_plugins(&plugin_specs)?;

        let mut runtime = Self {
            core,
            _connector_plugins: connector_plugins,
        };
        if let Err(error) = runtime.apply_definitions(market_sources, accounts, strategies) {
            let _ = runtime.core.shutdown(StopReason::Failure);
            return Err(error);
        }
        Ok(runtime)
    }

    pub fn core(&self) -> &TitanCoreRuntime {
        &self.core
    }

    pub fn core_mut(&mut self) -> &mut TitanCoreRuntime {
        &mut self.core
    }

    pub fn shutdown(&mut self, reason: StopReason) -> Result<(), ApplicationRuntimeError> {
        self.core.shutdown(reason)?;
        Ok(())
    }

    fn apply_definitions(
        &mut self,
        market_sources: Vec<MarketSourceDefinition>,
        accounts: Vec<AccountDefinition>,
        strategies: Vec<StrategyDefinition>,
    ) -> Result<(), ApplicationRuntimeError> {
        if !market_sources.is_empty() {
            let market = self
                .core
                .plugins()
                .services()
                .bind_typed::<MarketAdminApi>(&service_key("titan.market", "admin"))
                .ok_or_else(|| {
                    ApplicationRuntimeError::Market("admin service is unavailable".into())
                })?;
            for definition in market_sources {
                let enabled = definition.enabled;
                let response = market
                    .call(
                        MarketAdminRequest::Create(definition),
                        TraceContext::default(),
                    )?
                    .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                let MarketAdminResponse::Handle(handle) = response else {
                    return Err(ApplicationRuntimeError::Market(
                        "create returned an unexpected response".into(),
                    ));
                };
                if enabled {
                    market
                        .call(MarketAdminRequest::Start(handle), TraceContext::default())?
                        .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                }
            }
        }

        if !accounts.is_empty() {
            let account = self
                .core
                .plugins()
                .services()
                .bind_typed::<AccountAdminApi>(&service_key("titan.account", "admin"))
                .ok_or_else(|| {
                    ApplicationRuntimeError::Account("admin service is unavailable".into())
                })?;
            for definition in accounts {
                let enabled = definition.enabled;
                let response = account
                    .call(
                        AccountAdminRequest::Create(definition),
                        TraceContext::default(),
                    )?
                    .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
                let AccountAdminResponse::Handle(handle) = response else {
                    return Err(ApplicationRuntimeError::Account(
                        "create returned an unexpected response".into(),
                    ));
                };
                if enabled {
                    account
                        .call(AccountAdminRequest::Start(handle), TraceContext::default())?
                        .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
                }
            }
        }
        if !strategies.is_empty() {
            let strategy = self
                .core
                .plugins()
                .services()
                .bind_typed::<StrategyAdminApi>(&service_key("titan.strategy", "admin"))
                .ok_or_else(|| {
                    ApplicationRuntimeError::Strategy("admin service is unavailable".into())
                })?;
            for definition in strategies {
                let enabled = definition.enabled;
                let startup_timeout = definition.runtime.startup_timeout;
                let response = strategy
                    .call(
                        StrategyAdminRequest::Create(definition),
                        TraceContext::default(),
                    )?
                    .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                let StrategyAdminResponse::Handle(handle) = response else {
                    return Err(ApplicationRuntimeError::Strategy(
                        "create returned an unexpected response".into(),
                    ));
                };
                if enabled {
                    let prepare = strategy
                        .call(
                            StrategyAdminRequest::Prepare(handle),
                            TraceContext::default(),
                        )?
                        .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                    let StrategyAdminResponse::OperationId(prepare) = prepare else {
                        return Err(ApplicationRuntimeError::Strategy(
                            "prepare returned an unexpected response".into(),
                        ));
                    };
                    wait_strategy_operation(&strategy, prepare, startup_timeout)?;
                    let start = strategy
                        .call(StrategyAdminRequest::Start(handle), TraceContext::default())?
                        .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                    let StrategyAdminResponse::OperationId(start) = start else {
                        return Err(ApplicationRuntimeError::Strategy(
                            "start returned an unexpected response".into(),
                        ));
                    };
                    wait_strategy_operation(&strategy, start, startup_timeout)?;
                }
            }
        }
        Ok(())
    }
}

fn wait_strategy_operation(
    strategy: &titan_plugin_engine::ServiceHandle<StrategyAdminApi>,
    operation: titan_strategy_plugin::StrategyOperationId,
    timeout: std::time::Duration,
) -> Result<(), ApplicationRuntimeError> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let response = strategy
            .call(
                StrategyAdminRequest::Operation(operation),
                TraceContext::default(),
            )?
            .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
        let StrategyAdminResponse::Operation(snapshot) = response else {
            return Err(ApplicationRuntimeError::Strategy(
                "operation returned an unexpected response".into(),
            ));
        };
        match snapshot.state {
            StrategyOperationState::Succeeded => return Ok(()),
            StrategyOperationState::Failed => {
                return Err(ApplicationRuntimeError::Strategy(format!(
                    "strategy operation failed: {}",
                    snapshot.detail
                )));
            }
            StrategyOperationState::Pending if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            StrategyOperationState::Pending => {
                return Err(ApplicationRuntimeError::Strategy(
                    "strategy operation timed out".into(),
                ));
            }
        }
    }
}

fn register_event_catalog(core: &TitanCoreRuntime) -> Result<(), PluginError> {
    for event_type in PLUGIN_RUNTIME_EVENT_TYPES {
        core.event_handle()
            .register_event(
                event_type,
                PLUGIN_RUNTIME_EVENT_SCHEMA_VERSION,
                EventClass::Critical,
                PoolKind::Snapshot,
            )
            .map_err(event_error)?;
    }
    for event_type in MARKET_EVENT_TYPES {
        core.event_handle()
            .register_event(
                event_type,
                MARKET_EVENT_SCHEMA_VERSION,
                EventClass::Market,
                PoolKind::MarketBatch,
            )
            .map_err(event_error)?;
    }
    for event_type in ACCOUNT_EVENT_TYPES {
        core.event_handle()
            .register_event(
                event_type,
                if event_type == FILL_EVENT {
                    FILL_EVENT_SCHEMA_VERSION
                } else {
                    ACCOUNT_EVENT_SCHEMA_VERSION
                },
                EventClass::Critical,
                PoolKind::SmallEvent,
            )
            .map_err(event_error)?;
    }
    for event in &STRATEGY_PLUGIN_MANIFEST.publishes {
        core.event_handle()
            .register_event(
                event.event_type.clone(),
                event.schema_version,
                EventClass::Critical,
                PoolKind::SmallEvent,
            )
            .map_err(event_error)?;
    }
    Ok(())
}

fn validate_runtime_definitions(
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
) -> Result<(), ConfigurationError> {
    let mut source_keys = HashSet::new();
    for source in market_sources {
        if source.source_key.trim().is_empty()
            || source.connector_type.trim().is_empty()
            || source.instruments.is_empty()
            || !source_keys.insert(source.source_key.clone())
        {
            return Err(ConfigurationError::Invalid(format!(
                "invalid or duplicate market source {}",
                source.source_key
            )));
        }
    }
    let mut account_keys = HashSet::new();
    let mut account_ids = HashSet::new();
    for account in accounts {
        if account.account_key.trim().is_empty()
            || account.connector_type.trim().is_empty()
            || !account_keys.insert(account.account_key.clone())
            || !account_ids.insert(account.account_id)
        {
            return Err(ConfigurationError::Invalid(format!(
                "invalid or duplicate account {}",
                account.account_key
            )));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct StrategyManifestProbe {
    strategy_type: String,
}

fn adapt_strategies(
    strategies: &mut [StrategyDefinition],
    config_directory: &Path,
    bootstrap: Option<StrategyPluginBootstrapConfig>,
    plugin_enabled: bool,
) -> Result<Option<StrategyBootstrap>, ConfigurationError> {
    if strategies.is_empty() && !plugin_enabled {
        return Ok(None);
    }
    if !plugin_enabled {
        return Err(ConfigurationError::Invalid(
            "strategy definitions require one enabled titan.strategy plugin".into(),
        ));
    }
    let bootstrap = bootstrap.unwrap_or_default();
    let mut allowed_roots = Vec::with_capacity(bootstrap.allowed_artifact_roots.len());
    for root in bootstrap.allowed_artifact_roots {
        allowed_roots.push(canonical_config_path(
            config_directory,
            &root,
            "artifact root",
        )?);
    }
    if !strategies.is_empty() && allowed_roots.is_empty() {
        return Err(ConfigurationError::Invalid(
            "titan.strategy config requires at least one allowed_artifact_roots entry".into(),
        ));
    }
    let mut python_paths = Vec::with_capacity(bootstrap.python_paths.len());
    for path in bootstrap.python_paths {
        python_paths.push(canonical_config_path(
            config_directory,
            &path,
            "Python path",
        )?);
    }
    let mut strategy_types = BTreeSet::new();
    for definition in strategies {
        if definition.recovery != StrategyRecoveryPolicy::Fresh {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} must use recovery = fresh",
                definition.strategy_key
            )));
        }
        if definition.package.loader_type.as_ref() != "numba-python" {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} uses unsupported production loader {}",
                definition.strategy_key, definition.package.loader_type
            )));
        }
        let relative = definition
            .package
            .uri
            .strip_prefix("file://")
            .ok_or_else(|| {
                ConfigurationError::Invalid(format!(
                    "strategy {} package must use a file:// URI",
                    definition.strategy_key
                ))
            })?;
        let package_root =
            canonical_config_path(config_directory, Path::new(relative), "strategy package")?;
        if !package_root.is_dir() {
            return Err(ConfigurationError::Invalid(format!(
                "strategy package {} is not a directory",
                package_root.display()
            )));
        }
        if !allowed_roots
            .iter()
            .any(|root| package_root.starts_with(root))
        {
            return Err(ConfigurationError::Invalid(format!(
                "strategy package {} is outside allowed_artifact_roots",
                package_root.display()
            )));
        }
        let bytes =
            std::fs::read(package_root.join("strategy-manifest.json")).map_err(|error| {
                ConfigurationError::Invalid(format!(
                    "cannot read strategy {} manifest: {error}",
                    definition.strategy_key
                ))
            })?;
        let probe = serde_json::from_slice::<StrategyManifestProbe>(&bytes).map_err(|error| {
            ConfigurationError::Invalid(format!(
                "invalid strategy {} manifest: {error}",
                definition.strategy_key
            ))
        })?;
        if probe.strategy_type.trim().is_empty() {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} manifest has an empty strategy_type",
                definition.strategy_key
            )));
        }
        strategy_types.insert(Arc::from(probe.strategy_type));
        definition.package.uri = Arc::from(format!("file://{}", package_root.display()));
    }
    python_paths.sort();
    python_paths.dedup();
    let mut config = StrategyPluginConfig::default();
    config.allowed_artifact_roots = allowed_roots
        .iter()
        .map(|root| Arc::from(root.to_string_lossy().as_ref()))
        .collect::<Vec<_>>()
        .into();
    Ok(Some(StrategyBootstrap {
        config,
        python_paths,
        strategy_types,
    }))
}

fn canonical_config_path(
    config_directory: &Path,
    path: &Path,
    kind: &str,
) -> Result<PathBuf, ConfigurationError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_directory.join(path)
    };
    std::fs::canonicalize(&path).map_err(|error| {
        ConfigurationError::Invalid(format!("cannot resolve {kind} {}: {error}", path.display()))
    })
}

fn event_error(error: titan_event_engine::EngineError) -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        titan_plugin_engine::PluginIdentity::new("titan.core", "event-catalog"),
        titan_plugin_engine::LifecycleState::Discovered,
        "register_event_catalog",
        error.to_string(),
    )
}

fn service_key(namespace: &str, name: &str) -> ServiceKey {
    ServiceKey {
        id: ServiceId::new(namespace, name),
        version: semver::Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}

const fn one() -> u32 {
    1
}
const fn one_u64() -> u64 {
    1
}
const fn enabled() -> bool {
    true
}
const fn passive_execution() -> ExecutionModel {
    ExecutionModel::Passive
}
const fn default_subscription_capacity() -> usize {
    1024
}
fn default_qos() -> BTreeSet<EventQos> {
    [
        EventQos::Latest,
        EventQos::ReliableOrdered,
        EventQos::BestEffort,
    ]
    .into_iter()
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn plugin(instance_id: &str, plugin_type: &str) -> PluginConfig {
        PluginConfig {
            instance_id: instance_id.into(),
            plugin_type: plugin_type.into(),
            config_schema_version: 1,
            config_version: 1,
            config: serde_json::json!({}),
            enabled: true,
            execution: PluginExecutionConfig::default(),
            subscription: PluginSubscriptionConfig::default(),
        }
    }

    #[test]
    fn adapter_produces_standard_specs_and_rejects_duplicate_runtime_keys() {
        let config = ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            connector_plugin_packages: vec![],
            plugins: vec![plugin("market", "titan.market")],
            market_sources: vec![],
            accounts: vec![],
            strategies: vec![],
        };
        let adapted = ConfigurationAdapter::adapt(config, Path::new(".")).unwrap();
        assert_eq!(adapted.plugin_specs.len(), 1);
        assert_eq!(adapted.plugin_specs[0].plugin_type.as_ref(), "titan.market");
        assert_eq!(adapted.plugin_specs[0].config.schema_version, 1);

        let mut duplicate = plugin("market", "titan.account");
        duplicate.config_version = 2;
        let config = ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            connector_plugin_packages: vec![],
            plugins: vec![plugin("market", "titan.market"), duplicate],
            market_sources: vec![],
            accounts: vec![],
            strategies: vec![],
        };
        assert!(matches!(
            ConfigurationAdapter::adapt(config, Path::new(".")),
            Err(ConfigurationError::Invalid(_))
        ));
    }

    #[test]
    fn toml_adapter_resolves_only_explicit_plugin_packages_relative_to_config() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "titan-configuration-adapter-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let package = directory.join("venue.plugin");
        std::fs::write(&package, b"fixture").unwrap();
        let config_path = directory.join("runtime.toml");
        std::fs::write(
            &config_path,
            r#"
schema_version = 1
connector_plugin_packages = ["venue.plugin"]

[[plugins]]
instance_id = "market"
plugin_type = "titan.market"
config_schema_version = 1
config_version = 7
config = {}
"#,
        )
        .unwrap();

        let adapted = ConfigurationAdapter::load_toml(&config_path).unwrap();
        let canonical_package = std::fs::canonicalize(&package).unwrap();
        assert_eq!(adapted.connector_plugin_packages, vec![canonical_package]);
        assert_eq!(adapted.plugin_specs.len(), 1);
        assert_eq!(adapted.plugin_specs[0].config.version, 7);

        std::fs::remove_file(config_path).unwrap();
        std::fs::remove_file(package).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn configured_runtime_starts_builtin_plugins_through_core_transaction() {
        let config = ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            connector_plugin_packages: vec![],
            plugins: vec![
                plugin("market", "titan.market"),
                plugin("account", "titan.account"),
            ],
            market_sources: vec![],
            accounts: vec![],
            strategies: vec![],
        };
        let adapted = ConfigurationAdapter::adapt(config, Path::new(".")).unwrap();
        let mut runtime = ConfiguredCoreRuntime::start(adapted).unwrap();
        assert_eq!(runtime.core().plugins().diagnostics().len(), 2);
        assert!(
            runtime
                .core()
                .plugins()
                .diagnostics()
                .iter()
                .all(|item| item.lifecycle_state == titan_plugin_engine::LifecycleState::Running)
        );
        runtime.shutdown(StopReason::Shutdown).unwrap();
        assert_eq!(runtime.core().events().arena().outstanding_blocks(), 0);
    }

    #[test]
    fn configured_runtime_binds_strategy_dependencies_without_risk_or_recovery_services() {
        let mut strategy = plugin("strategy", STRATEGY_PLUGIN_TYPE);
        strategy.execution.model = ExecutionModel::Dedicated;
        let config = ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            connector_plugin_packages: vec![],
            plugins: vec![
                plugin("market", "titan.market"),
                plugin("account", "titan.account"),
                strategy,
            ],
            market_sources: vec![],
            accounts: vec![],
            strategies: vec![],
        };
        let adapted = ConfigurationAdapter::adapt(config, Path::new(".")).unwrap();
        let mut runtime = ConfiguredCoreRuntime::start(adapted).unwrap();
        assert_eq!(runtime.core().plugins().diagnostics().len(), 3);
        assert!(
            runtime
                .core()
                .plugins()
                .services()
                .bind_typed::<StrategyAdminApi>(&service_key("titan.strategy", "admin"))
                .is_some()
        );
        assert!(
            runtime
                .core()
                .plugins()
                .plan()
                .unwrap()
                .entries()
                .find(|entry| entry.spec.plugin_type.as_ref() == STRATEGY_PLUGIN_TYPE)
                .unwrap()
                .bindings
                .iter()
                .filter_map(|binding| binding.key.as_ref())
                .all(|key| {
                    key.id.namespace.as_ref() != "titan.risk"
                        && key.id.namespace.as_ref() != "titan.store"
                })
        );
        runtime.shutdown(StopReason::Shutdown).unwrap();
        assert_eq!(runtime.core().events().arena().outstanding_blocks(), 0);
    }
}

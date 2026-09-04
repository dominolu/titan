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
    BAR_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, MARKET_EVENT_TYPES, MarketAdminApi,
    MarketAdminRequest, MarketAdminResponse, MarketSourceDefinition,
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
    StrategyDataMode, StrategyDefinition, StrategyOperationState, StrategyPluginConfig,
    StrategyPluginFactory, StrategyRecoveryPolicy,
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
        validate_core_live_strategy_profile(&config.strategies)?;
        validate_strategy_unit_consistency(
            &config.strategies,
            &config.market_sources,
            &config.accounts,
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

/// Freezes the still-unimplemented live Bar/Hybrid profiles before they can reach a RUNNING
/// strategy that silently waits for events no configured producer can emit.
///
/// The tick path is fully wired (venue -> EventEngine -> strategy lane). Bar delivery exists only
/// as an encoded adapter plus fake-connector tests; no live connector or aggregator publishes
/// `titan.market.BarBatch` yet, so any live strategy that subscribes bars or declares Bar/Hybrid
/// data mode is rejected at configuration time.
fn validate_core_live_strategy_profile(
    strategies: &[StrategyDefinition],
) -> Result<(), ConfigurationError> {
    for definition in strategies {
        if !definition.enabled {
            continue;
        }
        for binding in definition.markets.iter() {
            if binding.data_mode != StrategyDataMode::Tick {
                return Err(ConfigurationError::Invalid(format!(
                    "strategy {} declares non-tick data mode; live Bar/Hybrid is unavailable until a production BarBatch publisher is implemented",
                    definition.strategy_key
                )));
            }
        }
        for subscription in definition.subscriptions.iter() {
            if subscription.event_type.as_ref() == BAR_BATCH_EVENT {
                return Err(ConfigurationError::Invalid(format!(
                    "strategy {} subscribes to titan.market.BarBatch; no live BarBatch producer is currently configured",
                    definition.strategy_key
                )));
            }
        }
    }
    Ok(())
}

/// Rejects live configurations where the same asset id is priced in different tick/lot units by
/// the Market and Account definitions.
///
/// Strategies consume integer ticks/lots from the market batch and send integer ticks/lots through
/// the account execution path. If the two sides are configured with different units, the strategy
/// believes it is quoting one price/quantity while the account adapter converts to another. This
/// validation only checks pairs that a strategy actually binds; unrelated definitions are ignored.
fn validate_strategy_unit_consistency(
    strategies: &[StrategyDefinition],
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
) -> Result<(), ConfigurationError> {
    for definition in strategies {
        if !definition.enabled {
            continue;
        }
        let mut market_units = Vec::new();
        for binding in definition.markets.iter() {
            let Some(source) = market_sources
                .iter()
                .find(|source| source.source_key == binding.source_key)
            else {
                // Missing market sources are reported with a dedicated runtime error during
                // dependency resolution; this validation only checks the units that exist.
                continue;
            };
            let Some(instrument) = source
                .instruments
                .iter()
                .find(|instrument| instrument.asset_id.0 == binding.asset_id)
            else {
                return Err(ConfigurationError::Invalid(format!(
                    "strategy {} binds asset {} but market source {} has no matching instrument",
                    definition.strategy_key, binding.asset_id, binding.source_key
                )));
            };
            market_units.push((
                binding.asset_id,
                instrument.price_tick,
                instrument.quantity_lot,
            ));
        }
        for account_binding in definition.accounts.iter() {
            let Some(account) = accounts
                .iter()
                .find(|account| account.account_key == account_binding.account_key)
            else {
                continue;
            };
            for tradable in account_binding.tradable_assets.iter() {
                let Some(&(_, market_price, market_lot)) = market_units
                    .iter()
                    .find(|(asset_id, _, _)| *asset_id == tradable.asset_id)
                else {
                    continue;
                };
                let Some(instrument) = account
                    .instruments
                    .iter()
                    .find(|instrument| instrument.asset_id.0 == tradable.asset_id)
                else {
                    return Err(ConfigurationError::Invalid(format!(
                        "strategy {} trades asset {} on account {} but the account definition has no matching instrument",
                        definition.strategy_key, tradable.asset_id, account_binding.account_key
                    )));
                };
                if instrument.price_tick != market_price || instrument.quantity_lot != market_lot {
                    return Err(ConfigurationError::Invalid(format!(
                        "strategy {} asset {} unit mismatch: market price={market_price} lot={market_lot}, account price={} lot={}",
                        definition.strategy_key,
                        tradable.asset_id,
                        instrument.price_tick,
                        instrument.quantity_lot
                    )));
                }
            }
        }
    }
    Ok(())
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

    fn live_strategy(data_mode: StrategyDataMode, subscribe_bar_batch: bool) -> StrategyDefinition {
        let event_type: &str = if subscribe_bar_batch {
            BAR_BATCH_EVENT
        } else {
            titan_market_plugin::DEPTH_BATCH_EVENT
        };
        StrategyDefinition {
            strategy_key: Arc::from("freeze-test"),
            strategy_id: titan_strategy_plugin::StrategyId(99),
            package: titan_strategy_plugin::StrategyPackageRef {
                loader_type: Arc::from("numba-python"),
                uri: Arc::from("file:///unused"),
                expected_digest: [1; 32],
                signature_ref: None,
            },
            entrypoint: Arc::from("freeze.strategy:build"),
            parameters: Arc::from(b"{}".as_slice()),
            parameter_schema_version: 1,
            markets: Arc::from([titan_strategy_plugin::StrategyMarketBinding {
                local_market_no: 0,
                local_asset_no: 0,
                source_key: Arc::from("market"),
                asset_id: 1,
                data_mode,
            }]),
            accounts: Arc::from([]),
            subscriptions: Arc::from([titan_strategy_plugin::StrategySubscriptionSpec {
                event_type: Arc::from(event_type),
                schema_version: titan_market_plugin::MARKET_EVENT_SCHEMA_VERSION,
                routing_keys: Arc::from([1]),
                qos: EventQos::ReliableOrdered,
            }]),
            risk_scope: titan_strategy_plugin::RiskScopeRef(Arc::from("unused")),
            runtime: titan_strategy_plugin::StrategyRuntimeSpec::default(),
            recovery: titan_strategy_plugin::StrategyRecoveryPolicy::Fresh,
            shutdown: titan_strategy_plugin::StrategyShutdownPolicy::LeaveOwnedOrders,
            enabled: true,
            definition_version: 1,
        }
    }

    #[test]
    fn core_live_profile_freezes_bar_and_hybrid_until_producer_exists() {
        let bar_mode = live_strategy(
            titan_strategy_plugin::StrategyDataMode::Bar {
                timeframe_ns: 60_000_000_000,
            },
            false,
        );
        assert!(matches!(
            validate_core_live_strategy_profile(&[bar_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let hybrid_mode = live_strategy(
            titan_strategy_plugin::StrategyDataMode::Hybrid {
                signal_timeframe_ns: 60_000_000_000,
            },
            false,
        );
        assert!(matches!(
            validate_core_live_strategy_profile(&[hybrid_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let bar_subscription = live_strategy(titan_strategy_plugin::StrategyDataMode::Tick, true);
        assert!(matches!(
            validate_core_live_strategy_profile(&[bar_subscription]),
            Err(ConfigurationError::Invalid(_))
        ));

        let tick = live_strategy(titan_strategy_plugin::StrategyDataMode::Tick, false);
        assert!(validate_core_live_strategy_profile(&[tick]).is_ok());
    }

    #[test]
    fn unit_consistency_rejects_strategy_bound_account_mismatch() {
        let market_source = titan_market_plugin::MarketSourceDefinition {
            source_key: Arc::from("market"),
            connector_type: Arc::from("binance-futures"),
            connector_config: Arc::from([]),
            instruments: Arc::from([titan_market_plugin::MarketInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: titan_market_plugin::AssetId(1),
                price_tick: "0.0001".parse().unwrap(),
                quantity_lot: "0.001".parse().unwrap(),
            }]),
            enabled: true,
            definition_version: 1,
        };
        let account = titan_account_plugin::AccountDefinition {
            account_key: Arc::from("account"),
            account_id: titan_account_plugin::AccountId(7),
            connector_type: Arc::from("binance-futures-account"),
            credential_ref: titan_account_plugin::SecretRef::new("secret://test"),
            connector_config: Arc::from([]),
            instruments: Arc::from([titan_account_plugin::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: titan_account_plugin::AssetId(1),
                price_tick: "0.0001".parse().unwrap(),
                quantity_lot: "0.01".parse().unwrap(),
                contract_multiplier: "1".parse().unwrap(),
            }]),
            currencies: Arc::from([]),
            ownership: titan_account_plugin::OrderOwnershipPolicy::ManagedOnly {
                client_id_prefix: Arc::from("titan-"),
            },
            shutdown_order_policy: titan_account_plugin::ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        };
        let mut strategy = live_strategy(titan_strategy_plugin::StrategyDataMode::Tick, false);
        strategy.accounts = Arc::from([titan_strategy_plugin::StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([titan_strategy_plugin::StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
        }]);
        assert!(matches!(
            validate_strategy_unit_consistency(&[strategy], &[market_source], &[account]),
            Err(ConfigurationError::Invalid(_))
        ));
    }
}

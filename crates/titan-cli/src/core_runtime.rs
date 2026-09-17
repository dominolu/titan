use std::{
    collections::{BTreeSet, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use titan_account_service::{
    ACCOUNT_EVENT_SCHEMA_VERSION, ACCOUNT_EVENT_TYPES, AccountAdminService,
    AccountConnectorFactory, AccountCoreConfig, AccountDefinition,
    AccountService as AccountQueryService, AccountServiceCore, DirectorySecretProvider,
    ExecutionRuntime, FILL_EVENT, FILL_EVENT_SCHEMA_VERSION, SecretProvider,
    TracingExecutionObserver,
};
use titan_core_types::{ActivationGate, ComponentIdentity, CoreError, EventPublisher};
use titan_event_engine::{EventClass, EventEngine, EventEngineConfig, EventEngineHandle, PoolKind};
use titan_market_service::{
    BAR_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, MARKET_EVENT_TYPES, MarketAdminService,
    MarketConnectorFactory, MarketCoreConfig, MarketServiceCore, MarketSourceDefinition,
};
use titan_strategy_runtime::{
    NativeV13LoaderFactory, NativeV13RuntimeFactory, StrategyAdminService,
    StrategyCoreConfig, StrategyDataMode, StrategyDefinition, StrategyOperationState,
    StrategyPackageLoaderRegistry, StrategyRecoveryPolicy, StrategyRuntimeFactoryRegistry,
    StrategyServiceCore, StrategyServiceDependencies,
};

pub const APPLICATION_CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub event_engine: EventEngineConfig,
    #[serde(default)]
    pub market_service: MarketServiceConfig,
    #[serde(default)]
    pub account_service: AccountServiceConfig,
    #[serde(default)]
    pub strategy_service: StrategyServiceConfig,
    #[serde(default)]
    pub execution: ExecutionServiceConfig,
    #[serde(default)]
    pub market_sources: Vec<MarketSourceDefinition>,
    #[serde(default)]
    pub accounts: Vec<AccountDefinition>,
    #[serde(default)]
    pub strategies: Vec<StrategyDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketServiceConfig {
    #[serde(default = "default_market_sources")]
    pub max_sources: usize,
    #[serde(default = "default_market_instruments")]
    pub max_instruments: usize,
    #[serde(default = "default_shutdown_ms")]
    pub stop_timeout_ms: u64,
}

impl Default for MarketServiceConfig {
    fn default() -> Self {
        Self {
            max_sources: default_market_sources(),
            max_instruments: default_market_instruments(),
            stop_timeout_ms: default_shutdown_ms(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountServiceConfig {
    #[serde(default = "default_accounts")]
    pub max_accounts: usize,
    #[serde(default = "default_account_instruments")]
    pub max_instruments_per_account: usize,
    #[serde(default = "default_account_currencies")]
    pub max_currencies_per_account: usize,
    #[serde(default)]
    pub secret_root: Option<PathBuf>,
    #[serde(default = "default_shutdown_ms")]
    pub stop_timeout_ms: u64,
    #[serde(default = "default_account_startup_ms")]
    pub startup_timeout_ms: u64,
}

impl Default for AccountServiceConfig {
    fn default() -> Self {
        Self {
            max_accounts: default_accounts(),
            max_instruments_per_account: default_account_instruments(),
            max_currencies_per_account: default_account_currencies(),
            secret_root: None,
            stop_timeout_ms: default_shutdown_ms(),
            startup_timeout_ms: default_account_startup_ms(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyServiceConfig {
    #[serde(default)]
    pub allowed_artifact_roots: Vec<PathBuf>,
}

impl Default for StrategyServiceConfig {
    fn default() -> Self {
        Self {
            allowed_artifact_roots: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionServiceConfig {
    #[serde(default = "default_execution_threads")]
    pub worker_threads: usize,
    #[serde(default = "default_active_tasks")]
    pub max_active_tasks: usize,
    #[serde(default = "default_shutdown_ms")]
    pub shutdown_deadline_ms: u64,
}

impl Default for ExecutionServiceConfig {
    fn default() -> Self {
        Self {
            worker_threads: default_execution_threads(),
            max_active_tasks: default_active_tasks(),
            shutdown_deadline_ms: default_shutdown_ms(),
        }
    }
}

#[derive(Clone)]
pub struct AdaptedConfiguration {
    pub event_engine: EventEngineConfig,
    pub market_service: MarketServiceConfig,
    pub account_service: AccountServiceConfig,
    pub execution: ExecutionServiceConfig,
    pub market_sources: Vec<MarketSourceDefinition>,
    pub accounts: Vec<AccountDefinition>,
    pub strategies: Vec<StrategyDefinition>,
    account_secret_root: Option<PathBuf>,
    strategy_bootstrap: Option<StrategyBootstrap>,
}

#[derive(Clone)]
struct StrategyBootstrap {
    config: StrategyCoreConfig,
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
    #[error("cannot resolve account secret root {path}: {source}")]
    SecretRoot {
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

        if config.market_service.max_sources == 0
            || config.market_service.max_instruments == 0
            || config.account_service.max_accounts == 0
            || config.account_service.max_instruments_per_account == 0
            || config.account_service.max_currencies_per_account == 0
            || config.execution.worker_threads == 0
            || config.execution.max_active_tasks == 0
        {
            return Err(ConfigurationError::Invalid(
                "service capacities and execution worker counts must be non-zero".into(),
            ));
        }

        let account_secret_root = if let Some(root) = config.account_service.secret_root.take() {
            let root = if root.is_absolute() {
                root
            } else {
                config_directory.join(root)
            };
            let root =
                std::fs::canonicalize(&root).map_err(|source| ConfigurationError::SecretRoot {
                    path: root.clone(),
                    source,
                })?;
            if !root.is_dir() {
                return Err(ConfigurationError::Invalid(
                    "account_service.secret_root must be a directory".into(),
                ));
            }
            Some(root)
        } else {
            None
        };

        validate_runtime_definitions(&config.market_sources, &config.accounts)?;
        validate_account_source_capacity(&config.event_engine, &config.accounts)?;
        if !config.accounts.is_empty() && account_secret_root.is_none() {
            return Err(ConfigurationError::Invalid(
                "account definitions require account_service.secret_root".into(),
            ));
        }
        let strategy_bootstrap = adapt_strategies(
            &mut config.strategies,
            config_directory,
            config.strategy_service,
        )?;
        validate_core_live_strategy_profile(&config.strategies)?;
        validate_strategy_unit_consistency(
            &config.strategies,
            &config.market_sources,
            &config.accounts,
        )?;
        Ok(AdaptedConfiguration {
            event_engine: config.event_engine,
            market_service: config.market_service,
            account_service: config.account_service,
            execution: config.execution,
            market_sources: config.market_sources,
            accounts: config.accounts,
            strategies: config.strategies,
            account_secret_root,
            strategy_bootstrap,
        })
    }
}

#[derive(Debug, Error)]
pub enum ApplicationRuntimeError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    CoreContract(#[from] CoreError),
    #[error(transparent)]
    Core(#[from] titan_event_engine::CoreRuntimeError),
    #[error(transparent)]
    Event(#[from] titan_event_engine::EngineError),
    #[error("execution runtime failed: {0}")]
    Execution(String),
    #[error("market runtime definition failed: {0}")]
    Market(String),
    #[error("account runtime definition failed: {0}")]
    Account(String),
    #[error("strategy runtime definition failed: {0}")]
    Strategy(String),
    #[error("runtime shutdown failed: {0}")]
    Shutdown(String),
}

pub struct TradingRuntime {
    events: Arc<EventEngine>,
    event_handle: Arc<EventEngineHandle>,
    market: Arc<MarketServiceCore>,
    account: Arc<AccountServiceCore>,
    strategy: Option<Arc<StrategyServiceCore>>,
    execution: ExecutionRuntime,
    shutdown_deadline: std::time::Duration,
    account_startup_timeout: std::time::Duration,
}

/// Construction-only catalog of statically linked venue connectors.
pub struct ConnectorCatalog {
    market: Vec<Arc<dyn MarketConnectorFactory>>,
    account: Vec<Arc<dyn AccountConnectorFactory>>,
}

impl ConnectorCatalog {
    pub fn builtin() -> Self {
        Self {
            market: connector::market_runtime::venue_market_factories(),
            account: connector::account_runtime::venue_account_factories(),
        }
    }
}

impl TradingRuntime {
    pub fn start_from_toml(path: impl AsRef<Path>) -> Result<Self, ApplicationRuntimeError> {
        Self::start(ConfigurationAdapter::load_toml(path)?)
    }

    pub fn start(config: AdaptedConfiguration) -> Result<Self, ApplicationRuntimeError> {
        let AdaptedConfiguration {
            event_engine,
            market_service,
            account_service,
            execution: execution_config,
            market_sources,
            accounts,
            strategies,
            account_secret_root,
            strategy_bootstrap,
        } = config;
        let events = Arc::new(EventEngine::new(event_engine)?);
        let event_handle = Arc::new(events.handle());
        register_event_catalog(&event_handle)?;
        events.start()?;
        let catalog = ConnectorCatalog::builtin();

        let market = MarketServiceCore::new(MarketCoreConfig {
            max_sources: market_service.max_sources,
            max_instruments: market_service.max_instruments,
            stop_timeout: std::time::Duration::from_millis(market_service.stop_timeout_ms),
        });
        for factory in catalog.market {
            market
                .register_factory(factory)
                .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
        }
        market.activate(
            ComponentIdentity::new("titan.market", "market"),
            service_publisher(
                event_handle.clone(),
                "titan.market",
                MARKET_EVENT_TYPES
                    .iter()
                    .map(|event| (*event, MARKET_EVENT_SCHEMA_VERSION)),
            ),
        );

        let secret_provider: Arc<dyn SecretProvider> = if let Some(root) = account_secret_root {
            Arc::new(
                DirectorySecretProvider::new(root)
                    .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?,
            )
        } else {
            Arc::new(titan_account_service::UnavailableSecretProvider)
        };
        let account = AccountServiceCore::with_secret_provider(
            AccountCoreConfig {
                max_accounts: account_service.max_accounts,
                max_instruments_per_account: account_service.max_instruments_per_account,
                max_currencies_per_account: account_service.max_currencies_per_account,
                stop_timeout: std::time::Duration::from_millis(account_service.stop_timeout_ms),
            },
            secret_provider,
        );
        for factory in catalog.account {
            account
                .register_factory(factory)
                .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
        }
        let mut account_events = ACCOUNT_EVENT_TYPES
            .iter()
            .map(|event| {
                (
                    *event,
                    if *event == FILL_EVENT {
                        FILL_EVENT_SCHEMA_VERSION
                    } else {
                        ACCOUNT_EVENT_SCHEMA_VERSION
                    },
                )
            })
            .collect::<Vec<_>>();
        account_events.push((FILL_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION));
        account.activate(
            ComponentIdentity::new("titan.account", "account"),
            service_publisher(event_handle.clone(), "titan.account", account_events),
        );

        let execution = ExecutionRuntime::new(
            execution_config.worker_threads,
            execution_config.max_active_tasks,
        )
        .map_err(|error| ApplicationRuntimeError::Execution(error.to_string()))?;
        let shutdown_deadline =
            std::time::Duration::from_millis(execution_config.shutdown_deadline_ms);
        let account_startup_timeout =
            std::time::Duration::from_millis(account_service.startup_timeout_ms);
        let strategy = if let Some(bootstrap) = strategy_bootstrap {
            let loaders = Arc::new(StrategyPackageLoaderRegistry::default());
            loaders
                .register(Arc::new(NativeV13LoaderFactory))
                .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
            runtimes
                .register(Arc::new(NativeV13RuntimeFactory))
                .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            let account_query: Arc<dyn AccountQueryService> = account.clone();
            Some(Arc::new(
                StrategyServiceCore::new(
                    bootstrap.config,
                    StrategyServiceDependencies {
                        events: event_handle.as_ref().clone(),
                        markets: market.clone(),
                        accounts: account_query,
                        execution_dispatcher: Some(execution.dispatcher()),
                        execution_observer: Some(Arc::new(TracingExecutionObserver)),
                    },
                    loaders,
                    runtimes,
                )
                .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?,
            ))
        } else {
            None
        };

        let mut runtime = Self {
            events,
            event_handle,
            market,
            account,
            strategy,
            execution,
            shutdown_deadline,
            account_startup_timeout,
        };
        if let Err(error) = runtime.apply_definitions(market_sources, accounts, strategies) {
            let _ = runtime.shutdown();
            return Err(error);
        }
        Ok(runtime)
    }

    pub fn events(&self) -> &Arc<EventEngine> {
        &self.events
    }

    pub fn event_handle(&self) -> &Arc<EventEngineHandle> {
        &self.event_handle
    }

    pub fn strategy_service(&self) -> Option<&Arc<StrategyServiceCore>> {
        self.strategy.as_ref()
    }

    pub fn shutdown(&mut self) -> Result<(), ApplicationRuntimeError> {
        let deadline = std::time::Instant::now() + self.shutdown_deadline;
        let mut failures = Vec::new();
        if let Some(strategy) = self.strategy.as_ref()
            && let Err(error) = strategy.quiesce(deadline)
        {
            failures.push(format!("strategy quiesce: {error}"));
        }
        let execution_metrics = self.execution.metrics();
        tracing::info!(
            active_tasks = execution_metrics.active_tasks,
            spawned_tasks = execution_metrics.spawned_tasks,
            saturated_rejections = execution_metrics.saturated_rejections,
            stopping_rejections = execution_metrics.stopping_rejections,
            connector_future_allocations = execution_metrics.connector_future_allocations,
            spawn_p50_ns = execution_metrics.spawn_p50_ns,
            spawn_p99_ns = execution_metrics.spawn_p99_ns,
            spawn_p999_ns = execution_metrics.spawn_p999_ns,
            spawn_max_ns = execution_metrics.spawn_max_ns,
            "direct execution runtime summary"
        );
        self.execution.shutdown(deadline);
        if let Err(error) = self.account.quiesce_all(deadline) {
            failures.push(format!("account quiesce: {error}"));
        }
        if let Err(error) = self.account.shutdown() {
            failures.push(format!("account shutdown: {error}"));
        }
        if let Err(error) = self.market.quiesce_all(deadline) {
            failures.push(format!("market quiesce: {error}"));
        }
        if let Err(error) = self.market.shutdown() {
            failures.push(format!("market shutdown: {error}"));
        }
        if let Err(error) = self.events.stop() {
            failures.push(format!("event engine stop: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(ApplicationRuntimeError::Shutdown(failures.join("; ")))
        }
    }

    fn apply_definitions(
        &mut self,
        market_sources: Vec<MarketSourceDefinition>,
        accounts: Vec<AccountDefinition>,
        strategies: Vec<StrategyDefinition>,
    ) -> Result<(), ApplicationRuntimeError> {
        if !market_sources.is_empty() {
            for definition in market_sources {
                let enabled = definition.enabled;
                let handle = self
                    .market
                    .create(definition)
                    .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                if enabled {
                    self.market
                        .start(handle)
                        .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                }
            }
        }

        if !accounts.is_empty() {
            for definition in accounts {
                let enabled = definition.enabled;
                let handle = self
                    .account
                    .create(definition)
                    .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
                if enabled {
                    self.account
                        .start(handle)
                        .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
                    wait_account_ready(
                        self.account.as_ref(),
                        handle,
                        self.account_startup_timeout,
                    )?;
                }
            }
        }
        if !strategies.is_empty() {
            let strategy = self.strategy.as_ref().ok_or_else(|| {
                ApplicationRuntimeError::Strategy("strategy service is unavailable".into())
            })?;
            for definition in strategies {
                let enabled = definition.enabled;
                let startup_timeout = definition.runtime.startup_timeout;
                let handle = strategy
                    .create(definition)
                    .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                if enabled {
                    let prepare = strategy
                        .prepare(handle)
                        .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                    wait_strategy_operation(strategy.as_ref(), prepare, startup_timeout)?;
                    let start = strategy
                        .start(handle)
                        .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
                    wait_strategy_operation(strategy.as_ref(), start, startup_timeout)?;
                }
            }
        }
        Ok(())
    }
}

fn wait_strategy_operation(
    strategy: &dyn StrategyAdminService,
    operation: titan_strategy_runtime::StrategyOperationId,
    timeout: std::time::Duration,
) -> Result<(), ApplicationRuntimeError> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = strategy.operation(operation);
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

fn wait_account_ready(
    account: &dyn AccountQueryService,
    handle: titan_account_service::AccountHandle,
    timeout: std::time::Duration,
) -> Result<(), ApplicationRuntimeError> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let health = account
            .health(handle)
            .map_err(|error| ApplicationRuntimeError::Account(error.to_string()))?;
        match health.state {
            titan_account_service::AccountLifecycle::Ready => return Ok(()),
            titan_account_service::AccountLifecycle::Failed
            | titan_account_service::AccountLifecycle::Invalidated
            | titan_account_service::AccountLifecycle::Stopped => {
                return Err(ApplicationRuntimeError::Account(health.message.to_string()));
            }
            _ if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            _ => {
                return Err(ApplicationRuntimeError::Account(
                    "account bootstrap timed out".into(),
                ));
            }
        }
    }
}

fn register_event_catalog(events: &EventEngineHandle) -> Result<(), CoreError> {
    for event_type in MARKET_EVENT_TYPES {
        events
            .register_event(
                event_type,
                MARKET_EVENT_SCHEMA_VERSION,
                EventClass::Market,
                PoolKind::MarketBatch,
            )
            .map_err(event_error)?;
    }
    for event_type in ACCOUNT_EVENT_TYPES {
        events
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
    Ok(())
}

fn service_publisher(
    events: Arc<EventEngineHandle>,
    owner: &'static str,
    grants: impl IntoIterator<Item = (&'static str, u32)>,
) -> EventPublisher {
    let mut allowed = std::collections::BTreeMap::<Arc<str>, BTreeSet<u32>>::new();
    for (event_type, schema_version) in grants {
        allowed
            .entry(Arc::from(event_type))
            .or_default()
            .insert(schema_version);
    }
    let gate = Arc::new(ActivationGate::new());
    assert!(
        gate.activate(),
        "new service publication gate activates once"
    );
    EventPublisher::new(
        ComponentIdentity::new(owner, "publisher"),
        allowed,
        gate,
        events,
    )
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

fn validate_account_source_capacity(
    event_engine: &EventEngineConfig,
    accounts: &[AccountDefinition],
) -> Result<(), ConfigurationError> {
    let required_sources = accounts
        .iter()
        .map(|account| {
            u64::from(account.account_id.0)
                .saturating_mul(2)
                .saturating_add(2)
        })
        .max()
        .unwrap_or(0);
    if required_sources > event_engine.ingress.max_sources as u64 {
        return Err(ConfigurationError::Invalid(format!(
            "event_engine.ingress.max_sources={} is too small for account source streams; at least {required_sources} is required",
            event_engine.ingress.max_sources
        )));
    }
    Ok(())
}

fn adapt_strategies(
    strategies: &mut [StrategyDefinition],
    config_directory: &Path,
    bootstrap: StrategyServiceConfig,
) -> Result<Option<StrategyBootstrap>, ConfigurationError> {
    if strategies.is_empty() {
        return Ok(None);
    }
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
            "strategy_service requires at least one allowed_artifact_roots entry".into(),
        ));
    }
    for definition in strategies {
        if definition.recovery != StrategyRecoveryPolicy::Fresh {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} must use recovery = fresh",
                definition.strategy_key
            )));
        }
        if definition.package.loader_type.as_ref() != "native-v13" {
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
        if !package_root.is_file() {
            return Err(ConfigurationError::Invalid(format!(
                "strategy artifact {} is not a file",
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
        definition.package.uri = Arc::from(format!("file://{}", package_root.display()));
    }
    let mut config = StrategyCoreConfig::default();
    config.allowed_artifact_roots = allowed_roots
        .iter()
        .map(|root| Arc::from(root.to_string_lossy().as_ref()))
        .collect::<Vec<_>>()
        .into();
    Ok(Some(StrategyBootstrap {
        config,
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

fn event_error(error: titan_event_engine::EngineError) -> CoreError {
    CoreError::new(
        titan_core_types::ErrorKind::ComponentFailed,
        titan_core_types::ComponentIdentity::new("titan.core", "event-catalog"),
        titan_core_types::ComponentState::Discovered,
        "register_event_catalog",
        error.to_string(),
    )
}

const fn default_market_sources() -> usize {
    16
}
const fn default_market_instruments() -> usize {
    4_096
}
const fn default_accounts() -> usize {
    32
}
const fn default_account_instruments() -> usize {
    4_096
}
const fn default_account_currencies() -> usize {
    512
}
const fn default_execution_threads() -> usize {
    2
}
const fn default_active_tasks() -> usize {
    8_192
}
const fn default_shutdown_ms() -> u64 {
    5_000
}
const fn default_account_startup_ms() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use titan_core_types::EventQos;

    fn application() -> ApplicationConfig {
        ApplicationConfig {
            schema_version: APPLICATION_CONFIG_SCHEMA_VERSION,
            event_engine: EventEngineConfig::default(),
            market_service: MarketServiceConfig::default(),
            account_service: AccountServiceConfig::default(),
            strategy_service: StrategyServiceConfig::default(),
            execution: ExecutionServiceConfig::default(),
            market_sources: vec![],
            accounts: vec![],
            strategies: vec![],
        }
    }

    #[test]
    fn adapter_accepts_static_services_and_rejects_zero_capacity() {
        let config = application();
        let adapted = ConfigurationAdapter::adapt(config, Path::new(".")).unwrap();
        assert_eq!(adapted.execution.worker_threads, 2);
        assert_eq!(adapted.market_service.max_sources, 16);

        let mut config = application();
        config.execution.max_active_tasks = 0;
        assert!(matches!(
            ConfigurationAdapter::adapt(config, Path::new(".")),
            Err(ConfigurationError::Invalid(_))
        ));
    }

    #[test]
    fn account_secret_root_is_resolved_relative_to_the_runtime_config() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("titan-secret-root-{}-{nonce}", std::process::id()));
        let secrets = directory.join("secrets");
        std::fs::create_dir_all(&secrets).unwrap();
        let mut config = application();
        config.account_service.secret_root = Some(PathBuf::from("secrets"));
        let adapted = ConfigurationAdapter::adapt(config, &directory).unwrap();
        assert_eq!(
            adapted.account_secret_root,
            Some(std::fs::canonicalize(&secrets).unwrap())
        );
        std::fs::remove_dir(secrets).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn pair_arb_v13_runtime_template_deserializes_human_readable_byte_fields() {
        let config: ApplicationConfig = toml::from_str(include_str!(
            "../../../deploy/pair_arb_v13/runtime.toml"
        ))
        .unwrap();
        assert_eq!(config.market_sources.len(), 2);
        assert!(
            std::str::from_utf8(&config.market_sources[0].connector_config)
                .unwrap()
                .contains("simulated = false")
        );
        assert!(
            std::str::from_utf8(&config.market_sources[1].connector_config)
                .unwrap()
                .contains("is_mainnet = true")
        );
        assert_eq!(config.accounts.len(), 2);
        assert_eq!(config.event_engine.ingress.max_sources, 8_192);
        validate_account_source_capacity(&config.event_engine, &config.accounts).unwrap();
        assert_eq!(config.strategies.len(), 1);
        assert_eq!(config.strategies[0].package.loader_type.as_ref(), "native-v13");
        assert_eq!(config.strategies[0].parameters.as_ref(), b"{}");
        let deployment = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/pair_arb_v13");
        ConfigurationAdapter::adapt(config, &deployment).unwrap();
    }

    #[test]
    fn legacy_plugin_configuration_is_rejected() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "titan-configuration-adapter-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
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

        assert!(matches!(
            ConfigurationAdapter::load_toml(&config_path),
            Err(ConfigurationError::Toml { .. })
        ));

        std::fs::remove_file(config_path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn configured_runtime_starts_static_core_services() {
        let config = application();
        let adapted = ConfigurationAdapter::adapt(config, Path::new(".")).unwrap();
        let mut runtime = TradingRuntime::start(adapted).unwrap();
        assert_eq!(runtime.events().arena().outstanding_blocks(), 0);
        assert!(runtime.strategy_service().is_none());
        runtime.shutdown().unwrap();
        assert_eq!(runtime.events().arena().outstanding_blocks(), 0);
    }

    fn live_strategy(data_mode: StrategyDataMode, subscribe_bar_batch: bool) -> StrategyDefinition {
        let event_type: &str = if subscribe_bar_batch {
            BAR_BATCH_EVENT
        } else {
            titan_market_service::DEPTH_BATCH_EVENT
        };
        StrategyDefinition {
            strategy_key: Arc::from("freeze-test"),
            strategy_id: titan_strategy_runtime::StrategyId(99),
            package: titan_strategy_runtime::StrategyPackageRef {
                loader_type: Arc::from("native-v13"),
                uri: Arc::from("file:///unused"),
                expected_digest: [1; 32],
                signature_ref: None,
            },
            entrypoint: Arc::from("freeze.strategy:build"),
            parameters: Arc::from(b"{}".as_slice()),
            parameter_schema_version: 1,
            markets: Arc::from([titan_strategy_runtime::StrategyMarketBinding {
                local_market_no: 0,
                local_asset_no: 0,
                source_key: Arc::from("market"),
                asset_id: 1,
                data_mode,
            }]),
            accounts: Arc::from([]),
            subscriptions: Arc::from([titan_strategy_runtime::StrategySubscriptionSpec {
                event_type: Arc::from(event_type),
                schema_version: titan_market_service::MARKET_EVENT_SCHEMA_VERSION,
                routing_keys: Arc::from([1]),
                qos: EventQos::ReliableOrdered,
            }]),
            risk_scope: titan_strategy_runtime::RiskScopeRef(Arc::from("unused")),
            runtime: titan_strategy_runtime::StrategyRuntimeSpec::default(),
            recovery: titan_strategy_runtime::StrategyRecoveryPolicy::Fresh,
            enabled: true,
            definition_version: 1,
        }
    }

    #[test]
    fn core_live_profile_freezes_bar_and_hybrid_until_producer_exists() {
        let bar_mode = live_strategy(
            titan_strategy_runtime::StrategyDataMode::Bar {
                timeframe_ns: 60_000_000_000,
            },
            false,
        );
        assert!(matches!(
            validate_core_live_strategy_profile(&[bar_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let hybrid_mode = live_strategy(
            titan_strategy_runtime::StrategyDataMode::Hybrid {
                signal_timeframe_ns: 60_000_000_000,
            },
            false,
        );
        assert!(matches!(
            validate_core_live_strategy_profile(&[hybrid_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let bar_subscription = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick, true);
        assert!(matches!(
            validate_core_live_strategy_profile(&[bar_subscription]),
            Err(ConfigurationError::Invalid(_))
        ));

        let tick = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick, false);
        assert!(validate_core_live_strategy_profile(&[tick]).is_ok());

        let mut account_strategy =
            live_strategy(titan_strategy_runtime::StrategyDataMode::Tick, false);
        account_strategy.accounts = Arc::from([titan_strategy_runtime::StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([titan_strategy_runtime::StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
        }]);
        assert!(validate_core_live_strategy_profile(&[account_strategy]).is_ok());
    }

    #[test]
    fn unit_consistency_rejects_strategy_bound_account_mismatch() {
        let market_source = titan_market_service::MarketSourceDefinition {
            source_key: Arc::from("market"),
            connector_type: Arc::from("binance-futures"),
            connector_config: Arc::from([]),
            instruments: Arc::from([titan_market_service::MarketInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: titan_market_service::AssetId(1),
                price_tick: "0.0001".parse().unwrap(),
                quantity_lot: "0.001".parse().unwrap(),
            }]),
            enabled: true,
            definition_version: 1,
        };
        let account = titan_account_service::AccountDefinition {
            account_key: Arc::from("account"),
            account_id: titan_account_service::AccountId(7),
            connector_type: Arc::from("binance-futures-account"),
            credential_ref: titan_account_service::SecretRef::new("secret://test"),
            connector_config: Arc::from([]),
            instruments: Arc::from([titan_account_service::AccountInstrumentBinding {
                native_symbol: Arc::from("BTCUSDT"),
                asset_id: titan_account_service::AssetId(1),
                price_tick: "0.0001".parse().unwrap(),
                quantity_lot: "0.01".parse().unwrap(),
                contract_multiplier: "1".parse().unwrap(),
            }]),
            currencies: Arc::from([]),
            ownership: titan_account_service::OrderOwnershipPolicy::ManagedOnly {
                client_id_prefix: Arc::from("titan-"),
            },
            shutdown_order_policy: titan_account_service::ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        };
        let mut strategy = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick, false);
        strategy.accounts = Arc::from([titan_strategy_runtime::StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([titan_strategy_runtime::StrategyTradableAsset {
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

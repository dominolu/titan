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
    MARKET_EVENT_SCHEMA_VERSION, MARKET_EVENT_TYPES, MarketAdminService, MarketConnectorFactory,
    MarketCoreConfig, MarketServiceCore, MarketSourceDefinition,
};
use titan_strategy_runtime::{
    NativeLoaderFactory, NativeRuntimeFactory, StrategyAdminService, StrategyCoreConfig,
    StrategyDataMode, StrategyDefinition, StrategyLoaderRegistry, StrategyOperationState,
    StrategyRecoveryPolicy, StrategyRuntimeFactoryRegistry, StrategyServiceCore,
    StrategyServiceDependencies,
};

pub const APPLICATION_CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    #[serde(default)]
    pub managed_by_console: bool,
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
    #[serde(default)]
    pub history_path: Option<PathBuf>,
    #[serde(default = "live_fact_environment")]
    pub recording_environment: titan_persistence::FactEnvironment,
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
            history_path: None,
            recording_environment: titan_persistence::FactEnvironment::Live,
            max_accounts: default_accounts(),
            max_instruments_per_account: default_account_instruments(),
            max_currencies_per_account: default_account_currencies(),
            secret_root: None,
            stop_timeout_ms: default_shutdown_ms(),
            startup_timeout_ms: default_account_startup_ms(),
        }
    }
}
fn live_fact_environment() -> titan_persistence::FactEnvironment {
    titan_persistence::FactEnvironment::Live
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct StrategyServiceConfig {
    #[serde(default, alias = "allowed_artifact_roots")]
    pub allowed_strategy_roots: Vec<PathBuf>,
    #[serde(default)]
    pub checkpoint_root: Option<PathBuf>,
    #[serde(default)]
    pub recovery_storage: Option<titan_strategy_runtime::SqliteRecoveryConfig>,
    /// Startup-fetched metadata snapshot used to build V14 object tables. Relative paths resolve
    /// against the runtime config directory. The snapshot digest is verified before use.
    #[serde(default)]
    pub metadata_snapshot: Option<PathBuf>,
    /// Fetch instrument metadata from the venue's public endpoint at startup instead of reading a
    /// file. Only venues with an implemented transport are accepted; the current set is
    /// `hyperliquid` (public `/info`, no credentials). Mutually exclusive with `metadata_snapshot`.
    #[serde(default)]
    pub metadata_source: Option<MetadataSourceConfig>,
}

/// Where the startup metadata snapshot comes from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MetadataSourceConfig {
    /// Gate.io USDT perpetuals. Publishes tick, size step, contract multiplier and minimum size,
    /// so its symbols are the ones that can currently enter multi-leg admission.
    Gate,
    /// OKX linear swaps. Publishes tick, contract value and minimum size.
    Okx,
    /// Binance USDⓈ-M perpetuals. Publishes tick, step size, minimum quantity and notional.
    Binance,
    /// Hyperliquid mainnet public `/info`. Publishes size steps and multiplier but no price tick,
    /// so its instruments are refused at admission rather than run on a guessed tick.
    Hyperliquid,
    /// Hyperliquid testnet public `/info`.
    HyperliquidTestnet,
}

impl MetadataSourceConfig {
    fn transport(self) -> Arc<dyn titan_strategy_runtime::MetadataTransport> {
        match self {
            Self::Gate => Arc::new(crate::metadata_transport::GateFuturesMetadataTransport),
            Self::Okx => Arc::new(crate::metadata_transport::OkxSwapMetadataTransport),
            Self::Binance => Arc::new(crate::metadata_transport::BinanceFuturesMetadataTransport),
            Self::Hyperliquid => {
                Arc::new(crate::metadata_transport::HyperliquidMetadataTransport::mainnet())
            }
            Self::HyperliquidTestnet => {
                Arc::new(crate::metadata_transport::HyperliquidMetadataTransport::testnet())
            }
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
    /// Resolved snapshot path retained so the service can re-read and attribute failures to the
    /// strategy that needed it. The file is validated once here and again on every read.
    metadata_snapshot_path: Option<PathBuf>,
    /// Venue whose public endpoint supplies the snapshot, when configured instead of a file.
    metadata_source: Option<MetadataSourceConfig>,
    /// Symbols the venue must answer for: every enabled market source binding.
    metadata_requests: Vec<titan_strategy_runtime::MetadataRequest>,
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
        Self::load_contents(&contents, path)
    }

    pub fn load_contents(
        contents: &str,
        path: &Path,
    ) -> Result<AdaptedConfiguration, ConfigurationError> {
        let config = toml::from_str::<ApplicationConfig>(contents).map_err(|source| {
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
        if config.managed_by_console {
            return Err(ConfigurationError::Invalid(
                "managed configuration requires the authenticated console entry point".into(),
            ));
        }
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
        if let Some(path) = config.account_service.history_path.as_mut()
            && !path.is_absolute()
        {
            *path = config_directory.join(&*path);
        }
        if matches!(
            config.account_service.recording_environment,
            titan_persistence::FactEnvironment::Backtest
        ) {
            return Err(ConfigurationError::Invalid(
                "account recording environment must be live or simulation".into(),
            ));
        }
        validate_account_source_capacity(&config.event_engine, &config.accounts)?;
        validate_strategy_run_capacity(&config.event_engine, &config.strategies)?;
        if !config.accounts.is_empty() && account_secret_root.is_none() {
            return Err(ConfigurationError::Invalid(
                "account definitions require account_service.secret_root".into(),
            ));
        }
        if config.strategy_service.metadata_snapshot.is_some()
            && config.strategy_service.metadata_source.is_some()
        {
            return Err(ConfigurationError::Invalid(
                "strategy_service.metadata_snapshot and metadata_source cannot both be configured"
                    .into(),
            ));
        }
        let strategy_bootstrap = adapt_strategies(
            &mut config.strategies,
            config_directory,
            config.strategy_service,
            &config.market_sources,
        )?;
        validate_core_live_strategy_profile(&config.strategies)?;
        validate_instrument_definitions(
            &config.market_sources,
            &config.accounts,
            &config.strategies,
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
    observation: Option<Arc<titan_account_service::AccountFactJournal>>,
    observation_routes: Vec<titan_event_engine::FastLaneToken>,
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
        Self::start_inner(config, None, false)
    }
    pub fn start_observed(
        config: AdaptedConfiguration,
        path: PathBuf,
        source: String,
    ) -> Result<Self, ApplicationRuntimeError> {
        Self::start_inner(config, Some((path, source)), false)
    }
    pub(crate) fn start_managed(
        config: AdaptedConfiguration,
        path: PathBuf,
        source: String,
    ) -> Result<Self, ApplicationRuntimeError> {
        Self::start_inner(config, Some((path, source)), true)
    }
    fn start_inner(
        config: AdaptedConfiguration,
        observed: Option<(PathBuf, String)>,
        defer_strategies: bool,
    ) -> Result<Self, ApplicationRuntimeError> {
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
        let observed = observed.or_else(|| {
            if accounts.is_empty() {
                return None;
            }
            let path = account_service
                .history_path
                .clone()
                .or_else(|| {
                    strategy_bootstrap.as_ref().and_then(|b| {
                        b.config
                            .recovery_storage
                            .as_ref()
                            .map(|s| s.path.with_file_name("history.db"))
                            .or_else(|| {
                                b.config
                                    .checkpoint_root
                                    .as_ref()
                                    .map(|p| p.join("history.db"))
                            })
                    })
                })
                .unwrap_or_else(|| PathBuf::from("history.db"));
            Some((
                path,
                format!(
                    "runtime-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ),
            ))
        });
        if let Some((history_path, _)) = &observed {
            let recovery_path = strategy_bootstrap.as_ref().and_then(|bootstrap| {
                bootstrap
                    .config
                    .recovery_storage
                    .as_ref()
                    .map(|s| s.path.clone())
                    .or_else(|| {
                        bootstrap
                            .config
                            .checkpoint_root
                            .as_ref()
                            .map(|p| p.join("runtime.db"))
                    })
            });
            if let Some(path) = recovery_path {
                let normalize = |p: &Path| -> std::io::Result<PathBuf> {
                    if p.exists() {
                        std::fs::canonicalize(p)
                    } else {
                        let parent = p.parent().unwrap_or(Path::new("."));
                        std::fs::create_dir_all(parent)?;
                        Ok(std::fs::canonicalize(parent)?.join(p.file_name().unwrap_or_default()))
                    }
                };
                let recovery = normalize(&path)
                    .map_err(|e| ApplicationRuntimeError::Account(e.to_string()))?;
                let history = normalize(history_path)
                    .map_err(|e| ApplicationRuntimeError::Account(e.to_string()))?;
                if recovery == history {
                    return Err(ApplicationRuntimeError::Account(
                        "recovery and observation database paths must differ".into(),
                    ));
                }
            }
        }
        let events = Arc::new(EventEngine::new(event_engine)?);
        let event_handle = Arc::new(events.handle());
        register_event_catalog(&event_handle)?;
        events.start()?;
        let observation = observed
            .map(|(path, source)| {
                titan_account_service::AccountFactJournal::open_with_environment(
                    path,
                    source,
                    &accounts,
                    account_service.recording_environment.clone(),
                )
                .map_err(|e| ApplicationRuntimeError::Account(e.to_string()))
            })
            .transpose()?;
        let mut observation_routes = Vec::new();
        if let Some(journal) = &observation {
            observation_routes.push(event_handle.register_fast_lane(
                titan_market_service::BBO_EVENT,
                MARKET_EVENT_SCHEMA_VERSION,
                Vec::new(),
                journal.clone(),
            )?);
            for event in ACCOUNT_EVENT_TYPES {
                observation_routes.push(event_handle.register_fast_lane(
                    event,
                    if event == FILL_EVENT {
                        FILL_EVENT_SCHEMA_VERSION
                    } else {
                        ACCOUNT_EVENT_SCHEMA_VERSION
                    },
                    Vec::new(),
                    journal.clone(),
                )?);
            }
            observation_routes.push(event_handle.register_fast_lane(
                FILL_EVENT,
                ACCOUNT_EVENT_SCHEMA_VERSION,
                Vec::new(),
                journal.clone(),
            )?);
        }
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
        if let Some(journal) = &observation {
            account.set_recording_guard(journal.recording_guard());
        }
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
            let loaders = Arc::new(StrategyLoaderRegistry::default());
            loaders
                .register(Arc::new(NativeLoaderFactory))
                .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
            runtimes
                .register(Arc::new(NativeRuntimeFactory))
                .map_err(|error| ApplicationRuntimeError::Strategy(error.to_string()))?;
            let account_query: Arc<dyn AccountQueryService> = account.clone();
            // Built before `bootstrap.config` is moved into the service.
            let provider = metadata_provider(&bootstrap);
            Some(Arc::new(
                StrategyServiceCore::new(
                    bootstrap.config,
                    StrategyServiceDependencies {
                        events: event_handle.as_ref().clone(),
                        markets: market.clone(),
                        accounts: account_query,
                        execution_dispatcher: Some(execution.dispatcher()),
                        execution_observer: Some(Arc::new(TracingExecutionObserver)),
                        metadata_snapshot: None,
                        metadata_provider: provider,
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
            observation,
            observation_routes,
        };
        if let Err(error) =
            runtime.apply_definitions(market_sources, accounts, strategies, defer_strategies)
        {
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
    pub fn account_service(&self) -> &Arc<AccountServiceCore> {
        &self.account
    }
    pub fn observation(&self) -> Option<&Arc<titan_account_service::AccountFactJournal>> {
        self.observation.as_ref()
    }

    pub fn check_health(&self) -> Result<(), ApplicationRuntimeError> {
        self.check_dependency_health()?;
        if let Some(service) = &self.strategy {
            for entry in service.list().iter().filter(|entry| entry.enabled) {
                if entry.lifecycle != titan_strategy_runtime::StrategyLifecycle::Running {
                    return Err(ApplicationRuntimeError::Shutdown(format!(
                        "strategy {}: {:?}",
                        entry.strategy_key, entry.lifecycle
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn check_dependency_health(&self) -> Result<(), ApplicationRuntimeError> {
        if self
            .observation
            .as_ref()
            .is_some_and(|j| !j.recording_healthy())
        {
            return Err(ApplicationRuntimeError::Shutdown(
                "critical account recording unavailable".into(),
            ));
        }
        use titan_market_service::MarketService;
        let events = self.event_handle.runtime_health();
        if events.event_loop_failed
            || events.market_stream_invalid
            || events.critical_ingress_backpressure
            || events.arena_pressure_mask != 0
        {
            return Err(ApplicationRuntimeError::Shutdown(format!(
                "event engine unhealthy: {events:?}"
            )));
        }
        for entry in self.market.list().iter().filter(|entry| entry.enabled) {
            let health = self
                .market
                .health(entry.handle)
                .map_err(|error| ApplicationRuntimeError::Shutdown(error.to_string()))?;
            if health.state != titan_market_service::ConnectorHealth::Running {
                return Err(ApplicationRuntimeError::Shutdown(format!(
                    "market {}: {:?}: {}",
                    entry.source_key, health.state, health.message
                )));
            }
        }
        for entry in self.account.list().iter().filter(|entry| entry.enabled) {
            let health = self
                .account
                .health(entry.handle)
                .map_err(|error| ApplicationRuntimeError::Shutdown(error.to_string()))?;
            if health.state != titan_account_service::AccountLifecycle::Ready {
                return Err(ApplicationRuntimeError::Shutdown(format!(
                    "account {}: {:?}: {}",
                    entry.account_key, health.state, health.message
                )));
            }
        }
        Ok(())
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
        if let Err(active) = self.execution.shutdown(deadline) {
            failures.push(format!(
                "execution shutdown timed out with {active} active requests"
            ));
        }
        if let Err(error) = self.account.quiesce_all(deadline) {
            failures.push(format!("account quiesce: {error}"));
        }
        if let Err(error) = self.account.shutdown() {
            failures.push(format!("account shutdown: {error}"));
        }
        for token in self.observation_routes.drain(..) {
            self.event_handle.unregister_fast_lane(token);
        }
        if let Some(journal) = &self.observation
            && let Err(error) = journal.close(deadline)
        {
            failures.push(format!("account recorder: {error}"));
        }
        if let Err(error) = self.market.quiesce_all(deadline) {
            failures.push(format!("market quiesce: {error}"));
        }
        if let Err(error) = self.market.shutdown() {
            failures.push(format!("market shutdown: {error}"));
        }
        if let Err(error) = self.events.stop_before(deadline) {
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
        defer_strategies: bool,
    ) -> Result<(), ApplicationRuntimeError> {
        let observe_prices = self.observation.is_some() && strategies.is_empty();
        if !market_sources.is_empty() {
            for definition in market_sources {
                let enabled = definition.enabled;
                let instruments = definition.instruments.clone();
                let handle = self
                    .market
                    .create(definition)
                    .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                if let Some(journal) = &self.observation {
                    use titan_market_service::MarketService;
                    journal
                        .bind_risk_market(
                            self.market
                                .source_stream(handle)
                                .map_err(|e| ApplicationRuntimeError::Market(e.to_string()))?
                                .0,
                            &instruments,
                        )
                        .map_err(|e| ApplicationRuntimeError::Account(e.to_string()))?;
                }
                if enabled {
                    self.market
                        .start(handle)
                        .map_err(|error| ApplicationRuntimeError::Market(error.to_string()))?;
                    if observe_prices {
                        use titan_market_service::MarketService;
                        for instrument in &*instruments {
                            self.market
                                .subscribe(
                                    handle,
                                    titan_market_service::MarketSubscribeRequest {
                                        asset_id: instrument.asset_id,
                                        kinds: Arc::from([
                                            titan_market_service::MarketDataKind::Bbo,
                                        ]),
                                    },
                                )
                                .map_err(|e| ApplicationRuntimeError::Market(e.to_string()))?;
                        }
                    }
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
                if enabled && !defer_strategies {
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

pub(crate) fn wait_strategy_operation(
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
    events
        .register_event(
            FILL_EVENT,
            ACCOUNT_EVENT_SCHEMA_VERSION,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .map_err(event_error)?;
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

fn validate_strategy_run_capacity(
    event_engine: &EventEngineConfig,
    strategies: &[StrategyDefinition],
) -> Result<(), ConfigurationError> {
    // The PRIMARY lane derives its snapshot staging capacity from async_lane_capacity;
    // the engine rejects lanes above the per-barrier staging budget at registration time.
    let per_barrier = event_engine.snapshot_barriers.per_barrier_staging_capacity;
    for definition in strategies {
        if !definition.enabled {
            continue;
        }
        if definition.runtime.async_lane_capacity > per_barrier {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} async_lane_capacity {} exceeds event_engine.snapshot_barriers.per_barrier_staging_capacity {}",
                definition.strategy_key, definition.runtime.async_lane_capacity, per_barrier
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

/// Builds the configured startup metadata provider: a verified file, a venue fetch, or none.
///
/// The venue request list is the set of symbols the configured market sources bind, frozen at
/// configuration time. The provider re-fetches on every instance creation, so a venue that changes
/// its metadata mid-run makes the affected instance fail closed instead of silently reusing units.
fn metadata_provider(
    bootstrap: &StrategyBootstrap,
) -> Option<Arc<dyn titan_strategy_runtime::MetadataSnapshotProvider>> {
    match (
        bootstrap.metadata_snapshot_path.clone(),
        bootstrap.metadata_source,
    ) {
        (Some(path), _) => Some(Arc::new(FileMetadataSnapshotProvider { path })),
        // Without a bound symbol there is nothing to ask the venue, and an empty snapshot would
        // replace the "no object context" state with an empty-identity table for every instance.
        (None, Some(source)) if !bootstrap.metadata_requests.is_empty() => Some(Arc::new(
            titan_strategy_runtime::VenueMetadataProvider::new(
                source.transport(),
                bootstrap.metadata_requests.clone(),
                titan_strategy_runtime::MetadataFetchPolicy::default(),
            ),
        )),
        (None, Some(_)) => None,
        (None, None) => None,
    }
}

/// File-backed provider for the startup metadata snapshot. Re-validates on every call so a file
/// that changes after startup cannot silently feed different units into a new instance.
pub(crate) struct FileMetadataSnapshotProvider {
    pub path: PathBuf,
}

impl titan_strategy_runtime::MetadataSnapshotProvider for FileMetadataSnapshotProvider {
    fn snapshot(&self) -> Result<Arc<titan_market_service::MetadataSnapshot>, String> {
        load_metadata_snapshot_file(&self.path).map_err(|error| error.to_string())
    }
}

/// Loads and verifies one startup metadata snapshot. The digest is part of the identity that later
/// feeds `ObjectTable::metadata_snapshot_digest`, so a mismatched file is refused instead of trusted.
pub(crate) fn load_metadata_snapshot_file(
    path: &Path,
) -> Result<Arc<titan_market_service::MetadataSnapshot>, ConfigurationError> {
    let bytes = std::fs::read(path).map_err(|source| ConfigurationError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let snapshot: titan_market_service::MetadataSnapshot =
        serde_json::from_slice(&bytes).map_err(|error| {
            ConfigurationError::Invalid(format!(
                "metadata snapshot {} is not valid JSON: {error}",
                path.display()
            ))
        })?;
    if !snapshot.digest_matches() {
        return Err(ConfigurationError::Invalid(format!(
            "metadata snapshot {} digest does not match its contents",
            path.display()
        )));
    }
    if snapshot.schema_version != 1 {
        return Err(ConfigurationError::Invalid(format!(
            "metadata snapshot {} has unsupported schema version {}",
            path.display(),
            snapshot.schema_version
        )));
    }
    Ok(Arc::new(snapshot))
}

fn adapt_strategies(
    strategies: &mut [StrategyDefinition],
    config_directory: &Path,
    bootstrap: StrategyServiceConfig,
    market_sources: &[MarketSourceDefinition],
) -> Result<Option<StrategyBootstrap>, ConfigurationError> {
    if strategies.is_empty() {
        return Ok(None);
    }
    let mut allowed_roots = Vec::with_capacity(bootstrap.allowed_strategy_roots.len());
    for root in bootstrap.allowed_strategy_roots {
        allowed_roots.push(canonical_config_path(
            config_directory,
            &root,
            "compiled_strategy root",
        )?);
    }
    if !strategies.is_empty() && allowed_roots.is_empty() {
        return Err(ConfigurationError::Invalid(
            "strategy_service requires at least one allowed_strategy_roots entry".into(),
        ));
    }
    if bootstrap.checkpoint_root.is_some() && bootstrap.recovery_storage.is_some() {
        return Err(ConfigurationError::Invalid(
            "checkpoint_root and recovery_storage cannot both be configured".into(),
        ));
    }
    let metadata_source = bootstrap.metadata_source;
    // The venue must answer for every symbol a configured market source binds: those are the only
    // symbols an object table can reference.
    let metadata_requests = metadata_source.map(|_| {
        market_sources
            .iter()
            .filter(|source| source.enabled)
            .flat_map(|source| {
                source.instruments.iter().map(move |instrument| {
                    titan_strategy_runtime::MetadataRequest {
                        local_asset_no: instrument.asset_id.0,
                        source_key: source.source_key.clone(),
                        native_symbol: instrument.native_symbol.clone(),
                    }
                })
            })
            .collect::<Vec<_>>()
    });
    let metadata_snapshot_path = match bootstrap.metadata_snapshot.as_ref() {
        Some(path) => Some(canonical_config_path(
            config_directory,
            path,
            "metadata snapshot",
        )?),
        None => None,
    };
    if let Some(path) = metadata_snapshot_path.as_ref() {
        load_metadata_snapshot_file(path)?;
    }
    let recovery_storage = if let Some(mut storage) = bootstrap.recovery_storage {
        storage
            .validate()
            .map_err(|e| ConfigurationError::Invalid(e.to_string()))?;
        if storage.path.is_relative() {
            storage.path = config_directory.join(&storage.path);
        }
        let parent = storage.path.parent().ok_or_else(|| {
            ConfigurationError::Invalid("recovery database parent missing".into())
        })?;
        std::fs::create_dir_all(parent).map_err(|e| ConfigurationError::Invalid(e.to_string()))?;
        storage.path = if storage.path.exists() {
            std::fs::canonicalize(&storage.path)
        } else {
            std::fs::canonicalize(parent)
                .map(|p| p.join(storage.path.file_name().expect("database filename")))
        }
        .map_err(|e| ConfigurationError::Invalid(e.to_string()))?;
        Some(storage)
    } else {
        None
    };
    let checkpoint_root = if let Some(path) = bootstrap.checkpoint_root {
        let path = if path.is_absolute() {
            path
        } else {
            config_directory.join(path)
        };
        std::fs::create_dir_all(&path).map_err(|error| {
            ConfigurationError::Invalid(format!(
                "cannot create strategy checkpoint root {}: {error}",
                path.display()
            ))
        })?;
        Some(std::fs::canonicalize(&path).map_err(|error| {
            ConfigurationError::Invalid(format!(
                "cannot resolve strategy checkpoint root {}: {error}",
                path.display()
            ))
        })?)
    } else {
        None
    };
    for definition in strategies {
        if (definition.recovery != StrategyRecoveryPolicy::Fresh || !definition.accounts.is_empty())
            && checkpoint_root.is_none()
            && recovery_storage.is_none()
        {
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} requires strategy_service.checkpoint_root",
                definition.strategy_key
            )));
        }
        if definition.strategy_file.loader_type.as_ref() != "native" {
            let hint = if definition.strategy_file.loader_type.as_ref() == "native-v13" {
                "; migrate: recompile the strategy and set loader_type = \"native\""
            } else {
                ""
            };
            return Err(ConfigurationError::Invalid(format!(
                "strategy {} uses unsupported production loader {}{}",
                definition.strategy_key, definition.strategy_file.loader_type, hint
            )));
        }
        let relative = definition
            .strategy_file
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
                "strategy compiled_strategy {} is not a file",
                package_root.display()
            )));
        }
        if !allowed_roots
            .iter()
            .any(|root| package_root.starts_with(root))
        {
            return Err(ConfigurationError::Invalid(format!(
                "strategy package {} is outside allowed_strategy_roots",
                package_root.display()
            )));
        }
        definition.strategy_file.uri = Arc::from(format!("file://{}", package_root.display()));
    }
    let config = StrategyCoreConfig {
        allowed_strategy_roots: allowed_roots
            .iter()
            .map(|root| Arc::from(root.to_string_lossy().as_ref()))
            .collect::<Vec<_>>()
            .into(),
        checkpoint_root,
        recovery_storage,
        ..StrategyCoreConfig::default()
    };
    Ok(Some(StrategyBootstrap {
        metadata_snapshot_path,
        metadata_source,
        metadata_requests: metadata_requests.unwrap_or_default(),
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
    }
    Ok(())
}

/// Rejects any configuration whose instrument units cannot be proven consistent.
///
/// The arithmetic lives in `titan_strategy_runtime::validate_instrument_units` so `core-run`,
/// `titan instruments verify`, the management path and the Python preflight share one verdict
/// instead of each re-deriving base deltas. This wrapper only maps the first failure into the
/// configuration error surface.
fn validate_instrument_definitions(
    market_sources: &[MarketSourceDefinition],
    accounts: &[AccountDefinition],
    strategies: &[StrategyDefinition],
) -> Result<(), ConfigurationError> {
    let report =
        titan_strategy_runtime::validate_instrument_units(market_sources, accounts, strategies);
    if let Some(error) = report.errors.first() {
        return Err(ConfigurationError::Invalid(format!(
            "{} [{}] {}",
            error.code(),
            error.subject(),
            error.detail()
        )));
    }
    Ok(())
}

/// Parses a runtime TOML and validates its instrument units without starting any service.
///
/// This is the `titan instruments verify` entry point: it is offline, read-only and refuses the
/// same configurations `core-run` refuses.
pub fn verify_instrument_units_toml(
    path: impl AsRef<Path>,
    metadata: Option<&titan_market_service::MetadataSnapshot>,
) -> Result<titan_strategy_runtime::InstrumentValidationReport, ConfigurationError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigurationError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let config = toml::from_str::<ApplicationConfig>(&contents).map_err(|source| {
        ConfigurationError::Toml {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if config.schema_version != APPLICATION_CONFIG_SCHEMA_VERSION {
        return Err(ConfigurationError::UnsupportedSchema(config.schema_version));
    }
    validate_runtime_definitions(&config.market_sources, &config.accounts)?;
    for strategy in config.strategies.iter().filter(|strategy| strategy.enabled) {
        titan_strategy_runtime::validate_execution_binding(
            &strategy.runtime,
            &strategy.markets,
            &strategy.accounts,
            None,
        )
        .map_err(|code| ConfigurationError::Invalid(format!("{} {code}", strategy.strategy_key)))?;
    }
    Ok(
        titan_strategy_runtime::validate_instrument_units_with_metadata(
            &config.market_sources,
            &config.accounts,
            &config.strategies,
            metadata,
        ),
    )
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

    /// The V14 `fee` domain is fed from the operator binding, so the TOML shape must parse: a
    /// binding without `fees` stays valid (no fee record), and a declared one carries both rates.
    #[test]
    fn account_fees_are_optional_but_both_rates_travel_together() {
        let inline = r#"
schema_version = 1
[[strategies]]
strategy_key = "fees"
strategy_id = 1
package = { loader_type = "native", uri = "file://unused.titan", expected_digest = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }
markets = []
accounts = [
  { local_account_no = 0, account_key = "maker", tradable_assets = [], fees = { maker_fee_bps = -3, taker_fee_bps = 5 } },
  { local_account_no = 1, account_key = "hedge", tradable_assets = [] },
]
risk_scope = "test"
runtime = { async_lane_capacity = 8192, critical_reserve = 1024, reliable_pending_capacity = 256, worker_policy = "spin_sleep", timer_capacity = 16, max_handler_duration = { secs = 0, nanos = 100000000 }, startup_timeout = { secs = 30, nanos = 0 }, stop_timeout = { secs = 10, nanos = 0 } }
recovery = "fresh"
enabled = true
definition_version = 1
"#;
        let config: ApplicationConfig = toml::from_str(inline).expect("fee bindings parse");
        let accounts = &config.strategies[0].accounts;
        let declared = accounts[0].fees.expect("maker declares rates");
        assert_eq!(declared.maker_fee_bps, -3, "negative is a maker rebate");
        assert_eq!(declared.taker_fee_bps, 5);
        assert!(
            accounts[1].fees.is_none(),
            "an undeclared account must stay absent, never default to zero"
        );

        // A half-declared fee (missing taker) is a configuration error, not a zero-filled rate.
        let half = inline.replace(", taker_fee_bps = 5 ", " ");
        assert!(
            toml::from_str::<ApplicationConfig>(&half).is_err(),
            "a partial fee declaration must be refused"
        );
    }

    fn application() -> ApplicationConfig {
        ApplicationConfig {
            managed_by_console: false,
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
    fn metadata_snapshot_and_venue_source_cannot_both_be_configured() {
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("metadata.json");
        std::fs::write(&snapshot, b"{}").unwrap();
        let mut config = application();
        config.strategy_service.metadata_source = Some(MetadataSourceConfig::Hyperliquid);
        config.strategy_service.metadata_snapshot = Some(snapshot);
        let error = match ConfigurationAdapter::adapt(config, directory.path()) {
            Ok(_) => panic!("a snapshot file and a venue source must not coexist"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("cannot both be configured"),
            "{error}"
        );
    }

    #[test]
    fn venue_metadata_requests_cover_every_enabled_market_binding() {
        // The provider is asked for exactly the symbols a configured market source binds; that is
        // the set an object table can reference.
        let requests: Vec<titan_strategy_runtime::MetadataRequest> = [MarketSourceDefinition {
            source_key: Arc::from("hyperliquid"),
            connector_type: Arc::from("hyperliquid"),
            connector_config: Arc::from(&b""[..]),
            instruments: Arc::from([titan_market_service::MarketInstrumentBinding {
                native_symbol: Arc::from("BTC"),
                asset_id: titan_market_service::AssetId(1),
                price_tick: "0.1".parse().unwrap(),
                quantity_lot: "0.00001".parse().unwrap(),
                contract_multiplier: "1".parse().unwrap(),
                min_quantity: None,
                min_notional_quote: None,
                linear_quote_currency: Some(Arc::from("USDC")),
            }]),
            enabled: true,
            definition_version: 1,
        }]
        .iter()
        .filter(|source| source.enabled)
        .flat_map(|source| {
            source.instruments.iter().map(move |instrument| {
                titan_strategy_runtime::MetadataRequest {
                    local_asset_no: instrument.asset_id.0,
                    source_key: source.source_key.clone(),
                    native_symbol: instrument.native_symbol.clone(),
                }
            })
        })
        .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].native_symbol.as_ref(), "BTC");
        assert_eq!(requests[0].source_key.as_ref(), "hyperliquid");
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
    fn startup_metadata_snapshot_is_digest_and_schema_verified() {
        let directory = std::env::temp_dir().join(format!(
            "titan-metadata-snapshot-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("metadata.json");
        let mut snapshot = titan_market_service::MetadataSnapshot {
            schema_version: 1,
            captured_at_ns: 1,
            instruments: vec![],
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
        assert!(load_metadata_snapshot_file(&path).is_ok());

        let mut tampered = snapshot.clone();
        tampered.captured_at_ns = 2;
        std::fs::write(&path, serde_json::to_vec(&tampered).unwrap()).unwrap();
        assert!(load_metadata_snapshot_file(&path).is_err());

        let mut future = snapshot.clone();
        future.schema_version = 2;
        future.digest = future.compute_digest();
        std::fs::write(&path, serde_json::to_vec(&future).unwrap()).unwrap();
        assert!(load_metadata_snapshot_file(&path).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_metadata_provider_reports_unavailable_or_invalid_snapshots() {
        use titan_strategy_runtime::MetadataSnapshotProvider;

        let directory = std::env::temp_dir().join(format!(
            "titan-metadata-provider-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("metadata.json");
        let mut snapshot = titan_market_service::MetadataSnapshot {
            schema_version: 1,
            captured_at_ns: 1,
            instruments: vec![],
            digest: [0; 32],
        };
        snapshot.digest = snapshot.compute_digest();
        std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
        let provider = FileMetadataSnapshotProvider { path: path.clone() };
        assert!(provider.snapshot().is_ok());

        std::fs::write(&path, b"{}").unwrap();
        assert!(provider.snapshot().is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(provider.snapshot().is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pair_arb_runtime_template_deserializes_human_readable_byte_fields() {
        let config: ApplicationConfig =
            toml::from_str(include_str!("../../../deploy/pair_arb/runtime.toml")).unwrap();
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
        assert_eq!(
            config.strategies[0].strategy_file.loader_type.as_ref(),
            "native"
        );
        let deployment = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/pair_arb");
        ConfigurationAdapter::adapt(config, &deployment).unwrap();
    }

    #[test]
    fn strategy_async_lane_capacity_must_fit_snapshot_staging() {
        // The PRIMARY lane derives its snapshot staging capacity from
        // async_lane_capacity, so the engine only accepts lanes at or below the
        // per-barrier staging budget (8192 by default).
        let inline = r#"
schema_version = 1
[[strategies]]
strategy_key = "lane"
strategy_id = 1
package = { loader_type = "native", uri = "file://unused.titan", expected_digest = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }
markets = []
accounts = []
risk_scope = "test"
runtime = { async_lane_capacity = 8192, critical_reserve = 1024, reliable_pending_capacity = 256, worker_policy = "spin_sleep", timer_capacity = 16, max_handler_duration = { secs = 0, nanos = 100000000 }, startup_timeout = { secs = 30, nanos = 0 }, stop_timeout = { secs = 10, nanos = 0 } }
recovery = "fresh"
enabled = true
definition_version = 1
"#;
        let mut config: ApplicationConfig = toml::from_str(inline).unwrap();
        validate_strategy_run_capacity(&config.event_engine, &config.strategies).unwrap();
        config.strategies[0].runtime.async_lane_capacity = 16_384;
        assert!(validate_strategy_run_capacity(&config.event_engine, &config.strategies).is_err());
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

    #[test]
    fn recovery_configuration_rejects_driver_conflicts_and_shared_history_path() {
        let directory = tempfile::tempdir().unwrap();
        let mut definitions = vec![live_strategy(StrategyDataMode::Tick)];
        let mut storage =
            titan_strategy_runtime::SqliteRecoveryConfig::new(directory.path().join("runtime.db"));
        storage.driver = "postgres".into();
        let mut config = StrategyServiceConfig {
            allowed_strategy_roots: vec![directory.path().to_owned()],
            recovery_storage: Some(storage),
            ..Default::default()
        };
        assert!(adapt_strategies(&mut definitions, directory.path(), config.clone(), &[]).is_err());
        config.recovery_storage.as_mut().unwrap().driver = "sqlite".into();
        config.checkpoint_root = Some(directory.path().to_owned());
        assert!(adapt_strategies(&mut definitions, directory.path(), config, &[]).is_err());
        let mut adapted = ConfigurationAdapter::adapt(application(), directory.path()).unwrap();
        adapted.strategy_bootstrap = Some(StrategyBootstrap {
            metadata_requests: Vec::new(),
            metadata_source: None,
            config: StrategyCoreConfig {
                recovery_storage: Some(titan_strategy_runtime::SqliteRecoveryConfig::new(
                    directory.path().join("shared.db"),
                )),
                ..Default::default()
            },
            metadata_snapshot_path: None,
        });
        assert!(
            TradingRuntime::start_observed(
                adapted,
                directory.path().join("shared.db"),
                "test".into()
            )
            .is_err()
        );
    }

    fn live_strategy(data_mode: StrategyDataMode) -> StrategyDefinition {
        StrategyDefinition {
            strategy_key: Arc::from("freeze-test"),
            runtime_strategy_no: titan_strategy_runtime::RuntimeStrategyNo(99),
            strategy_file: titan_strategy_runtime::StrategyFileRef {
                loader_type: Arc::from("native"),
                uri: Arc::from("file:///unused"),
                expected_digest: [1; 32],
                parameters_digest: None,
            },
            markets: Arc::from([titan_strategy_runtime::StrategyMarketBinding {
                local_market_no: 0,
                local_asset_no: 0,
                source_key: Arc::from("market"),
                asset_id: 1,
                data_mode,
            }]),
            accounts: Arc::from([]),
            risk_scope: titan_strategy_runtime::RiskScopeRef(Arc::from("unused")),
            runtime: titan_strategy_runtime::StrategyRuntimeSpec::default(),
            recovery: titan_strategy_runtime::StrategyRecoveryPolicy::Fresh,
            enabled: true,
            run_config_revision: 1,
        }
    }

    #[test]
    fn core_live_profile_freezes_bar_and_hybrid_until_producer_exists() {
        let bar_mode = live_strategy(titan_strategy_runtime::StrategyDataMode::Bar {
            timeframe_ns: 60_000_000_000,
        });
        assert!(matches!(
            validate_core_live_strategy_profile(&[bar_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let hybrid_mode = live_strategy(titan_strategy_runtime::StrategyDataMode::Hybrid {
            signal_timeframe_ns: 60_000_000_000,
        });
        assert!(matches!(
            validate_core_live_strategy_profile(&[hybrid_mode]),
            Err(ConfigurationError::Invalid(_))
        ));

        let tick = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick);
        assert!(validate_core_live_strategy_profile(&[tick]).is_ok());

        let mut account_strategy = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick);
        account_strategy.accounts = Arc::from([titan_strategy_runtime::StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([titan_strategy_runtime::StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
            fees: None,
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
                contract_multiplier: "1".parse().unwrap(),
                min_quantity: None,
                min_notional_quote: None,
                linear_quote_currency: Some(Arc::from("USDT")),
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
                linear_quote_currency: None,
            }]),
            currencies: Arc::from([]),
            ownership: titan_account_service::OrderOwnershipPolicy::ManagedOnly {
                client_id_prefix: Arc::from("titan-"),
            },
            shutdown_order_policy: titan_account_service::ShutdownOrderPolicy::LeaveOpen,
            enabled: true,
            definition_version: 1,
        };
        let mut strategy = live_strategy(titan_strategy_runtime::StrategyDataMode::Tick);
        strategy.accounts = Arc::from([titan_strategy_runtime::StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([titan_strategy_runtime::StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
            fees: None,
        }]);
        let report = titan_strategy_runtime::validate_instrument_units(
            &[market_source],
            &[account],
            &[strategy],
        );
        assert!(!report.is_ok());
        assert!(
            report
                .error_codes()
                .contains(&titan_strategy_runtime::INSTRUMENT_UNITS_MARKET_ACCOUNT_MISMATCH)
        );
    }
}

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use titan_account_service::{
    ACCOUNT_EVENT_SCHEMA_VERSION, AccountLifecycle, AccountService, AccountSnapshotState,
    BALANCE_CHANGED_EVENT, ExecutionDispatcher, ExecutionObserver, FILL_EVENT,
    FILL_EVENT_SCHEMA_VERSION, ORDER_CHANGED_EVENT, ObservedExecutionResult, OrderFilter,
    POSITION_CHANGED_EVENT, PositionFilter,
    STREAM_INVALIDATED_EVENT as ACCOUNT_STREAM_INVALIDATED_EVENT,
    STREAM_STATE_CHANGED_EVENT as ACCOUNT_STREAM_STATE_CHANGED_EVENT,
};
use titan_core_types::{ClosureResource, ResourceScope};
use titan_event_engine::{
    EventEngineHandle, PrimaryAsyncLaneConfig, PrimaryAsyncLaneHandle, PrimarySubscriptionSpec,
    SubscriberState,
};
use titan_market_service::{
    BAR_BATCH_EVENT, BBO_EVENT, ConnectorHealth, DEPTH_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION,
    MarketDataKind, MarketService, MarketSubscribeRequest,
    STREAM_INVALIDATED_EVENT as MARKET_STREAM_INVALIDATED_EVENT, TRADE_BATCH_EVENT,
};
use tracing::warn;

use crate::*;

struct StrategyExecutionResultRelay {
    target: RwLock<Option<std::sync::Weak<dyn StrategyRuntime>>>,
    audit: Arc<dyn ExecutionObserver>,
}

impl StrategyExecutionResultRelay {
    fn new(audit: Arc<dyn ExecutionObserver>) -> Self {
        Self {
            target: RwLock::new(None),
            audit,
        }
    }

    fn attach(&self, runtime: &Arc<dyn StrategyRuntime>) {
        *self.target.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::downgrade(runtime));
    }
}

impl ExecutionObserver for StrategyExecutionResultRelay {
    fn observe(&self, result: ObservedExecutionResult) {
        self.audit.observe(result.clone());
        let runtime = self
            .target
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(runtime) = runtime
            && let Err(error) = runtime.execution_result(result)
        {
            warn!(
                reason_code = error.reason_code.as_ref(),
                "strategy execution result could not be queued"
            );
        }
    }
}

#[derive(Clone)]
pub struct StrategyCoreConfig {
    pub max_strategy_runtimes: usize,
    pub max_strategy_cache_entries: usize,
    pub allowed_loader_types: BTreeSet<Arc<str>>,
    pub allowed_strategy_roots: Arc<[Arc<str>]>,
    pub allowed_event_types: BTreeSet<(Arc<str>, u32)>,
    pub allowed_worker_policies: Vec<titan_event_engine::SubscriberRuntimeMode>,
    pub allowed_capabilities: StrategyCapabilities,
    pub max_lane_capacity: usize,
    pub max_pending_capacity: usize,
    pub max_state_capacity: usize,
    pub max_concurrent_loads: usize,
    /// Legacy directory configuration; maps only to SQLite, never to a file writer.
    pub checkpoint_root: Option<PathBuf>,
    pub recovery_storage: Option<SqliteRecoveryConfig>,
    pub recovery_store: Option<Arc<dyn StrategyRecoveryStore>>,
    pub require_strategy_signature: bool,
    pub trusted_strategy_keys: BTreeMap<Arc<str>, [u8; 32]>,
}

impl Default for StrategyCoreConfig {
    fn default() -> Self {
        Self {
            max_strategy_runtimes: 128,
            max_strategy_cache_entries: 64,
            allowed_loader_types: [Arc::from("native-v13")].into_iter().collect(),
            allowed_strategy_roots: Arc::from([]),
            allowed_event_types: BTreeSet::new(),
            allowed_worker_policies: vec![
                titan_event_engine::SubscriberRuntimeMode::Dedicated,
                titan_event_engine::SubscriberRuntimeMode::SpinSleep,
                titan_event_engine::SubscriberRuntimeMode::Park,
            ],
            allowed_capabilities: StrategyCapabilities(
                !(StrategyCapabilities::READ_RISK.0 | StrategyCapabilities::CHECKPOINT_STATE.0),
            ),
            max_lane_capacity: 1 << 20,
            max_pending_capacity: 1 << 18,
            max_state_capacity: 1 << 20,
            max_concurrent_loads: 2,
            checkpoint_root: None,
            recovery_storage: None,
            recovery_store: None,
            require_strategy_signature: true,
            trusted_strategy_keys: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct StrategyServiceDependencies {
    pub events: EventEngineHandle,
    pub markets: Arc<dyn MarketService>,
    pub accounts: Arc<dyn AccountService>,
    pub execution_dispatcher: Option<ExecutionDispatcher>,
    pub execution_observer: Option<Arc<dyn ExecutionObserver>>,
}

pub struct StrategyEntry {
    pub handle: StrategyHandle,
    pub definition: StrategyDefinition,
    pub manifest: CompiledStrategyMetadata,
    pub compiled_strategy_digest: CompiledStrategyDigest,
    pub runtime: Arc<dyn StrategyRuntime>,
    pub lane: PrimaryAsyncLaneHandle,
    pub activation: Arc<StrategyActivationGate>,
    resources: Mutex<Option<ResourceScope>>,
    cleanup_deadline: Arc<Mutex<Option<Instant>>>,
}

struct DisabledSnapshotSink;

impl StrategyRecoveryStore for DisabledSnapshotSink {
    fn submit(&self, _snapshot: StrategyRecoveryPoint) -> LocalResult<()> {
        Ok(())
    }

    fn enabled(&self) -> bool {
        false
    }
}

fn checkpoint_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::CheckpointFailed,
        "state_snapshot",
        code,
        "strategy checkpoint operation failed",
    )
}

impl StrategyEntry {
    fn cleanup_before(&self, deadline: Instant) -> LocalResult<()> {
        *self
            .cleanup_deadline
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(deadline);
        self.cleanup()
    }
    fn cleanup(&self) -> LocalResult<()> {
        if let Some(mut scope) = self
            .resources
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            scope.close().map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::Internal,
                    "cleanup",
                    "resource_release_failed",
                    "strategy resources could not be completely released",
                )
            })?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct StrategyRegistryState {
    by_id: HashMap<RuntimeStrategyNo, Arc<StrategyEntry>>,
    by_key: HashMap<Arc<str>, StrategyHandle>,
    last_generation: HashMap<RuntimeStrategyNo, u64>,
}

#[derive(Default)]
pub struct StrategyRegistry {
    state: RwLock<StrategyRegistryState>,
}

struct GlobalOperation {
    snapshot: StrategyOperationSnapshot,
    runtime: Option<(Arc<dyn StrategyRuntime>, StrategyOperationId)>,
    cleanup: Option<Arc<StrategyEntry>>,
}

pub struct StrategyServiceCore {
    config: StrategyCoreConfig,
    dependencies: StrategyServiceDependencies,
    loaders: Arc<StrategyLoaderRegistry>,
    runtimes: Arc<StrategyRuntimeFactoryRegistry>,
    cache: LoadedStrategyCache,
    registry: StrategyRegistry,
    accepting: AtomicBool,
    next_operation: AtomicU64,
    operations: Mutex<titan_core_types::OperationHistory<StrategyOperationId, GlobalOperation>>,
    cold_loads: Arc<ColdLoadLimiter>,
    checkpoints: Option<Arc<dyn StrategyRecoveryStore>>,
    scheduler: crate::scheduler::Scheduler,
}

struct ColdLoadLimiter {
    active: Mutex<usize>,
    changed: Condvar,
    capacity: usize,
}

struct ColdLoadPermit(Arc<ColdLoadLimiter>);

impl Drop for ColdLoadPermit {
    fn drop(&mut self) {
        let mut active = self.0.active.lock().unwrap_or_else(|p| p.into_inner());
        *active = active.saturating_sub(1);
        self.0.changed.notify_one();
    }
}

impl ColdLoadLimiter {
    fn acquire(self: &Arc<Self>, deadline: Instant) -> LocalResult<ColdLoadPermit> {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        while *active >= self.capacity {
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(load_timeout("cold_load_capacity_timeout"));
            }
            let (next, result) = self
                .changed
                .wait_timeout(active, timeout)
                .unwrap_or_else(|p| p.into_inner());
            active = next;
            if result.timed_out() && *active >= self.capacity {
                return Err(load_timeout("cold_load_capacity_timeout"));
            }
        }
        *active += 1;
        drop(active);
        Ok(ColdLoadPermit(self.clone()))
    }
}

impl StrategyServiceCore {
    pub fn new(
        mut config: StrategyCoreConfig,
        dependencies: StrategyServiceDependencies,
        loaders: Arc<StrategyLoaderRegistry>,
        runtimes: Arc<StrategyRuntimeFactoryRegistry>,
    ) -> LocalResult<Self> {
        if config.max_strategy_runtimes == 0 {
            return Err(definition_error("max_strategy_runtimes_zero"));
        }
        if config.max_concurrent_loads == 0 {
            return Err(definition_error("max_concurrent_loads_zero"));
        }
        let cache = LoadedStrategyCache::new(config.max_strategy_cache_entries)?;
        let max_concurrent_loads = config.max_concurrent_loads;
        if (config.recovery_storage.is_some() && config.checkpoint_root.is_some())
            || (config.recovery_store.is_some()
                && (config.recovery_storage.is_some() || config.checkpoint_root.is_some()))
        {
            return Err(checkpoint_error("recovery_config_conflict"));
        }
        let checkpoints = match config.recovery_store.clone() {
            Some(store) => Some(store),
            None if config.recovery_storage.is_some() => {
                let storage = config.recovery_storage.clone().expect("checked above");
                if let Some(parent) = storage.path.parent() {
                    validate_legacy_recovery_imports(parent, &storage.path)?;
                }
                Some(SqliteRecoveryStore::open(storage)? as Arc<dyn StrategyRecoveryStore>)
            }
            None => config
                .checkpoint_root
                .as_ref()
                .map(|root| {
                    reject_legacy_recovery_directory(root)?;
                    SqliteRecoveryStore::open(SqliteRecoveryConfig::new(root.join("runtime.db")))
                        .map(|store| store as Arc<dyn StrategyRecoveryStore>)
                })
                .transpose()?,
        };
        if checkpoints.as_ref().is_some_and(|store| !store.durable()) {
            return Err(checkpoint_error("recovery_durability_unavailable"));
        }
        config.recovery_store = checkpoints.clone();
        Ok(Self {
            config,
            dependencies,
            loaders,
            runtimes,
            cache,
            registry: StrategyRegistry::default(),
            accepting: AtomicBool::new(true),
            next_operation: AtomicU64::new(1),
            operations: Mutex::new(titan_core_types::OperationHistory::new(|value| {
                value.snapshot.state == StrategyOperationState::Pending
            })),
            cold_loads: Arc::new(ColdLoadLimiter {
                active: Mutex::new(0),
                changed: Condvar::new(),
                capacity: max_concurrent_loads,
            }),
            checkpoints,
            scheduler: crate::scheduler::Scheduler::new()
                .map_err(|_| checkpoint_error("scheduler_start_failed"))?,
        })
    }

    pub fn quiesce(&self, deadline: Instant) -> LocalResult<()> {
        self.accepting.store(false, Ordering::Release);
        let entries = self
            .registry
            .state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut first_error = None;
        for entry in entries {
            let runtime_result = (|| {
                if entry.runtime.state().lifecycle == StrategyLifecycle::Stopped {
                    return Ok(());
                }
                let local = entry.runtime.stop(deadline)?;
                wait_runtime(&entry.runtime, local, deadline)
            })();
            let cleanup_result = entry.cleanup_before(deadline);
            if first_error.is_none() {
                first_error = runtime_result.err().or_else(|| cleanup_result.err());
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            if let Some(store) = &self.checkpoints {
                store.close(deadline)?;
            }
            Ok(())
        }
    }

    fn ensure_accepting(&self) -> LocalResult<()> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StrategyError::new(
                StrategyErrorKind::InvalidState,
                "admin",
                "service_quiescing",
                "strategy service is not accepting operations",
            ))
        }
    }

    fn create_entry(
        &self,
        definition: StrategyDefinition,
        generation: u64,
        checkpoint: Option<StrategyRecoveryPoint>,
    ) -> LocalResult<Arc<StrategyEntry>> {
        validate_definition(&self.config, &definition)?;
        let handle = StrategyHandle {
            runtime_strategy_no: definition.runtime_strategy_no,
            generation,
        };
        let loader = self.loaders.create(
            &definition.strategy_file.loader_type,
            StrategyLoaderContext {
                allowed_strategy_roots: self.config.allowed_strategy_roots.clone(),
                require_signature: self.config.require_strategy_signature,
                ed25519_keys: self.config.trusted_strategy_keys.clone(),
            },
        )?;
        let deadline = Instant::now() + definition.runtime.startup_timeout;
        let inspect_loader = loader.clone();
        let inspect_strategy_file = definition.strategy_file.clone();
        let manifest = self.run_cold(deadline, move || {
            inspect_loader.inspect(&inspect_strategy_file)
        })?;
        validate_manifest(&self.config, &definition, &manifest)?;
        let key = StrategyCacheKey {
            compiled_strategy_digest: definition.strategy_file.expected_digest,
        };
        let compiled_strategy = if let Some(value) = self.cache.get(&key) {
            value
        } else {
            let request = StrategyLoadRequest {
                strategy_file: definition.strategy_file.clone(),
            };
            let load_loader = loader.clone();
            let loaded = self.run_cold(deadline, move || load_loader.load(request, deadline))?;
            if loaded.id.digest != definition.strategy_file.expected_digest {
                return Err(StrategyError::new(
                    StrategyErrorKind::DigestMismatch,
                    "load",
                    "compiled_strategy_digest_mismatch",
                    "loaded compiled_strategy digest does not match the pinned definition",
                ));
            }
            self.cache.insert(key, loaded.clone());
            loaded
        };
        validate_manifest(&self.config, &definition, &compiled_strategy.manifest)?;
        if compiled_strategy.manifest != manifest {
            return Err(StrategyError::new(
                StrategyErrorKind::DigestMismatch,
                "load",
                "manifest_changed_during_load",
                "loaded compiled_strategy manifest does not match the inspected manifest",
            ));
        }
        let compiled_strategy_digest = compiled_strategy.id;
        let resolved_markets = self.resolve_markets(&definition)?;
        let resolved_accounts = self.resolve_accounts(&definition)?;
        if !resolved_accounts.is_empty() && self.checkpoints.is_none() {
            return Err(StrategyError::new(
                StrategyErrorKind::UnsupportedCapability,
                "definition",
                "execution_requires_checkpoint_root",
                "strategies with account execution require a durable checkpoint root",
            ));
        }
        let dispatcher = self.dependencies.execution_dispatcher.as_ref();
        let execution_relay = if resolved_accounts.is_empty() {
            None
        } else {
            let audit_observer = self
                .dependencies
                .execution_observer
                .as_ref()
                .ok_or_else(|| definition_error("direct_execution_observer_unavailable"))?;
            Some(Arc::new(StrategyExecutionResultRelay::new(
                audit_observer.clone(),
            )))
        };
        let resources = ResourceScope::new(titan_core_types::ComponentIdentity::new(
            "titan.strategy.runtime",
            format!("{}-{}", definition.runtime_strategy_no.0, generation),
        ));
        for binding in &resolved_accounts {
            let lease = self
                .dependencies
                .accounts
                .pin_binding(binding.account)
                .map_err(|_| dependency_error("account_binding_expired"))?;
            resources
                .handle()
                .register("account_binding", lease)
                .map_err(|_| dependency_error("account_binding_registration_failed"))?;
        }
        for binding in &resolved_markets {
            let lease = self
                .dependencies
                .markets
                .pin_binding(binding.source)
                .map_err(|_| dependency_error("market_binding_expired"))?;
            resources
                .handle()
                .register("market_binding", lease)
                .map_err(|_| dependency_error("market_binding_registration_failed"))?;
        }
        let direct_execution = resolved_accounts
            .iter()
            .map(|binding| {
                let dispatcher = dispatcher
                    .ok_or_else(|| definition_error("direct_execution_dispatcher_unavailable"))?;
                let connector = self
                    .dependencies
                    .accounts
                    .execution_connector(binding.account)
                    .map_err(|_| definition_error("direct_execution_connector_unavailable"))?;
                Ok(StrategyExecutionBinding {
                    local_account_no: binding.local_account_no,
                    assets: binding
                        .tradable_assets
                        .iter()
                        .map(|asset| (u64::from(asset.local_asset_no), asset.asset_id))
                        .collect(),
                    handle: dispatcher.bind(
                        binding.account,
                        connector,
                        execution_relay
                            .as_ref()
                            .expect("non-empty account bindings have an execution relay")
                            .clone(),
                    ),
                })
            })
            .collect::<LocalResult<Vec<_>>>()?;
        let activation = Arc::new(StrategyActivationGate::default());
        let context = StrategyRuntimeBuildContext {
            market_service: Some(self.dependencies.markets.clone()),
            strategy: handle,
            compiled_strategy_digest: compiled_strategy.id,
            markets: resolved_markets.clone().into(),
            accounts: resolved_accounts.clone().into(),
            execution: direct_execution.into(),
            state_snapshot_sink: self
                .checkpoints
                .as_ref()
                .map(|store| store.clone() as Arc<dyn StrategyRecoveryStore>)
                .unwrap_or_else(|| Arc::new(DisabledSnapshotSink)),
            clock: Arc::new(SystemStrategyClock),
            metrics: Arc::new(NoopStrategyMetrics),
            resources: resources.handle(),
            activation: activation.clone(),
        };
        let factory = self.runtimes.get(&manifest.runtime_type)?;
        let runtime = factory.create(&definition, compiled_strategy, context)?;
        if let Some(execution_relay) = execution_relay {
            execution_relay.attach(&runtime);
        }
        let mut subscriptions = manifest
            .subscriptions
            .iter()
            .map(|spec| PrimarySubscriptionSpec {
                event_type: spec.event_type.clone(),
                schema_version: spec.schema_version,
                qos: spec.qos,
                routing_keys: spec.routing_keys.clone(),
            })
            .collect::<Vec<_>>();
        let market_keys = resolved_markets
            .iter()
            .map(|binding| u64::from(binding.asset_id))
            .collect::<Vec<_>>();
        for subscription in subscriptions.iter_mut().filter(|subscription| {
            matches!(
                subscription.event_type.as_ref(),
                BBO_EVENT
                    | DEPTH_BATCH_EVENT
                    | TRADE_BATCH_EVENT
                    | BAR_BATCH_EVENT
                    | titan_market_service::STREAM_INVALIDATED_EVENT
            )
        }) {
            subscription.routing_keys = market_keys.clone().into();
        }
        add_host_account_subscriptions(&mut subscriptions, &resolved_accounts);
        let lane = self
            .dependencies
            .events
            .register_primary_async_lane(
                &subscriptions,
                PrimaryAsyncLaneConfig {
                    capacity: definition.runtime.async_lane_capacity,
                    critical_reserve: definition.runtime.critical_reserve,
                    reliable_pending_capacity: definition.runtime.reliable_pending_capacity,
                    snapshot_staging_capacity: definition.runtime.async_lane_capacity,
                    control_capacity: definition.runtime.timer_capacity.max(16),
                    runtime_mode: definition.runtime.worker_policy,
                    spin_iterations: 256,
                    idle_sleep: Duration::from_micros(10),
                    cpu_affinity: definition.runtime.cpu_affinity,
                    max_handler_duration: definition.runtime.max_handler_duration,
                },
                runtime.clone(),
            )
            .map_err(|error| {
                StrategyError::new(
                    StrategyErrorKind::RouteFailed,
                    "create",
                    "primary_lane_failed",
                    format!("PRIMARY async lane registration failed: {error}"),
                )
            })?;
        let events = self.dependencies.events.clone();
        let token = lane.token();
        let lane_stop_timeout = definition.runtime.stop_timeout;
        let cleanup_deadline = Arc::new(Mutex::new(None::<Instant>));
        let lane_deadline = cleanup_deadline.clone();
        resources
            .handle()
            .register(
                "primary_async_lane",
                ClosureResource(Some(move || {
                    events
                        .unregister_primary_async_lane_before(
                            token,
                            lane_deadline
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .unwrap_or_else(|| Instant::now() + lane_stop_timeout),
                        )
                        .map(|_| ())
                        .map_err(|error| {
                            titan_core_types::CoreError::new(
                                titan_core_types::ErrorKind::ResourceReleaseFailed,
                                titan_core_types::ComponentIdentity::new(
                                    "titan.strategy.runtime",
                                    "primary-lane",
                                ),
                                titan_core_types::ComponentState::Stopping,
                                "primary_lane_stop",
                                error.to_string(),
                            )
                        })
                })),
            )
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::Internal,
                    "create",
                    "resource_registration_failed",
                    "lane cleanup could not be registered",
                )
            })?;
        runtime.attach_lane(lane.clone())?;
        let readiness_deadline = Instant::now()
            .checked_add(definition.runtime.startup_timeout)
            .unwrap_or_else(Instant::now);
        loop {
            let mut all_ready = true;
            for binding in resolved_accounts.iter() {
                let orders = self
                    .dependencies
                    .accounts
                    .orders(
                        binding.account,
                        OrderFilter {
                            asset_id: None,
                            include_final: false,
                        },
                    )
                    .map_err(|_| dependency_error("account_orders_snapshot_failed"))?;
                let positions = self
                    .dependencies
                    .accounts
                    .positions(binding.account, PositionFilter::default())
                    .map_err(|_| dependency_error("account_positions_snapshot_failed"))?;
                let balances = self
                    .dependencies
                    .accounts
                    .balances(binding.account)
                    .map_err(|_| dependency_error("account_balances_snapshot_failed"))?;
                all_ready &= orders.state == AccountSnapshotState::Ready
                    && positions.state == AccountSnapshotState::Ready
                    && balances.state == AccountSnapshotState::Ready;
            }
            if all_ready {
                break;
            }
            if Instant::now() >= readiness_deadline {
                let pending = resolved_accounts
                    .iter()
                    .map(|binding| {
                        self.dependencies
                            .accounts
                            .health(binding.account)
                            .map(|health| {
                                format!(
                                    "account_id={} state={:?} message={}",
                                    binding.account.account_id.0, health.state, health.message
                                )
                            })
                            .unwrap_or_else(|error| {
                                format!(
                                    "account_id={} health_error={}",
                                    binding.account.account_id.0, error
                                )
                            })
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(StrategyError::new(
                    StrategyErrorKind::DependencyUnavailable,
                    "readiness",
                    "account_snapshot_not_ready",
                    format!("account snapshots did not become ready: {pending}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        // A new account process bootstraps open orders only. Recover unresolved checkpoint
        // intents through authoritative read-only lookups before capturing the final image.
        let mut lookups = Vec::new();
        if let Some(saved) = &checkpoint {
            for order in saved
                .orders_list
                .iter()
                .filter(|o| !matches!(o.status, 4 | 6 | 7 | 8))
            {
                let binding = resolved_accounts
                    .iter()
                    .find(|b| b.local_account_no == order.account_no)
                    .ok_or_else(|| definition_error("recovery_order_account_not_bound"))?;
                let asset = binding
                    .tradable_assets
                    .iter()
                    .find(|a| a.local_asset_no == order.asset_no)
                    .ok_or_else(|| definition_error("recovery_order_asset_not_bound"))?;
                lookups.push((
                    binding.local_account_no,
                    binding.account,
                    titan_account_service::AssetId(asset.asset_id),
                    crate::runtime_v13::client_order_id(handle, order.order_id),
                ));
            }
        }
        if lookups.len() > 128 {
            return Err(definition_error("recovery_order_lookup_capacity"));
        }
        let mut recovered = if lookups.is_empty() {
            Vec::new()
        } else {
            let accounts = self.dependencies.accounts.clone();
            self.run_cold(deadline, move || {
                let executor = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| dependency_error("recovery_lookup_executor_failed"))?;
                executor.block_on(async {
                    tokio::time::timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        async {
                            let mut evidence = Vec::new();
                            for (local, account, asset, client) in lookups {
                                let order = accounts
                                    .order_evidence(account, asset, client)
                                    .await
                                    .map_err(|error| {
                                    StrategyError::new(
                                        StrategyErrorKind::DependencyUnavailable,
                                        "recovery",
                                        "recovery_order_lookup_failed",
                                        error.to_string(),
                                    )
                                })?;
                                if order.asset_id != asset || order.client_order_id != client {
                                    return Err(definition_error(
                                        "recovery_order_lookup_identity_changed",
                                    ));
                                }
                                evidence.push((local, order));
                            }
                            Ok(evidence)
                        },
                    )
                    .await
                    .map_err(|_| load_timeout("recovery_order_lookup_deadline"))?
                })
            })?
        };
        let mut history_queries = Vec::new();
        if let Some(saved) = &checkpoint {
            if saved.orders_list.len() > 128 {
                return Err(definition_error("recovery_history_order_capacity"));
            }
            for order in saved.orders_list.iter() {
                let binding = resolved_accounts
                    .iter()
                    .find(|b| b.local_account_no == order.account_no)
                    .ok_or_else(|| definition_error("recovery_order_account_not_bound"))?;
                let asset = binding
                    .tradable_assets
                    .iter()
                    .find(|a| a.local_asset_no == order.asset_no)
                    .ok_or_else(|| definition_error("recovery_order_asset_not_bound"))?;
                history_queries.push((
                    order.account_no,
                    order.asset_no,
                    order.order_id,
                    binding.account,
                    titan_account_service::AssetId(asset.asset_id),
                    crate::runtime_v13::client_order_id(handle, order.order_id),
                ));
            }
        }
        let execution_history = if history_queries.is_empty() {
            Vec::new()
        } else {
            let accounts = self.dependencies.accounts.clone();
            self.run_cold(deadline, move || {
                let executor = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| dependency_error("recovery_lookup_executor_failed"))?;
                executor.block_on(async {
                    tokio::time::timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        async {
                            let mut results = Vec::new();
                            for (local, local_asset, order_id, account, asset, client) in
                                history_queries
                            {
                                if let Some(proof) = accounts
                                    .execution_evidence(account, asset, client)
                                    .await
                                    .map_err(|error| {
                                        StrategyError::new(
                                            StrategyErrorKind::DependencyUnavailable,
                                            "recovery",
                                            "recovery_execution_history_failed",
                                            error.to_string(),
                                        )
                                    })?
                                {
                                    if proof.order.client_order_id != client
                                        || proof.order.asset_id != asset
                                        || proof.fills.len() > 4096
                                        || proof.fills.iter().any(|f| {
                                            f.client_order_id != client
                                                || f.asset_id != asset.0
                                                || f.side != proof.order.side
                                                || f.venue_order_id != proof.order.venue_order_id
                                        })
                                    {
                                        return Err(definition_error(
                                            "recovery_execution_history_identity_changed",
                                        ));
                                    }
                                    results.push((
                                        local,
                                        proof.order,
                                        RecoveryExecutionHistory {
                                            account_no: local,
                                            asset_no: local_asset,
                                            order_id,
                                            fills: proof.fills.into(),
                                        },
                                    ));
                                }
                            }
                            Ok(results)
                        },
                    )
                    .await
                    .map_err(|_| load_timeout("recovery_execution_history_deadline"))?
                })
            })?
        };
        let mut histories = Vec::new();
        for (local, order, history) in execution_history {
            if let Some((_, prior)) = recovered
                .iter()
                .find(|(a, o)| *a == local && o.client_order_id == order.client_order_id)
            {
                if *prior != order {
                    return Err(definition_error("recovery_order_changed_during_history"));
                }
            } else {
                recovered.push((local, order));
            }
            histories.push(history);
        }
        let mut initial_positions = Vec::new();
        let mut initial_balances = Vec::new();
        let mut initial_accounts = Vec::new();
        let mut initial_active_orders = Vec::new();
        let mut initial_order_boundaries = Vec::new();
        let mut initial_recovery_orders = Vec::new();
        for binding in resolved_accounts.iter() {
            let captured = self
                .dependencies
                .accounts
                .recovery_snapshot(binding.account)
                .map_err(|_| dependency_error("account_recovery_snapshot_unstable"))?;
            let orders = captured.orders;
            let positions = captured.positions;
            let balances = captured.balances;
            if orders.state != AccountSnapshotState::Ready
                || positions.state != AccountSnapshotState::Ready
                || balances.state != AccountSnapshotState::Ready
            {
                return Err(dependency_error("account_snapshot_not_ready"));
            }
            let epoch = positions
                .committed_epoch
                .ok_or_else(|| dependency_error("account_snapshot_epoch_missing"))?;
            let version = positions
                .committed_version
                .ok_or_else(|| dependency_error("account_snapshot_version_missing"))?;
            let order_epoch = orders
                .committed_epoch
                .ok_or_else(|| dependency_error("order_snapshot_epoch_missing"))?;
            let order_version = orders
                .committed_version
                .ok_or_else(|| dependency_error("order_snapshot_version_missing"))?;
            for asset in binding.tradable_assets.iter() {
                let mut found = false;
                for position in positions
                    .items
                    .iter()
                    .filter(|position| position.asset_id.0 == asset.asset_id)
                {
                    found = true;
                    initial_positions.push(TitanPositionView {
                        asset_no: asset.local_asset_no,
                        account_no: binding.local_account_no,
                        position_side: position.position_side,
                        reserved: [0; 7],
                        qty_lots: position.quantity_lots,
                        average_price_ticks: position.entry_price_ticks,
                        realized_pnl_ticks: position.realized_pnl_units,
                        account_sequence: version,
                    });
                }
                if !found {
                    initial_positions.push(TitanPositionView {
                        asset_no: asset.local_asset_no,
                        account_no: binding.local_account_no,
                        account_sequence: version,
                        ..TitanPositionView::default()
                    });
                }
            }
            let balance_epoch = balances
                .committed_epoch
                .ok_or_else(|| dependency_error("balance_snapshot_epoch_missing"))?;
            let balance_version = balances
                .committed_version
                .ok_or_else(|| dependency_error("balance_snapshot_version_missing"))?;
            if epoch != order_epoch || epoch != balance_epoch {
                return Err(dependency_error("account_snapshot_epoch_mismatch"));
            }
            initial_balances.extend(balances.items.iter().map(|balance| TitanBalanceView {
                account_no: binding.local_account_no,
                currency_no: balance.currency_id.0,
                total_units: balance.wallet_units,
                available_units: balance.available_units,
                account_sequence: balance_version,
            }));
            initial_accounts.push(TitanAccountView {
                account_no: binding.local_account_no,
                account_epoch: epoch,
                account_sequence: version.max(balance_version).max(order_version),
                state: AccountLifecycle::Ready as u8,
                ..TitanAccountView::default()
            });
            let missing_results = recovered
                .iter()
                .filter(|(local, order)| {
                    *local == binding.local_account_no
                        && !orders
                            .items
                            .iter()
                            .any(|known| known.client_order_id == order.client_order_id)
                })
                .map(|(_, order)| order);
            for order in orders.items.iter().chain(missing_results) {
                let Some(asset) = binding
                    .tradable_assets
                    .iter()
                    .find(|asset| asset.asset_id == order.asset_id.0)
                else {
                    continue;
                };
                let order_id = v13_strategy_order_id(handle, order.client_order_id);
                if order_id == 0 {
                    continue;
                }
                let view = TitanActiveOrderView {
                    order_id,
                    asset_no: asset.local_asset_no,
                    account_no: binding.local_account_no,
                    price_ticks: order.price_ticks,
                    qty_lots: order.quantity_lots,
                    cumulative_filled_lots: order.filled_quantity_lots,
                    created_ts_ns: orders.captured_at,
                    updated_ts_ns: orders.captured_at,
                    account_sequence: order_version,
                    side: order.side,
                    order_type: account_order_type_to_v13(order.order_type)?,
                    time_in_force: account_tif_to_v13(order.time_in_force)?,
                    status: account_order_status_to_v13(order.status)?,
                    reduce_only: u8::from(order.reduce_only),
                    position_side: order.position_side,
                    ..TitanActiveOrderView::default()
                };
                initial_recovery_orders.push(view);
                if !matches!(view.status, 4 | 6 | 7 | 8) {
                    initial_active_orders.push(view);
                }
            }
            initial_order_boundaries.push((binding.local_account_no, order_version));
        }
        runtime.seed_public_state(StrategyPublicStateSeedV13 {
            positions: initial_positions.into(),
            balances: initial_balances.into(),
            accounts: initial_accounts.into(),
            active_orders: initial_active_orders.into(),
            order_boundaries: initial_order_boundaries.into(),
            recovery_orders: initial_recovery_orders.into(),
        })?;
        if let Some(checkpoint) = checkpoint {
            runtime.seed_recovery_executions(histories.into())?;
            runtime.restore_state(checkpoint)?;
        }
        // Event routing must exist before an upstream subscription can emit its initial image.
        // Each token is owned by the strategy resource scope, so replace/remove cannot leak a
        // venue subscription.
        for binding in resolved_markets.iter() {
            let mut kinds = Vec::new();
            for subscription in manifest.subscriptions.iter() {
                if !subscription.routing_keys.is_empty()
                    && !subscription
                        .routing_keys
                        .contains(&u64::from(binding.asset_id))
                {
                    continue;
                }
                let kind = match subscription.event_type.as_ref() {
                    DEPTH_BATCH_EVENT => Some(MarketDataKind::Depth),
                    TRADE_BATCH_EVENT => Some(MarketDataKind::Trades),
                    BBO_EVENT => Some(MarketDataKind::Bbo),
                    _ => None,
                };
                if let Some(kind) = kind
                    && !kinds.contains(&kind)
                {
                    kinds.push(kind);
                }
            }
            if kinds.is_empty() {
                continue;
            }
            let market_subscription = self
                .dependencies
                .markets
                .subscribe(
                    binding.source,
                    MarketSubscribeRequest {
                        asset_id: titan_market_service::AssetId(binding.asset_id),
                        kinds: kinds.clone().into(),
                    },
                )
                .map_err(|_| dependency_error("market_subscribe_failed"))?;
            let markets = self.dependencies.markets.clone();
            let source = binding.source;
            resources
                .handle()
                .register(
                    format!(
                        "market_subscription_{}_{}",
                        binding.local_market_no, binding.local_asset_no
                    ),
                    ClosureResource(Some(move || {
                        markets
                            .unsubscribe(source, market_subscription)
                            .map(|_| ())
                            .map_err(|error| {
                                titan_core_types::CoreError::new(
                                    titan_core_types::ErrorKind::ResourceReleaseFailed,
                                    titan_core_types::ComponentIdentity::new(
                                        "titan.strategy.runtime",
                                        "market-subscription",
                                    ),
                                    titan_core_types::ComponentState::Stopping,
                                    "market_unsubscribe",
                                    error.to_string(),
                                )
                            })
                    })),
                )
                .map_err(|_| {
                    StrategyError::new(
                        StrategyErrorKind::Internal,
                        "create",
                        "market_subscription_resource_failed",
                        "market subscription cleanup could not be registered",
                    )
                })?;
            if kinds.contains(&MarketDataKind::Depth) {
                let operation = self
                    .dependencies
                    .markets
                    .request_snapshot(
                        binding.source,
                        titan_market_service::AssetId(binding.asset_id),
                    )
                    .map_err(|_| dependency_error("market_snapshot_failed"))?;
                loop {
                    let snapshot = self
                        .dependencies
                        .markets
                        .operation(binding.source, operation)
                        .map_err(|_| dependency_error("market_snapshot_status_failed"))?;
                    match snapshot.state {
                        titan_market_service::OperationState::Succeeded => break,
                        titan_market_service::OperationState::Failed => {
                            return Err(dependency_error("market_snapshot_rejected"));
                        }
                        titan_market_service::OperationState::Pending
                            if Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        titan_market_service::OperationState::Pending => {
                            return Err(dependency_error("market_snapshot_timeout"));
                        }
                    }
                }
            }
        }
        loop {
            let health = resolved_markets
                .iter()
                .map(|binding| {
                    self.dependencies
                        .markets
                        .health(binding.source)
                        .map(|health| (binding, health))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| dependency_error("market_health_unavailable"))?;
            if health
                .iter()
                .all(|(_, health)| health.state == ConnectorHealth::Running)
            {
                break;
            }
            if Instant::now() >= readiness_deadline {
                let pending = health
                    .iter()
                    .filter(|(_, health)| health.state != ConnectorHealth::Running)
                    .map(|(binding, health)| {
                        format!(
                            "source_id={} state={:?} message={}",
                            binding.source.source_id.0, health.state, health.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(StrategyError::new(
                    StrategyErrorKind::DependencyUnavailable,
                    "readiness",
                    "market_not_ready",
                    format!("market sources did not become ready: {pending}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let supervisor_lane = lane.clone();
        let supervisor_activation = activation.clone();
        let supervisor_runtime = runtime.clone();
        let invalidated_flag = Arc::new(AtomicBool::new(false));
        let supervisor = self.scheduler.schedule(Duration::from_millis(1), move || {
                    let _ = supervisor_runtime.advance_lifecycle();
                    let state = supervisor_lane.health().state;
                    if matches!(
                        state,
                        SubscriberState::ResyncRequired | SubscriberState::Failed
                    ) {
                        supervisor_activation.close();
                        if !invalidated_flag.swap(true, Ordering::AcqRel) {
                            let reason: Arc<str> = if state == SubscriberState::ResyncRequired {
                                Arc::from("subscriber_resync_required")
                            } else {
                                Arc::from("subscriber_failed")
                            };
                            warn!(
                                runtime_strategy_no = handle.runtime_strategy_no.0,
                                generation = handle.generation,
                                lane_state = ?state,
                                reason = reason.as_ref(),
                                "strategy subscriber lane lost; runtime invalidated; recovery requires stop/replace",
                            );
                            let _ = supervisor_runtime.invalidate(reason);
                        }
                    } else if state == SubscriberState::Stopped {
                        supervisor_activation.close();
                    }
        });
        resources
            .handle()
            .register("subscriber_supervisor", supervisor)
            .map_err(|_| checkpoint_error("supervisor_registration_failed"))?;

        if let Some(interval) = definition.runtime.timer_interval {
            let timer_runtime = runtime.clone();
            let mut timer_error_reported = false;
            let timer =
                self.scheduler
                    .schedule(interval, move || match timer_runtime.fire_timer(1) {
                        Ok(()) => timer_error_reported = false,
                        Err(error) if !timer_error_reported => {
                            warn!(
                                reason_code = error.reason_code.as_ref(),
                                "strategy timer safe point could not be queued"
                            );
                            timer_error_reported = true;
                        }
                        Err(_) => {}
                    });
            resources
                .handle()
                .register("strategy_timer", timer)
                .map_err(|_| checkpoint_error("timer_registration_failed"))?;
        }
        Ok(Arc::new(StrategyEntry {
            handle,
            definition,
            manifest,
            compiled_strategy_digest,
            runtime,
            lane,
            activation,
            resources: Mutex::new(Some(resources)),
            cleanup_deadline,
        }))
    }

    fn run_cold<T, F>(&self, deadline: Instant, action: F) -> LocalResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> LocalResult<T> + Send + 'static,
    {
        let permit = self.cold_loads.acquire(deadline)?;
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("strategy-cold-load".to_owned())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
                    .map_err(|_| {
                        StrategyError::new(
                            StrategyErrorKind::CompileFailed,
                            "load",
                            "cold_load_panicked",
                            "strategy file loader panicked",
                        )
                    })
                    .and_then(|result| result);
                let _ = sender.send(result);
                drop(permit);
            })
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::LoadFailed,
                    "load",
                    "cold_worker_start_failed",
                    "strategy file cold worker could not be started",
                )
            })?;
        receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|_| load_timeout("cold_load_deadline"))?
    }

    fn resolve_markets(
        &self,
        definition: &StrategyDefinition,
    ) -> LocalResult<Vec<ResolvedMarketBinding>> {
        definition
            .markets
            .iter()
            .map(|binding| {
                let source = self
                    .dependencies
                    .markets
                    .resolve(&binding.source_key)
                    .map_err(|_| dependency_error("market_not_found"))?;
                let source_stream = self
                    .dependencies
                    .markets
                    .source_stream(source)
                    .map_err(|_| dependency_error("market_source_changed"))?;
                Ok(ResolvedMarketBinding {
                    source_stream,
                    local_market_no: binding.local_market_no,
                    local_asset_no: binding.local_asset_no,
                    source,
                    asset_id: binding.asset_id,
                    data_mode: binding.data_mode,
                })
            })
            .collect()
    }

    fn resolve_accounts(
        &self,
        definition: &StrategyDefinition,
    ) -> LocalResult<Vec<ResolvedAccountBinding>> {
        definition
            .accounts
            .iter()
            .map(|binding| {
                let account = self
                    .dependencies
                    .accounts
                    .resolve(&binding.account_key)
                    .map_err(|_| dependency_error("account_not_found"))?;
                Ok(ResolvedAccountBinding {
                    local_account_no: binding.local_account_no,
                    account,
                    tradable_assets: binding.tradable_assets.clone(),
                })
            })
            .collect()
    }

    fn ensure_ready(&self, entry: &StrategyEntry) -> LocalResult<()> {
        for binding in entry.definition.markets.iter() {
            let handle = self
                .dependencies
                .markets
                .resolve(&binding.source_key)
                .map_err(|_| dependency_error("market_unavailable"))?;
            if self
                .dependencies
                .markets
                .health(handle)
                .map_err(|_| dependency_error("market_health_unavailable"))?
                .state
                != ConnectorHealth::Running
            {
                return Err(dependency_error("market_not_ready"));
            }
        }
        for binding in entry.definition.accounts.iter() {
            let handle = self
                .dependencies
                .accounts
                .resolve(&binding.account_key)
                .map_err(|_| dependency_error("account_unavailable"))?;
            if self
                .dependencies
                .accounts
                .health(handle)
                .map_err(|_| dependency_error("account_health_unavailable"))?
                .state
                != AccountLifecycle::Ready
            {
                return Err(dependency_error("account_not_ready"));
            }
        }
        if entry.lane.health().state != SubscriberState::Normal {
            return Err(dependency_error("subscriber_not_normal"));
        }
        Ok(())
    }

    fn entry(&self, handle: StrategyHandle) -> LocalResult<Arc<StrategyEntry>> {
        let state = self
            .registry
            .state
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let entry = state
            .by_id
            .get(&handle.runtime_strategy_no)
            .ok_or_else(|| stale_error(handle))?;
        if entry.handle != handle {
            return Err(stale_error(handle));
        }
        Ok(entry.clone())
    }

    fn register_runtime_operation(
        &self,
        entry: Arc<StrategyEntry>,
        local: StrategyOperationId,
        cleanup: bool,
    ) -> StrategyOperationId {
        let id = StrategyOperationId(self.next_operation.fetch_add(1, Ordering::AcqRel));
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                GlobalOperation {
                    snapshot: StrategyOperationSnapshot {
                        id,
                        strategy: Some(entry.handle),
                        state: StrategyOperationState::Pending,
                        detail: Arc::from("pending"),
                    },
                    runtime: Some((entry.runtime.clone(), local)),
                    cleanup: cleanup.then_some(entry),
                },
            );
        id
    }

    fn completed_operation(
        &self,
        handle: Option<StrategyHandle>,
        detail: &'static str,
    ) -> StrategyOperationId {
        let id = StrategyOperationId(self.next_operation.fetch_add(1, Ordering::AcqRel));
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                GlobalOperation {
                    snapshot: StrategyOperationSnapshot {
                        id,
                        strategy: handle,
                        state: StrategyOperationState::Succeeded,
                        detail: Arc::from(detail),
                    },
                    runtime: None,
                    cleanup: None,
                },
            );
        id
    }
}

impl StrategyAdminService for StrategyServiceCore {
    fn create(&self, definition: StrategyDefinition) -> LocalResult<StrategyHandle> {
        self.ensure_accepting()?;
        if (definition.runtime.recovery_checkpoint_sha256.is_some()
            || definition.runtime.recovery_reference.is_some())
            && definition.recovery != StrategyRecoveryPolicy::RequireCheckpoint
        {
            return Err(checkpoint_error("pinned_checkpoint_requires_recovery"));
        }
        let checkpoint = self
            .checkpoints
            .as_ref()
            .map(|store| {
                if let Some(reference) = &definition.runtime.recovery_reference {
                    if reference.runtime_strategy_no != definition.runtime_strategy_no
                        || definition
                            .runtime
                            .recovery_checkpoint_sha256
                            .as_ref()
                            .is_some_and(|sha| sha != &reference.payload_sha256)
                    {
                        return Err(checkpoint_error("checkpoint_reference_changed"));
                    }
                    store.load_reference(reference).map(Some)
                } else {
                    store.load_pinned(
                        definition.runtime_strategy_no,
                        definition.runtime.recovery_checkpoint_sha256.as_deref(),
                    )
                }
            })
            .transpose()?
            .flatten();
        if definition.recovery == StrategyRecoveryPolicy::RequireCheckpoint && checkpoint.is_none()
        {
            return Err(checkpoint_error("required_checkpoint_missing"));
        }
        let minimum_generation = {
            let state = self
                .registry
                .state
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if state.by_id.contains_key(&definition.runtime_strategy_no)
                || state.by_key.contains_key(&definition.strategy_key)
            {
                return Err(StrategyError::new(
                    StrategyErrorKind::AlreadyExists,
                    "create",
                    "strategy_exists",
                    "strategy key or id already exists",
                ));
            }
            if state.by_id.len() >= self.config.max_strategy_runtimes {
                return Err(StrategyError::new(
                    StrategyErrorKind::CapacityExceeded,
                    "create",
                    "runtime_limit",
                    "strategy runtime limit reached",
                ));
            }
            let registry_generation = state
                .last_generation
                .get(&definition.runtime_strategy_no)
                .copied()
                .unwrap_or(0);
            let checkpoint_generation = checkpoint.as_ref().map_or(0, |value| value.generation);
            registry_generation.max(checkpoint_generation)
        };
        let generation = if let Some(store) = &self.checkpoints {
            store.reserve_generation(definition.runtime_strategy_no, minimum_generation)?
        } else {
            minimum_generation
                .checked_add(1)
                .ok_or_else(|| definition_error("generation_overflow"))?
        };
        let restore = match definition.recovery {
            StrategyRecoveryPolicy::Fresh => None,
            StrategyRecoveryPolicy::RestoreLatestCheckpoint
            | StrategyRecoveryPolicy::RequireCheckpoint => checkpoint,
        };
        let entry = self.create_entry(definition, generation, restore)?;
        let handle = entry.handle;
        let mut state = self
            .registry
            .state
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if state.by_id.contains_key(&handle.runtime_strategy_no)
            || state.by_key.contains_key(&entry.definition.strategy_key)
        {
            drop(state);
            entry.cleanup()?;
            return Err(StrategyError::new(
                StrategyErrorKind::AlreadyExists,
                "create",
                "strategy_race",
                "strategy was concurrently created",
            ));
        }
        state
            .last_generation
            .insert(handle.runtime_strategy_no, handle.generation);
        state
            .by_key
            .insert(entry.definition.strategy_key.clone(), handle);
        state.by_id.insert(handle.runtime_strategy_no, entry);
        Ok(handle)
    }

    fn prepare(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId> {
        self.ensure_accepting()?;
        let entry = self.entry(strategy)?;
        self.ensure_ready(&entry)?;
        let local = entry.runtime.prepare()?;
        Ok(self.register_runtime_operation(entry, local, false))
    }

    fn start(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId> {
        self.ensure_accepting()?;
        let entry = self.entry(strategy)?;
        if !entry.definition.enabled {
            return Err(definition_error("strategy_disabled"));
        }
        self.ensure_ready(&entry)?;
        let local = entry.runtime.start()?;
        Ok(self.register_runtime_operation(entry, local, false))
    }

    fn pause(
        &self,
        strategy: StrategyHandle,
        reason: PauseReason,
    ) -> LocalResult<StrategyOperationId> {
        let entry = self.entry(strategy)?;
        let local = entry.runtime.pause(reason)?;
        Ok(self.register_runtime_operation(entry, local, false))
    }

    fn resume(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId> {
        self.ensure_accepting()?;
        let entry = self.entry(strategy)?;
        self.ensure_ready(&entry)?;
        let local = entry.runtime.resume()?;
        Ok(self.register_runtime_operation(entry, local, false))
    }

    fn stop(
        &self,
        strategy: StrategyHandle,
        deadline: Instant,
    ) -> LocalResult<StrategyOperationId> {
        let entry = self.entry(strategy)?;
        if entry.runtime.state().lifecycle == StrategyLifecycle::Stopped {
            return Ok(self.completed_operation(Some(strategy), "already_stopped"));
        }
        let local = entry.runtime.stop(deadline)?;
        Ok(self.register_runtime_operation(entry, local, true))
    }

    fn replace(
        &self,
        strategy: StrategyHandle,
        mut definition: StrategyDefinition,
    ) -> LocalResult<StrategyHandle> {
        self.ensure_accepting()?;
        let old = self.entry(strategy)?;
        if definition.runtime_strategy_no != strategy.runtime_strategy_no
            || definition.strategy_key != old.definition.strategy_key
        {
            return Err(definition_error("replace_identity_changed"));
        }
        if definition.run_config_revision <= old.definition.run_config_revision {
            return Err(definition_error("run_config_revision_not_increased"));
        }
        let generation = if let Some(store) = &self.checkpoints {
            store.reserve_generation(strategy.runtime_strategy_no, strategy.generation)?
        } else {
            strategy
                .generation
                .checked_add(1)
                .ok_or_else(|| definition_error("generation_overflow"))?
        };
        definition.runtime_strategy_no = strategy.runtime_strategy_no;
        let candidate = self.create_entry(definition, generation, None)?;
        if let Err(error) = self.ensure_ready(&candidate) {
            candidate.cleanup()?;
            return Err(error);
        }
        let prepared = match candidate.runtime.prepare() {
            Ok(operation) => operation,
            Err(error) => {
                candidate.cleanup()?;
                return Err(error);
            }
        };
        if let Err(error) = wait_runtime(
            &candidate.runtime,
            prepared,
            Instant::now() + candidate.definition.runtime.startup_timeout,
        ) {
            candidate.cleanup()?;
            return Err(error);
        }
        old.activation.close();
        let start = match candidate.runtime.start() {
            Ok(operation) => operation,
            Err(error) => {
                reopen_old_if_running(&old);
                candidate.cleanup()?;
                return Err(error);
            }
        };
        if let Err(error) = wait_runtime(
            &candidate.runtime,
            start,
            Instant::now() + candidate.definition.runtime.startup_timeout,
        ) {
            reopen_old_if_running(&old);
            let _ = candidate
                .runtime
                .stop(Instant::now() + candidate.definition.runtime.stop_timeout);
            candidate.cleanup()?;
            return Err(error);
        }
        {
            let mut state = self
                .registry
                .state
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let current = state
                .by_id
                .get(&strategy.runtime_strategy_no)
                .ok_or_else(|| stale_error(strategy))?;
            if current.handle != strategy {
                drop(state);
                let _ = candidate
                    .runtime
                    .stop(Instant::now() + candidate.definition.runtime.stop_timeout);
                candidate.cleanup()?;
                reopen_old_if_running(&old);
                return Err(stale_error(strategy));
            }
            state
                .by_key
                .insert(candidate.definition.strategy_key.clone(), candidate.handle);
            state
                .by_id
                .insert(strategy.runtime_strategy_no, candidate.clone());
            state
                .last_generation
                .insert(strategy.runtime_strategy_no, generation);
        }
        let retirement_deadline = Instant::now() + old.definition.runtime.stop_timeout;
        if let Ok(stop) = old.runtime.stop(retirement_deadline) {
            let _ = wait_runtime(&old.runtime, stop, retirement_deadline);
        }
        // Registry replacement is the irreversible commit point. Retirement failures are
        // contained to the old, gate-closed generation and must not report the cutover as failed.
        let _ = old.cleanup();
        Ok(candidate.handle)
    }

    fn remove(&self, strategy: StrategyHandle) -> LocalResult<StrategyOperationId> {
        let entry = self.entry(strategy)?;
        if entry.runtime.state().lifecycle != StrategyLifecycle::Stopped {
            return Err(StrategyError::new(
                StrategyErrorKind::InvalidState,
                "remove",
                "runtime_not_stopped",
                "strategy must be stopped before removal",
            ));
        }
        entry.cleanup()?;
        let mut state = self
            .registry
            .state
            .write()
            .unwrap_or_else(|p| p.into_inner());
        state.by_id.remove(&strategy.runtime_strategy_no);
        state.by_key.remove(&entry.definition.strategy_key);
        Ok(self.completed_operation(Some(strategy), "removed"))
    }

    fn list(&self) -> Arc<[StrategyInstanceSnapshot]> {
        self.registry
            .state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .values()
            .map(|entry| StrategyInstanceSnapshot {
                handle: entry.handle,
                strategy_key: entry.definition.strategy_key.clone(),
                run_config_revision: entry.definition.run_config_revision,
                compiled_strategy_digest: entry.compiled_strategy_digest,
                lifecycle: entry.runtime.state().lifecycle,
                enabled: entry.definition.enabled,
            })
            .collect::<Vec<_>>()
            .into()
    }

    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot {
        let mut operations = self.operations.lock().unwrap_or_else(|p| p.into_inner());
        let Some(operation) = operations.get_mut(&id) else {
            return StrategyOperationSnapshot {
                id,
                strategy: None,
                state: StrategyOperationState::Failed,
                detail: Arc::from("unknown_operation"),
            };
        };
        if let Some((runtime, local)) = &operation.runtime {
            let current = runtime.operation(*local);
            operation.snapshot.state = current.state;
            operation.snapshot.detail = current.detail;
            if current.state != StrategyOperationState::Pending {
                operation.runtime = None;
                if current.state == StrategyOperationState::Succeeded
                    && let Some(entry) = operation.cleanup.take()
                    && let Err(error) = entry.cleanup()
                {
                    operation.snapshot.state = StrategyOperationState::Failed;
                    operation.snapshot.detail = error.reason_code;
                }
            }
        }
        operation.snapshot.clone()
    }
}

impl StrategyService for StrategyServiceCore {
    fn execution_risk(
        &self,
        strategy: StrategyHandle,
    ) -> LocalResult<Vec<titan_account_service::ExecutionRiskEvidence>> {
        self.entry(strategy)?.runtime.execution_risk()
    }
    fn pause_supported(&self, strategy: StrategyHandle) -> LocalResult<bool> {
        Ok(self.entry(strategy)?.runtime.pause_supported())
    }
    fn release_safe(&self, strategy: StrategyHandle) -> LocalResult<bool> {
        Ok(self.entry(strategy)?.runtime.release_safe())
    }
    fn recovery_status(&self, strategy: StrategyHandle) -> LocalResult<serde_json::Value> {
        self.entry(strategy)?.runtime.recovery_status()
    }
    fn business_observation(
        &self,
        strategy: StrategyHandle,
    ) -> LocalResult<BusinessObservationSnapshot> {
        self.entry(strategy)?.runtime.business_observation()
    }
    fn resolve(&self, strategy_key: &str) -> LocalResult<StrategyHandle> {
        self.registry
            .state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_key
            .get(strategy_key)
            .copied()
            .ok_or_else(|| {
                StrategyError::new(
                    StrategyErrorKind::InvalidDefinition,
                    "resolve",
                    "strategy_not_found",
                    "strategy key was not found",
                )
            })
    }
    fn state(&self, strategy: StrategyHandle) -> LocalResult<StrategyRuntimeStateSnapshot> {
        Ok(self.entry(strategy)?.runtime.state())
    }
    fn health(&self, strategy: StrategyHandle) -> LocalResult<StrategyRuntimeHealthSnapshot> {
        Ok(self.entry(strategy)?.runtime.health())
    }
    fn diagnostics(
        &self,
        strategy: StrategyHandle,
    ) -> LocalResult<StrategyRuntimeDiagnosticSnapshot> {
        Ok(self.entry(strategy)?.runtime.diagnostics())
    }
}

pub(crate) fn validate_definition(
    config: &StrategyCoreConfig,
    definition: &StrategyDefinition,
) -> LocalResult<()> {
    if !definition.accounts.is_empty()
        && config.recovery_store.is_none()
        && config.checkpoint_root.is_none()
    {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "definition",
            "execution_requires_checkpoint_root",
            "strategies with account execution require a durable checkpoint root",
        ));
    }
    if definition.recovery != StrategyRecoveryPolicy::Fresh
        && config.recovery_store.is_none()
        && config.checkpoint_root.is_none()
    {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "definition",
            "recovery_unavailable",
            "strategy recovery requires a configured checkpoint root",
        ));
    }
    if definition.strategy_key.is_empty()
        || definition.runtime_strategy_no.0 == 0
        || definition.run_config_revision == 0
    {
        return Err(definition_error("missing_identity"));
    }
    if definition.strategy_file.expected_digest == [0; 32] {
        return Err(definition_error("unpinned_compiled_strategy_digest"));
    }
    if !config
        .allowed_loader_types
        .contains(&definition.strategy_file.loader_type)
    {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "definition",
            "loader_not_allowed",
            "strategy loader is not authorized",
        ));
    }
    let runtime = &definition.runtime;
    if config
        .recovery_store
        .as_ref()
        .and_then(|s| s.minimum_handler_budget())
        .is_some_and(|budget| runtime.max_handler_duration < budget)
    {
        return Err(definition_error("recovery_handler_budget_too_small"));
    }
    if runtime.async_lane_capacity == 0
        || runtime.critical_reserve >= runtime.async_lane_capacity
        || runtime.async_lane_capacity > config.max_lane_capacity
        || runtime.reliable_pending_capacity == 0
        || runtime.reliable_pending_capacity > config.max_pending_capacity
        || runtime.timer_capacity == 0
        || runtime.timer_capacity > config.max_pending_capacity
        || runtime.max_commands_per_callback == 0
        || runtime.max_commands_per_callback > config.max_pending_capacity
        || runtime.checkpoint_interval.is_zero()
        || runtime
            .timer_interval
            .is_some_and(|interval| interval.is_zero())
        || runtime.max_handler_duration.is_zero()
        || !config
            .allowed_worker_policies
            .contains(&runtime.worker_policy)
        || (runtime.worker_policy == titan_event_engine::SubscriberRuntimeMode::Dedicated
            && runtime.cpu_affinity.is_none())
    {
        return Err(definition_error("runtime_capacity_or_policy"));
    }
    contiguous(
        definition
            .markets
            .iter()
            .map(|binding| binding.local_market_no),
        "market_binding_gap",
    )?;
    contiguous(
        definition
            .markets
            .iter()
            .map(|binding| binding.local_asset_no),
        "market_asset_binding_gap",
    )?;
    contiguous(
        definition
            .accounts
            .iter()
            .map(|binding| binding.local_account_no),
        "account_binding_gap",
    )?;
    let market_assets = definition
        .markets
        .iter()
        .map(|binding| (binding.local_asset_no, binding.asset_id))
        .collect::<BTreeMap<_, _>>();
    for account in definition.accounts.iter() {
        let mut seen = BTreeSet::new();
        for asset in account.tradable_assets.iter() {
            if !seen.insert(asset.local_asset_no) {
                return Err(definition_error("duplicate_account_asset"));
            }
            if market_assets.get(&asset.local_asset_no) != Some(&asset.asset_id) {
                return Err(definition_error("account_market_asset_mismatch"));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_manifest(
    config: &StrategyCoreConfig,
    definition: &StrategyDefinition,
    manifest: &CompiledStrategyMetadata,
) -> LocalResult<()> {
    if manifest.compiled_strategy_digest != definition.strategy_file.expected_digest {
        return Err(StrategyError::new(
            StrategyErrorKind::DigestMismatch,
            "inspect",
            "manifest_digest_mismatch",
            "manifest digest does not match the pinned definition",
        ));
    }
    if manifest.runtime_abi.major != STRATEGY_ABI_V13 as u16 {
        return Err(StrategyError::new(
            StrategyErrorKind::AbiMismatch,
            "inspect",
            "runtime_abi_mismatch",
            "strategy file requires an incompatible runtime ABI",
        ));
    }
    if manifest.subscriptions.is_empty() {
        return Err(definition_error("strategy_subscriptions_empty"));
    }
    if manifest.state_byte_len == 0 || manifest.state_byte_len > config.max_state_capacity {
        return Err(definition_error("strategy_recovery_state_capacity"));
    }
    for subscription in manifest.subscriptions.iter() {
        if !supported_strategy_subscription(
            subscription.event_type.as_ref(),
            subscription.schema_version,
        ) {
            return Err(definition_error("strategy_subscription_unsupported"));
        }
        if !config.allowed_event_types.is_empty()
            && !config
                .allowed_event_types
                .contains(&(subscription.event_type.clone(), subscription.schema_version))
        {
            return Err(definition_error("strategy_event_not_allowed"));
        }
    }
    let unavailable = StrategyCapabilities(StrategyCapabilities::AMEND_ORDER.0);
    if manifest.capabilities.contains(unavailable) {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "inspect",
            "command_capability_unavailable",
            "timer scheduling and order amendment are not implemented in the current strategy runtime profile",
        ));
    }
    if !config.allowed_capabilities.contains(manifest.capabilities) {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "inspect",
            "package_capability_not_allowed",
            "strategy capability exceeds service authorization",
        ));
    }
    // V13 parameters are normalized and baked into the signed compiled_strategy. Deployment-time
    // definitions cannot override them or their schema.
    Ok(())
}

fn contiguous(values: impl Iterator<Item = u32>, code: &'static str) -> LocalResult<()> {
    let mut values = values.collect::<Vec<_>>();
    let original_len = values.len();
    values.sort_unstable();
    values.dedup();
    if values.len() != original_len
        || values
            .iter()
            .copied()
            .enumerate()
            .any(|(index, value)| index as u32 != value)
    {
        return Err(definition_error(code));
    }
    Ok(())
}

fn wait_runtime(
    runtime: &Arc<dyn StrategyRuntime>,
    operation: StrategyOperationId,
    deadline: Instant,
) -> LocalResult<()> {
    loop {
        match runtime.operation(operation).state {
            StrategyOperationState::Succeeded => return Ok(()),
            StrategyOperationState::Failed => {
                return Err(StrategyError::new(
                    StrategyErrorKind::InvalidState,
                    "runtime_operation",
                    "operation_failed",
                    "runtime operation failed",
                ));
            }
            StrategyOperationState::Pending if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1))
            }
            StrategyOperationState::Pending => {
                return Err(StrategyError::new(
                    StrategyErrorKind::StopTimeout,
                    "runtime_operation",
                    "operation_timeout",
                    "runtime operation deadline expired",
                ));
            }
        }
    }
}

fn reopen_old_if_running(entry: &StrategyEntry) {
    if entry.runtime.state().lifecycle == StrategyLifecycle::Running {
        entry.activation.open();
    }
}

fn definition_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::InvalidDefinition,
        "definition",
        code,
        "strategy definition is invalid",
    )
}
fn dependency_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::DependencyUnavailable,
        "readiness",
        code,
        "strategy dependency is not ready",
    )
}
fn load_timeout(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::LoadFailed,
        "load",
        code,
        "strategy file load deadline expired",
    )
}
fn stale_error(handle: StrategyHandle) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::StaleHandle,
        "registry",
        "stale_handle",
        "strategy handle is stale",
    )
    .for_handle(handle)
}

struct SystemStrategyClock;
impl StrategyClock for SystemStrategyClock {
    fn now_ns(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(i64::MAX as u128) as i64
    }
}
struct NoopStrategyMetrics;
impl StrategyMetrics for NoopStrategyMetrics {}

/// Canonical EventEngine facts that `CanonicalStrategyEventAdapter` can translate into typed
/// Strategy ABI views today.
///
/// Event types that the Account/Market services publish but the adapter cannot yet decode
/// (FundingRate, MarkPrice and unsupported control facts)
/// must be rejected here instead of falling through to a heuristic callback mapping.
///
/// Bar/Hybrid is not part of the runnable V13 profile until one canonical producer and backtest
/// adapter exist. Rejecting it at the service boundary prevents compiled_strategies from compiling into a
/// profile that the runtime cannot dispatch.
pub fn supported_strategy_subscription(event_type: &str, schema_version: u32) -> bool {
    matches!(
        (event_type, schema_version),
        (
            DEPTH_BATCH_EVENT | TRADE_BATCH_EVENT | BBO_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
        ) | (ORDER_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION)
            | (FILL_EVENT, FILL_EVENT_SCHEMA_VERSION)
            | (POSITION_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION)
            | (BALANCE_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION)
            | (
                ACCOUNT_STREAM_STATE_CHANGED_EVENT,
                ACCOUNT_EVENT_SCHEMA_VERSION
            )
            | (
                ACCOUNT_STREAM_INVALIDATED_EVENT,
                ACCOUNT_EVENT_SCHEMA_VERSION
            )
            // HOST_DEPTH_100 strategies have this market subscription added by the
            // compiler, so the service boundary must accept it as well.
            | (
                MARKET_STREAM_INVALIDATED_EVENT,
                MARKET_EVENT_SCHEMA_VERSION
            )
    )
}

// Host accounting and epoch fencing is independent of strategy callback selection.
fn add_host_account_subscriptions(
    subscriptions: &mut Vec<PrimarySubscriptionSpec>,
    accounts: &[ResolvedAccountBinding],
) {
    if !accounts.is_empty() {
        let account_keys: Arc<[u64]> = accounts
            .iter()
            .map(|binding| u64::from(binding.account.account_id.0))
            .collect::<Vec<_>>()
            .into();
        for event_type in [
            ORDER_CHANGED_EVENT,
            FILL_EVENT,
            POSITION_CHANGED_EVENT,
            BALANCE_CHANGED_EVENT,
            ACCOUNT_STREAM_STATE_CHANGED_EVENT,
            ACCOUNT_STREAM_INVALIDATED_EVENT,
        ] {
            if let Some(spec) = subscriptions
                .iter_mut()
                .find(|spec| spec.event_type.as_ref() == event_type)
            {
                let mut keys = spec.routing_keys.to_vec();
                for key in account_keys.iter().copied() {
                    if !keys.contains(&key) {
                        keys.push(key);
                    }
                }
                keys.sort_unstable();
                spec.routing_keys = keys.into();
                continue;
            }
            subscriptions.push(PrimarySubscriptionSpec {
                event_type: Arc::from(event_type),
                schema_version: if event_type == FILL_EVENT {
                    FILL_EVENT_SCHEMA_VERSION
                } else {
                    ACCOUNT_EVENT_SCHEMA_VERSION
                },
                qos: titan_core_types::EventQos::ReliableOrdered,
                routing_keys: account_keys.clone(),
            });
        }
    }
}

#[cfg(test)]
mod host_subscription_tests {
    use super::*;

    #[test]
    fn host_depth_invalidation_subscription_is_supported() {
        // The compiler adds titan.market.StreamInvalidated for HOST_DEPTH_100.
        assert!(supported_strategy_subscription(
            titan_market_service::STREAM_INVALIDATED_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
        ));
        assert!(!supported_strategy_subscription(
            titan_market_service::STREAM_INVALIDATED_EVENT,
            MARKET_EVENT_SCHEMA_VERSION + 1,
        ));
    }

    #[test]
    fn bbo_only_strategy_receives_all_host_account_facts() {
        let mut subscriptions = vec![PrimarySubscriptionSpec {
            event_type: Arc::from(BBO_EVENT),
            schema_version: 1,
            qos: titan_core_types::EventQos::Latest,
            routing_keys: Arc::from([42]),
        }];
        let bindings = [ResolvedAccountBinding {
            local_account_no: 0,
            account: titan_account_service::AccountHandle {
                account_id: titan_account_service::AccountId(8),
                generation: 1,
            },
            tradable_assets: Arc::from([]),
        }];
        add_host_account_subscriptions(&mut subscriptions, &bindings);
        assert_eq!(subscriptions.len(), 7);
        for event_type in [
            ORDER_CHANGED_EVENT,
            FILL_EVENT,
            POSITION_CHANGED_EVENT,
            BALANCE_CHANGED_EVENT,
            ACCOUNT_STREAM_STATE_CHANGED_EVENT,
            ACCOUNT_STREAM_INVALIDATED_EVENT,
        ] {
            let spec = subscriptions
                .iter()
                .find(|s| s.event_type.as_ref() == event_type)
                .unwrap();
            assert_eq!(spec.routing_keys.as_ref(), &[8]);
            assert_eq!(spec.qos, titan_core_types::EventQos::ReliableOrdered);
            assert_eq!(
                spec.schema_version,
                if event_type == FILL_EVENT {
                    FILL_EVENT_SCHEMA_VERSION
                } else {
                    ACCOUNT_EVENT_SCHEMA_VERSION
                }
            );
        }
        add_host_account_subscriptions(&mut subscriptions, &bindings);
        assert_eq!(subscriptions.len(), 7);
        assert_eq!(subscriptions[0].routing_keys.as_ref(), &[42]);
    }
}

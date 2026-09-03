use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use titan_account_plugin::{AccountExecutionService, AccountLifecycle, AccountService};
use titan_event_engine::{
    EventEngineHandle, PrimaryAsyncLaneConfig, PrimaryAsyncLaneHandle, PrimarySubscriptionSpec,
    SubscriberState,
};
use titan_market_plugin::{ConnectorHealth, MarketService};
use titan_plugin_engine::{ClosureResource, ResourceScope, ServiceId, ServiceKey, ServiceScope};

use crate::*;

#[derive(Clone)]
pub struct StrategyPluginConfig {
    pub max_strategy_runtimes: usize,
    pub max_artifact_cache_entries: usize,
    pub allowed_loader_types: BTreeSet<Arc<str>>,
    pub allowed_artifact_roots: Arc<[Arc<str>]>,
    pub allowed_event_types: BTreeSet<(Arc<str>, u32)>,
    pub allowed_worker_policies: Vec<titan_event_engine::SubscriberRuntimeMode>,
    pub allowed_capabilities: StrategyCapabilities,
    pub max_lane_capacity: usize,
    pub max_pending_capacity: usize,
    pub max_command_capacity: usize,
    pub max_state_capacity: usize,
    pub max_concurrent_loads: usize,
}

impl Default for StrategyPluginConfig {
    fn default() -> Self {
        Self {
            max_strategy_runtimes: 128,
            max_artifact_cache_entries: 64,
            allowed_loader_types: [Arc::from("numba-python"), Arc::from("rust-static")]
                .into_iter()
                .collect(),
            allowed_artifact_roots: Arc::from([]),
            allowed_event_types: BTreeSet::new(),
            allowed_worker_policies: vec![
                titan_event_engine::SubscriberRuntimeMode::Dedicated,
                titan_event_engine::SubscriberRuntimeMode::SpinSleep,
                titan_event_engine::SubscriberRuntimeMode::Park,
            ],
            allowed_capabilities: StrategyCapabilities(
                u64::MAX
                    & !(StrategyCapabilities::READ_RISK.0
                        | StrategyCapabilities::CHECKPOINT_STATE.0),
            ),
            max_lane_capacity: 1 << 20,
            max_pending_capacity: 1 << 18,
            max_command_capacity: 4_096,
            max_state_capacity: 1 << 20,
            max_concurrent_loads: 2,
        }
    }
}

#[derive(Clone)]
pub struct StrategyPluginDependencies {
    pub events: EventEngineHandle,
    pub markets: Arc<dyn MarketService>,
    pub accounts: Arc<dyn AccountService>,
    pub execution: Arc<dyn AccountExecutionService>,
}

pub struct StrategyEntry {
    pub handle: StrategyHandle,
    pub definition: StrategyDefinition,
    pub manifest: StrategyPackageManifest,
    pub artifact_id: StrategyArtifactId,
    pub runtime: Arc<dyn StrategyRuntime>,
    pub lane: PrimaryAsyncLaneHandle,
    pub gateway: Arc<dyn StrategyCommandGateway>,
    pub activation: Arc<StrategyActivationGate>,
    pub command_gate: Arc<StrategyCommandGate>,
    resources: Mutex<Option<ResourceScope>>,
}

struct DisabledSnapshotSink;

impl StrategyStateSnapshotSink for DisabledSnapshotSink {
    fn submit(&self, _snapshot: StrategyPrivateStateSnapshot) -> LocalResult<()> {
        Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "state_snapshot",
            "state_snapshot_unavailable",
            "strategy state snapshots are not part of the current runtime profile",
        ))
    }
}

impl StrategyEntry {
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
    by_id: HashMap<StrategyId, Arc<StrategyEntry>>,
    by_key: HashMap<Arc<str>, StrategyHandle>,
    last_generation: HashMap<StrategyId, u64>,
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

pub struct StrategyPluginCore {
    config: StrategyPluginConfig,
    dependencies: StrategyPluginDependencies,
    loaders: Arc<StrategyPackageLoaderRegistry>,
    runtimes: Arc<StrategyRuntimeFactoryRegistry>,
    cache: StrategyArtifactCache,
    registry: StrategyRegistry,
    accepting: AtomicBool,
    next_operation: AtomicU64,
    operations: Mutex<BTreeMap<StrategyOperationId, GlobalOperation>>,
    cold_loads: Arc<ColdLoadLimiter>,
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

impl StrategyPluginCore {
    pub fn new(
        config: StrategyPluginConfig,
        dependencies: StrategyPluginDependencies,
        loaders: Arc<StrategyPackageLoaderRegistry>,
        runtimes: Arc<StrategyRuntimeFactoryRegistry>,
    ) -> LocalResult<Self> {
        if config.max_strategy_runtimes == 0 {
            return Err(definition_error("max_strategy_runtimes_zero"));
        }
        if config.max_concurrent_loads == 0 {
            return Err(definition_error("max_concurrent_loads_zero"));
        }
        let cache = StrategyArtifactCache::new(config.max_artifact_cache_entries)?;
        let max_concurrent_loads = config.max_concurrent_loads;
        Ok(Self {
            config,
            dependencies,
            loaders,
            runtimes,
            cache,
            registry: StrategyRegistry::default(),
            accepting: AtomicBool::new(true),
            next_operation: AtomicU64::new(1),
            operations: Mutex::new(BTreeMap::new()),
            cold_loads: Arc::new(ColdLoadLimiter {
                active: Mutex::new(0),
                changed: Condvar::new(),
                capacity: max_concurrent_loads,
            }),
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
                let local = entry.runtime.stop(deadline)?;
                wait_runtime(&entry.runtime, local, deadline)
            })();
            let cleanup_result = entry.cleanup();
            if first_error.is_none() {
                first_error = runtime_result.err().or_else(|| cleanup_result.err());
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
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
                "plugin_quiescing",
                "strategy plugin is not accepting operations",
            ))
        }
    }

    fn create_entry(
        &self,
        definition: StrategyDefinition,
        generation: u64,
    ) -> LocalResult<Arc<StrategyEntry>> {
        validate_definition(&self.config, &definition)?;
        let handle = StrategyHandle {
            strategy_id: definition.strategy_id,
            generation,
        };
        let loader = self.loaders.create(
            &definition.package.loader_type,
            StrategyLoaderContext {
                allowed_artifact_roots: self.config.allowed_artifact_roots.clone(),
                require_signature: false,
            },
        )?;
        let deadline = Instant::now() + definition.runtime.startup_timeout;
        let inspect_loader = loader.clone();
        let inspect_package = definition.package.clone();
        let manifest = self.run_cold(deadline, move || inspect_loader.inspect(&inspect_package))?;
        validate_manifest(&self.config, &definition, &manifest)?;
        let abi = titan_runtime::runtime_abi_descriptor();
        let key = ArtifactCacheKey {
            artifact_digest: definition.package.expected_digest,
            entrypoint: definition.entrypoint.clone(),
            normalized_parameters_digest: normalized_parameters_digest(&definition.parameters)?,
            runtime_abi_fingerprint: Arc::from(abi.fingerprint.as_str()),
            target_cpu: Arc::from(std::env::consts::ARCH),
        };
        let artifact = if let Some(value) = self.cache.get(
            &key,
            definition.runtime.state_f64_capacity,
            definition.runtime.state_i64_capacity,
        ) {
            value
        } else {
            let request = StrategyLoadRequest {
                package: definition.package.clone(),
                entrypoint: definition.entrypoint.clone(),
                parameters: definition.parameters.clone(),
                runtime_abi_fingerprint: Arc::from(abi.fingerprint.as_str()),
                target_cpu: Arc::from(std::env::consts::ARCH),
            };
            let load_loader = loader.clone();
            let loaded = self.run_cold(deadline, move || load_loader.load(request, deadline))?;
            if loaded.id.digest != definition.package.expected_digest {
                return Err(StrategyError::new(
                    StrategyErrorKind::DigestMismatch,
                    "load",
                    "artifact_digest_mismatch",
                    "loaded artifact digest does not match the pinned definition",
                ));
            }
            let instance = loaded.clone_for_instance(
                definition.runtime.state_f64_capacity,
                definition.runtime.state_i64_capacity,
            );
            self.cache.insert(key, loaded);
            instance
        };
        validate_manifest(&self.config, &definition, &artifact.manifest)?;
        if artifact.manifest != manifest {
            return Err(StrategyError::new(
                StrategyErrorKind::DigestMismatch,
                "load",
                "manifest_changed_during_load",
                "loaded artifact manifest does not match the inspected manifest",
            ));
        }
        let artifact_id = artifact.id;
        let resolved_markets = self.resolve_markets(&definition)?;
        let resolved_accounts = self.resolve_accounts(&definition)?;
        let activation = Arc::new(StrategyActivationGate::default());
        let command_gate = Arc::new(StrategyCommandGate::new(handle));
        let gateway: Arc<dyn StrategyCommandGateway> =
            Arc::new(StandardStrategyCommandGateway::new(
                handle,
                manifest.capabilities,
                command_gate.clone(),
                &resolved_accounts,
                self.dependencies.execution.clone(),
            ));
        let resources = ResourceScope::new(titan_plugin_engine::PluginIdentity::new(
            "titan.strategy.runtime",
            format!("{}-{}", definition.strategy_id.0, generation),
        ));
        let event_adapter: Arc<dyn StrategyEventAdapter> =
            Arc::new(CanonicalStrategyEventAdapter::new(&resolved_markets));
        let context = StrategyRuntimeBuildContext {
            strategy: handle,
            artifact_id: artifact.id,
            markets: resolved_markets.into(),
            accounts: resolved_accounts.into(),
            event_adapter,
            command_gateway: gateway.clone(),
            state_snapshot_sink: Arc::new(DisabledSnapshotSink),
            clock: Arc::new(SystemStrategyClock),
            metrics: Arc::new(NoopStrategyMetrics),
            resources: resources.handle(),
            activation: activation.clone(),
            command_gate: command_gate.clone(),
        };
        let factory = self.runtimes.get(&manifest.strategy_type)?;
        let runtime = factory.create(&definition, artifact, context)?;
        let subscriptions = definition
            .subscriptions
            .iter()
            .map(|spec| PrimarySubscriptionSpec {
                event_type: spec.event_type.clone(),
                schema_version: spec.schema_version,
                qos: spec.qos,
                routing_keys: spec.routing_keys.clone(),
            })
            .collect::<Vec<_>>();
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
                    control_capacity: 64,
                    runtime_mode: definition.runtime.worker_policy,
                    spin_iterations: 256,
                    idle_sleep: Duration::from_micros(10),
                    cpu_affinity: definition.runtime.cpu_affinity,
                },
                runtime.clone(),
            )
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::RouteFailed,
                    "create",
                    "primary_lane_failed",
                    "PRIMARY async lane registration failed",
                )
            })?;
        let events = self.dependencies.events.clone();
        let token = lane.token();
        let lane_stop_timeout = definition.runtime.stop_timeout;
        resources
            .handle()
            .register(
                "primary_async_lane",
                ClosureResource(Some(move || {
                    events
                        .unregister_primary_async_lane_before(
                            token,
                            Instant::now() + lane_stop_timeout,
                        )
                        .map(|_| ())
                        .map_err(|error| {
                            titan_plugin_engine::PluginError::new(
                                titan_plugin_engine::ErrorKind::ResourceReleaseFailed,
                                titan_plugin_engine::PluginIdentity::new(
                                    "titan.strategy.runtime",
                                    "primary-lane",
                                ),
                                titan_plugin_engine::LifecycleState::Stopping,
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
        let supervisor_active = Arc::new(AtomicBool::new(true));
        let supervisor_flag = supervisor_active.clone();
        let supervisor_lane = lane.clone();
        let supervisor_command_gate = command_gate.clone();
        let supervisor_activation = activation.clone();
        let supervisor = std::thread::Builder::new()
            .name(format!(
                "strategy-supervisor-{}-{}",
                handle.strategy_id.0, handle.generation
            ))
            .spawn(move || {
                while supervisor_flag.load(Ordering::Acquire) {
                    if matches!(
                        supervisor_lane.health().state,
                        SubscriberState::ResyncRequired
                            | SubscriberState::Failed
                            | SubscriberState::Stopped
                    ) {
                        supervisor_command_gate.close();
                        supervisor_activation.close();
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::Internal,
                    "create",
                    "supervisor_start_failed",
                    "strategy supervisor could not be started",
                )
            })?;
        resources
            .handle()
            .register(
                "subscriber_supervisor",
                StrategySupervisorResource {
                    active: supervisor_active,
                    join: Some(supervisor),
                },
            )
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::Internal,
                    "create",
                    "supervisor_registration_failed",
                    "strategy supervisor cleanup could not be registered",
                )
            })?;
        Ok(Arc::new(StrategyEntry {
            handle,
            definition,
            manifest,
            artifact_id,
            runtime,
            lane,
            gateway,
            activation,
            command_gate,
            resources: Mutex::new(Some(resources)),
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
                            "strategy package loader panicked",
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
                    "strategy package cold worker could not be started",
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
                Ok(ResolvedMarketBinding {
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
            .get(&handle.strategy_id)
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

impl StrategyAdminService for StrategyPluginCore {
    fn create(&self, definition: StrategyDefinition) -> LocalResult<StrategyHandle> {
        self.ensure_accepting()?;
        let generation = {
            let state = self
                .registry
                .state
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if state.by_id.contains_key(&definition.strategy_id)
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
            state
                .last_generation
                .get(&definition.strategy_id)
                .copied()
                .unwrap_or(0)
                + 1
        };
        let entry = self.create_entry(definition, generation)?;
        let handle = entry.handle;
        let mut state = self
            .registry
            .state
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if state.by_id.contains_key(&handle.strategy_id)
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
            .insert(handle.strategy_id, handle.generation);
        state
            .by_key
            .insert(entry.definition.strategy_key.clone(), handle);
        state.by_id.insert(handle.strategy_id, entry);
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
        let local = entry.runtime.stop(deadline)?;
        if entry.definition.shutdown == StrategyShutdownPolicy::CancelOwnedOrders {
            entry.gateway.cancel_owned_orders(strategy)?;
        }
        Ok(self.register_runtime_operation(entry, local, true))
    }

    fn replace(
        &self,
        strategy: StrategyHandle,
        mut definition: StrategyDefinition,
    ) -> LocalResult<StrategyHandle> {
        self.ensure_accepting()?;
        let old = self.entry(strategy)?;
        if definition.strategy_id != strategy.strategy_id
            || definition.strategy_key != old.definition.strategy_key
        {
            return Err(definition_error("replace_identity_changed"));
        }
        if definition.definition_version <= old.definition.definition_version {
            return Err(definition_error("definition_version_not_increased"));
        }
        let generation = strategy
            .generation
            .checked_add(1)
            .ok_or_else(|| definition_error("generation_overflow"))?;
        definition.strategy_id = strategy.strategy_id;
        let candidate = self.create_entry(definition, generation)?;
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
        old.command_gate.close();
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
                .get(&strategy.strategy_id)
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
            state.by_id.insert(strategy.strategy_id, candidate.clone());
            state
                .last_generation
                .insert(strategy.strategy_id, generation);
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
        state.by_id.remove(&strategy.strategy_id);
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
                definition_version: entry.definition.definition_version,
                artifact_id: entry.artifact_id,
                lifecycle: entry.runtime.state().lifecycle,
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
                {
                    if let Err(error) = entry.cleanup() {
                        operation.snapshot.state = StrategyOperationState::Failed;
                        operation.snapshot.detail = error.reason_code;
                    }
                }
            }
        }
        operation.snapshot.clone()
    }
}

impl StrategyService for StrategyPluginCore {
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
    config: &StrategyPluginConfig,
    definition: &StrategyDefinition,
) -> LocalResult<()> {
    if definition.recovery != StrategyRecoveryPolicy::Fresh {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "definition",
            "recovery_plugin_unavailable",
            "only fresh strategy startup is supported until a recovery plugin is installed",
        ));
    }
    if definition.strategy_key.is_empty()
        || definition.strategy_id.0 == 0
        || definition.entrypoint.is_empty()
        || definition.definition_version == 0
    {
        return Err(definition_error("missing_identity"));
    }
    if definition.package.expected_digest == [0; 32] {
        return Err(definition_error("unpinned_artifact_digest"));
    }
    if !config
        .allowed_loader_types
        .contains(&definition.package.loader_type)
    {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "definition",
            "loader_not_allowed",
            "strategy loader is not authorized",
        ));
    }
    let runtime = &definition.runtime;
    if runtime.async_lane_capacity == 0
        || runtime.critical_reserve >= runtime.async_lane_capacity
        || runtime.async_lane_capacity > config.max_lane_capacity
        || runtime.reliable_pending_capacity == 0
        || runtime.reliable_pending_capacity > config.max_pending_capacity
        || runtime.command_capacity == 0
        || runtime.command_capacity > config.max_command_capacity
        || runtime.timer_capacity == 0
        || runtime.callback_budget.soft_budget.is_zero()
        || runtime.callback_budget.stall_threshold < runtime.callback_budget.soft_budget
        || runtime.callback_budget.max_consecutive_violations == 0
        || runtime.state_f64_capacity > config.max_state_capacity
        || runtime.state_i64_capacity > config.max_state_capacity
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
    if definition.subscriptions.is_empty() {
        return Err(definition_error("subscriptions_empty"));
    }
    for subscription in definition.subscriptions.iter() {
        if !config.allowed_event_types.is_empty()
            && !config
                .allowed_event_types
                .contains(&(subscription.event_type.clone(), subscription.schema_version))
        {
            return Err(StrategyError::new(
                StrategyErrorKind::UnsupportedCapability,
                "definition",
                "event_not_allowed",
                "strategy subscription is not authorized",
            ));
        }
        let critical_account_fact = matches!(
            subscription.event_type.as_ref(),
            titan_account_plugin::ORDER_CHANGED_EVENT
                | titan_account_plugin::FILL_EVENT
                | titan_account_plugin::POSITION_CHANGED_EVENT
                | titan_account_plugin::BALANCE_CHANGED_EVENT
                | titan_account_plugin::COMMAND_RESULT_EVENT
        );
        if critical_account_fact
            && subscription.qos != titan_plugin_engine::EventQos::ReliableOrdered
        {
            return Err(definition_error("critical_account_qos"));
        }
        if subscription.event_type.as_ref() == titan_account_plugin::FILL_EVENT
            && subscription.schema_version != titan_account_plugin::FILL_EVENT_SCHEMA_VERSION
        {
            return Err(definition_error("fill_schema_version"));
        }
    }
    Ok(())
}

fn validate_manifest(
    config: &StrategyPluginConfig,
    definition: &StrategyDefinition,
    manifest: &StrategyPackageManifest,
) -> LocalResult<()> {
    if manifest.artifact_digest != definition.package.expected_digest {
        return Err(StrategyError::new(
            StrategyErrorKind::DigestMismatch,
            "inspect",
            "manifest_digest_mismatch",
            "manifest digest does not match the pinned definition",
        ));
    }
    if manifest.runtime_abi.major != titan_runtime_abi::STRATEGY_ABI_VERSION as u16 {
        return Err(StrategyError::new(
            StrategyErrorKind::AbiMismatch,
            "inspect",
            "runtime_abi_mismatch",
            "strategy package requires an incompatible runtime ABI",
        ));
    }
    if !config.allowed_capabilities.contains(manifest.capabilities) {
        return Err(StrategyError::new(
            StrategyErrorKind::UnsupportedCapability,
            "inspect",
            "package_capability_not_allowed",
            "package capability exceeds plugin authorization",
        ));
    }
    if manifest.parameter_schema_version != definition.parameter_schema_version {
        return Err(StrategyError::new(
            StrategyErrorKind::ParameterInvalid,
            "inspect",
            "parameter_schema_version",
            "parameter schema version does not match",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&definition.parameters)
        .map_err(|_| definition_error("parameter_json"))?;
    let validator = jsonschema::validator_for(&manifest.parameter_schema)
        .map_err(|_| definition_error("parameter_schema_invalid"))?;
    if validator.validate(&value).is_err() {
        return Err(StrategyError::new(
            StrategyErrorKind::ParameterInvalid,
            "inspect",
            "parameter_schema_rejected",
            "strategy parameters do not match the package schema",
        ));
    }
    for subscription in definition.subscriptions.iter() {
        let required = if subscription.event_type.contains("Depth") {
            StrategyCapabilities::READ_DEPTH
        } else if subscription.event_type.contains("Bar") {
            StrategyCapabilities::READ_BAR
        } else if subscription.event_type.starts_with("titan.account") {
            StrategyCapabilities::READ_ACCOUNT
        } else {
            StrategyCapabilities::READ_TICK
        };
        if !manifest.capabilities.contains(required) {
            return Err(StrategyError::new(
                StrategyErrorKind::UnsupportedCapability,
                "inspect",
                "subscription_capability_missing",
                "package did not declare a required read capability",
            ));
        }
    }
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
        entry.command_gate.open();
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
        "strategy package load deadline expired",
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

struct StrategySupervisorResource {
    active: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl titan_plugin_engine::Resource for StrategySupervisorResource {
    fn close(&mut self) -> Result<(), titan_plugin_engine::PluginError> {
        self.active.store(false, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        Ok(())
    }
}

pub const STRATEGY_PLUGIN_TYPE: &str = "titan.strategy";

pub static STRATEGY_PLUGIN_MANIFEST: std::sync::LazyLock<titan_plugin_engine::PluginManifest> =
    std::sync::LazyLock::new(|| {
        use semver::{Version, VersionReq};
        use titan_plugin_engine::*;
        let mut subscribes = Vec::new();
        for event in titan_market_plugin::MARKET_EVENT_TYPES {
            subscribes.push(SubscribedEvent {
                event_type: Arc::from(event),
                schema_version: titan_market_plugin::MARKET_EVENT_SCHEMA_VERSION,
                allowed_qos: [
                    EventQos::Latest,
                    EventQos::ReliableOrdered,
                    EventQos::BestEffort,
                ]
                .into_iter()
                .collect(),
            });
        }
        for event in titan_account_plugin::ACCOUNT_EVENT_TYPES {
            subscribes.push(SubscribedEvent {
                event_type: Arc::from(event),
                schema_version: if event == titan_account_plugin::FILL_EVENT {
                    titan_account_plugin::FILL_EVENT_SCHEMA_VERSION
                } else {
                    titan_account_plugin::ACCOUNT_EVENT_SCHEMA_VERSION
                },
                allowed_qos: [
                    EventQos::Latest,
                    EventQos::ReliableOrdered,
                    EventQos::BestEffort,
                ]
                .into_iter()
                .collect(),
            });
        }
        PluginManifest {
            plugin_type: Arc::from(STRATEGY_PLUGIN_TYPE),
            name: Arc::from("Titan Strategy Plugin"),
            version: Version::new(1, 0, 0),
            engine_api_version: CORE_RUNTIME_API_VERSION,
            abi_version: ApiVersion::new(1, 0),
            config_schema_version: 1,
            config_schema: Arc::new(serde_json::json!({"type":"object"})),
            provides: vec![
                ProvidedService {
                    id: ServiceId::new("titan.strategy", "admin"),
                    version: Version::new(1, 0, 0),
                    scope_kind: ScopeKind::Global,
                    call_mode: CallMode::Command,
                },
                ProvidedService {
                    id: ServiceId::new("titan.strategy", "query"),
                    version: Version::new(1, 0, 0),
                    scope_kind: ScopeKind::Global,
                    call_mode: CallMode::Inline,
                },
            ],
            requires: vec![
                RequiredService {
                    id: ServiceId::new("titan.market", "market"),
                    version: VersionReq::parse("^1").unwrap(),
                    scope_kind: ScopeKind::Global,
                    required: true,
                },
                RequiredService {
                    id: ServiceId::new("titan.account", "query"),
                    version: VersionReq::parse("^1").unwrap(),
                    scope_kind: ScopeKind::Global,
                    required: true,
                },
                RequiredService {
                    id: ServiceId::new("titan.account", "execution"),
                    version: VersionReq::parse("^1").unwrap(),
                    scope_kind: ScopeKind::Global,
                    required: true,
                },
                RequiredService {
                    id: ServiceId::new("titan.metrics", "sink"),
                    version: VersionReq::parse("^1").unwrap(),
                    scope_kind: ScopeKind::Global,
                    required: false,
                },
            ],
            publishes: [
                "StateChanged",
                "HealthChanged",
                "CallbackFault",
                "OperationCompleted",
            ]
            .into_iter()
            .map(|name| PublishedEvent {
                event_type: Arc::from(format!("titan.strategy.{name}")),
                schema_version: 1,
            })
            .collect(),
            subscribes,
            supported_execution_models: [ExecutionModel::Dedicated].into_iter().collect(),
            reload_policy: ReloadPolicy::WhenQuiescent,
        }
    });

pub struct StrategyPluginFactory {
    config: StrategyPluginConfig,
    dependency_source: StrategyDependencySource,
    loaders: Arc<StrategyPackageLoaderRegistry>,
    runtimes: Arc<StrategyRuntimeFactoryRegistry>,
}

enum StrategyDependencySource {
    Direct(StrategyPluginDependencies),
    PluginServices(EventEngineHandle),
}

impl StrategyPluginFactory {
    pub fn new(config: StrategyPluginConfig, dependencies: StrategyPluginDependencies) -> Self {
        Self {
            config,
            dependency_source: StrategyDependencySource::Direct(dependencies),
            loaders: Arc::new(StrategyPackageLoaderRegistry::default()),
            runtimes: Arc::new(StrategyRuntimeFactoryRegistry::default()),
        }
    }

    /// Builds a production factory whose Market/Account dependencies are bound from the
    /// PluginEngine plan during lifecycle start. No RPC or global service locator is involved.
    pub fn from_plugin_services(config: StrategyPluginConfig, events: EventEngineHandle) -> Self {
        Self {
            config,
            dependency_source: StrategyDependencySource::PluginServices(events),
            loaders: Arc::new(StrategyPackageLoaderRegistry::default()),
            runtimes: Arc::new(StrategyRuntimeFactoryRegistry::default()),
        }
    }

    pub fn with_loader(self, loader: Arc<dyn StrategyPackageLoaderFactory>) -> LocalResult<Self> {
        self.loaders.register(loader)?;
        Ok(self)
    }

    pub fn with_runtime_factory(
        self,
        runtime: Arc<dyn StrategyRuntimeFactory>,
    ) -> LocalResult<Self> {
        self.runtimes.register(runtime)?;
        Ok(self)
    }
}

impl titan_plugin_engine::PluginFactory for StrategyPluginFactory {
    fn manifest(&self) -> &'static titan_plugin_engine::PluginManifest {
        &STRATEGY_PLUGIN_MANIFEST
    }

    fn create(
        &self,
        _init: titan_plugin_engine::PluginInit,
    ) -> Result<titan_plugin_engine::PluginBundle, titan_plugin_engine::PluginError> {
        use titan_plugin_engine::*;
        let (dependencies, service_bindings) = match &self.dependency_source {
            StrategyDependencySource::Direct(dependencies) => (dependencies.clone(), None),
            StrategyDependencySource::PluginServices(events) => {
                let bindings = Arc::new(PluginStrategyServices::default());
                (
                    StrategyPluginDependencies {
                        events: events.clone(),
                        markets: bindings.clone(),
                        accounts: bindings.clone(),
                        execution: bindings.clone(),
                    },
                    Some(bindings),
                )
            }
        };
        let core = Arc::new(
            StrategyPluginCore::new(
                self.config.clone(),
                dependencies,
                self.loaders.clone(),
                self.runtimes.clone(),
            )
            .map_err(strategy_plugin_error)?,
        );
        let admin: Arc<dyn StrategyAdminService> = core.clone();
        let query: Arc<dyn StrategyService> = core.clone();
        Ok(PluginBundle {
            lifecycle: Box::new(StrategyPluginLifecycle {
                core,
                service_bindings,
            }),
            service_exports: vec![
                ServiceExport {
                    service_key: ServiceKey {
                        id: ServiceId::new("titan.strategy", "admin"),
                        version: semver::Version::new(1, 0, 0),
                        scope: ServiceScope::Global,
                    },
                    endpoint: boxed_typed_endpoint::<StrategyAdminApi>(Arc::new(
                        StrategyAdminEndpoint(admin),
                    )),
                },
                ServiceExport {
                    service_key: ServiceKey {
                        id: ServiceId::new("titan.strategy", "query"),
                        version: semver::Version::new(1, 0, 0),
                        scope: ServiceScope::Global,
                    },
                    endpoint: boxed_typed_endpoint::<StrategyQueryApi>(Arc::new(
                        StrategyQueryEndpoint(query),
                    )),
                },
            ],
            subscription_bindings: vec![],
        })
    }
}

struct StrategyPluginLifecycle {
    core: Arc<StrategyPluginCore>,
    service_bindings: Option<Arc<PluginStrategyServices>>,
}

impl titan_plugin_engine::Plugin for StrategyPluginLifecycle {
    fn validate(
        &self,
        context: &titan_plugin_engine::ValidationContext,
    ) -> Result<(), titan_plugin_engine::PluginError> {
        if self.service_bindings.is_some() {
            context
                .services
                .require::<titan_market_plugin::MarketApi>(&ServiceKey {
                    id: ServiceId::new("titan.market", "market"),
                    version: semver::Version::new(1, 0, 0),
                    scope: ServiceScope::Global,
                })?;
            context
                .services
                .require::<titan_account_plugin::AccountApi>(&ServiceKey {
                    id: ServiceId::new("titan.account", "query"),
                    version: semver::Version::new(1, 0, 0),
                    scope: ServiceScope::Global,
                })?;
            context
                .services
                .require::<titan_account_plugin::AccountExecutionApi>(&ServiceKey {
                    id: ServiceId::new("titan.account", "execution"),
                    version: semver::Version::new(1, 0, 0),
                    scope: ServiceScope::Global,
                })?;
        }
        Ok(())
    }
    fn start(
        &mut self,
        context: &mut titan_plugin_engine::PluginContext,
    ) -> Result<(), titan_plugin_engine::PluginError> {
        if let Some(bindings) = &self.service_bindings {
            bindings.bind(&context.services)?;
        }
        Ok(())
    }
    fn quiesce(
        &mut self,
        _: titan_plugin_engine::StopReason,
    ) -> Result<(), titan_plugin_engine::PluginError> {
        self.core
            .quiesce(Instant::now() + Duration::from_secs(10))
            .map_err(strategy_plugin_error)
    }
    fn stop(&mut self) -> Result<(), titan_plugin_engine::PluginError> {
        if let Some(bindings) = &self.service_bindings {
            bindings.clear();
        }
        Ok(())
    }
}

fn strategy_plugin_error(error: StrategyError) -> titan_plugin_engine::PluginError {
    titan_plugin_engine::PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        titan_plugin_engine::PluginIdentity::new(STRATEGY_PLUGIN_TYPE, "strategy-provider"),
        titan_plugin_engine::LifecycleState::Running,
        "strategy_plugin",
        error.to_string(),
    )
}

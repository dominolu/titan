use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use semver::Version;
use titan_plugin_engine::{
    ApiVersion, CallMode, ConfigSnapshot, EventPublisher, ExecutionModel, Plugin, PluginBundle,
    PluginContext, PluginError, PluginFactory, PluginIdentity, PluginInit, PluginManifest,
    ProvidedService, PublishedEvent, ReloadPolicy, ResourceScope, ScopeKind, ServiceExport,
    ServiceId, ServiceKey, ServiceScope, StopReason, ValidationContext, boxed_typed_endpoint,
};

use crate::{
    AdminEndpoint, AssetId, ConnectorEntry, ConnectorLifecycle, ConnectorOperationSnapshot,
    ConnectorRegistry, LocalResult, MARKET_EVENT_SCHEMA_VERSION, MARKET_EVENT_TYPES,
    MarketAdminApi, MarketAdminService, MarketApi, MarketConnectorContext, MarketConnectorFactory,
    MarketEndpoint, MarketError, MarketErrorKind, MarketEventPublisher, MarketOperationSnapshot,
    MarketService, MarketSourceDefinition, MarketSourceHandle, MarketSourceId,
    MarketSourceSnapshot, MarketSubscribeRequest, MarketSubscription, OperationId, OperationState,
    SourceStreamId, connector_error,
};

pub const MARKET_PLUGIN_TYPE: &str = "titan.market";

pub static MARKET_PLUGIN_MANIFEST: LazyLock<PluginManifest> = LazyLock::new(|| PluginManifest {
    plugin_type: Arc::from(MARKET_PLUGIN_TYPE),
    name: Arc::from("Titan Market Plugin"),
    version: Version::new(1, 0, 0),
    engine_api_version: titan_plugin_engine::CORE_RUNTIME_API_VERSION,
    abi_version: ApiVersion::new(1, 0),
    config_schema: Arc::new(serde_json::json!({"type":"object"})),
    provides: vec![
        ProvidedService {
            id: ServiceId::new("titan.market", "admin"),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        },
        ProvidedService {
            id: ServiceId::new("titan.market", "market"),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        },
    ],
    requires: vec![],
    publishes: MARKET_EVENT_TYPES
        .iter()
        .map(|event_type| PublishedEvent {
            event_type: Arc::from(*event_type),
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
        })
        .collect(),
    subscribes: vec![],
    supported_execution_models: [ExecutionModel::Passive].into_iter().collect(),
    reload_policy: ReloadPolicy::WhenQuiescent,
});

#[derive(Clone, Copy, Debug)]
pub struct MarketPluginConfig {
    pub max_sources: usize,
    pub max_instruments: usize,
    pub stop_timeout: Duration,
}

impl Default for MarketPluginConfig {
    fn default() -> Self {
        Self {
            max_sources: 16,
            max_instruments: 4096,
            stop_timeout: Duration::from_secs(5),
        }
    }
}

impl MarketPluginConfig {
    fn from_snapshot(snapshot: &ConfigSnapshot) -> Result<Self, PluginError> {
        let section = snapshot
            .value
            .get("market_plugin")
            .unwrap_or(snapshot.value.as_ref());
        let mut config = Self::default();
        if let Some(value) = section.get("max_sources").and_then(|value| value.as_u64()) {
            config.max_sources = value as usize;
        }
        if let Some(value) = section
            .get("max_instruments")
            .and_then(|value| value.as_u64())
        {
            config.max_instruments = value as usize;
        }
        if config.max_sources == 0 || config.max_instruments == 0 {
            return Err(PluginError::new(
                titan_plugin_engine::ErrorKind::ConfigInvalid,
                PluginIdentity::new(MARKET_PLUGIN_TYPE, "market"),
                titan_plugin_engine::LifecycleState::Discovered,
                "market_config",
                "capacities must be non-zero",
            ));
        }
        Ok(config)
    }
}

struct RuntimeBindings {
    identity: PluginIdentity,
    publisher: EventPublisher,
}

pub struct MarketPluginCore {
    config: MarketPluginConfig,
    factories: RwLock<HashMap<Arc<str>, Arc<dyn MarketConnectorFactory>>>,
    registry: ConnectorRegistry,
    runtime: RwLock<Option<RuntimeBindings>>,
    next_operation_id: AtomicU64,
    operations: RwLock<HashMap<OperationId, MarketOperationSnapshot>>,
    accepting: AtomicBool,
    mutation: Mutex<()>,
}

impl MarketPluginCore {
    pub fn new(config: MarketPluginConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            factories: RwLock::new(HashMap::new()),
            registry: ConnectorRegistry::default(),
            runtime: RwLock::new(None),
            next_operation_id: AtomicU64::new(1),
            operations: RwLock::new(HashMap::new()),
            accepting: AtomicBool::new(false),
            mutation: Mutex::new(()),
        })
    }

    pub fn register_factory(&self, factory: Arc<dyn MarketConnectorFactory>) -> LocalResult<()> {
        let connector_type: Arc<str> = Arc::from(factory.connector_type());
        if connector_type.trim().is_empty() {
            return Err(MarketError::new(
                MarketErrorKind::InvalidDefinition,
                "empty connector type",
            ));
        }
        let mut factories = self.factories.write().unwrap_or_else(|p| p.into_inner());
        if factories.contains_key(&connector_type) {
            return Err(MarketError::new(
                MarketErrorKind::AlreadyExists,
                format!("factory {connector_type} already registered"),
            ));
        }
        factories.insert(connector_type, factory);
        Ok(())
    }

    pub fn activate(&self, identity: PluginIdentity, publisher: EventPublisher) {
        *self.runtime.write().unwrap_or_else(|p| p.into_inner()) = Some(RuntimeBindings {
            identity,
            publisher,
        });
        self.accepting.store(true, Ordering::Release);
    }

    fn ensure_accepting(&self) -> LocalResult<()> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(MarketError::new(
                MarketErrorKind::RuntimeNotActive,
                "market plugin is not accepting requests",
            ))
        }
    }

    fn next_operation(&self, state: OperationState, detail: impl Into<Arc<str>>) -> OperationId {
        let id = OperationId(self.next_operation_id.fetch_add(1, Ordering::Relaxed));
        self.operations
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                MarketOperationSnapshot {
                    id,
                    state,
                    detail: detail.into(),
                },
            );
        id
    }

    fn build_entry(
        &self,
        definition: MarketSourceDefinition,
        source_id: MarketSourceId,
        generation: u64,
    ) -> LocalResult<Arc<ConnectorEntry>> {
        let factory = self
            .factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&definition.connector_type)
            .cloned()
            .ok_or_else(|| {
                MarketError::new(
                    MarketErrorKind::FactoryNotFound,
                    format!("factory {} is not registered", definition.connector_type),
                )
            })?;
        let runtime = self.runtime.read().unwrap_or_else(|p| p.into_inner());
        let runtime = runtime.as_ref().ok_or_else(|| {
            MarketError::new(MarketErrorKind::RuntimeNotActive, "plugin has not started")
        })?;
        let handle = MarketSourceHandle {
            source_id,
            generation,
        };
        let market_stream = SourceStreamId(source_id.0.checked_mul(2).ok_or_else(|| {
            MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source stream id overflow",
            )
        })?);
        let control_stream = SourceStreamId(market_stream.0.checked_add(1).ok_or_else(|| {
            MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source stream id overflow",
            )
        })?);
        let resources = ResourceScope::new(runtime.identity.clone());
        let context = MarketConnectorContext {
            source: handle,
            instruments: definition.instruments.clone(),
            market_source_stream: market_stream,
            control_source_stream: control_stream,
            event_publisher: MarketEventPublisher::new(
                runtime.publisher.clone(),
                market_stream,
                control_stream,
            ),
            resources: resources.handle(),
        };
        let connector = factory
            .create(&definition, context)
            .map_err(|error| connector_error("create", error))?;
        Ok(Arc::new(ConnectorEntry::new(
            handle, definition, connector, resources,
        )))
    }

    pub fn quiesce_all(&self, deadline: Instant) -> LocalResult<()> {
        self.accepting.store(false, Ordering::Release);
        let mut failures = Vec::new();
        for entry in self.registry.list_entries() {
            if matches!(
                entry.lifecycle(),
                ConnectorLifecycle::Running
                    | ConnectorLifecycle::Starting
                    | ConnectorLifecycle::Failed
            ) {
                entry.set_lifecycle(ConnectorLifecycle::Stopping);
                if let Err(error) = entry.connector.stop(deadline) {
                    failures.push(error.to_string());
                    entry.set_lifecycle(ConnectorLifecycle::Failed);
                } else {
                    entry.set_lifecycle(ConnectorLifecycle::Stopped);
                }
            }
            if let Err(error) = entry.close_resources() {
                failures.push(error.to_string());
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(MarketError::new(
                MarketErrorKind::ResourceReleaseFailed,
                failures.join("; "),
            ))
        }
    }

    pub fn shutdown(&self) -> LocalResult<()> {
        self.accepting.store(false, Ordering::Release);
        let entries = self.registry.list_entries();
        let mut failures = Vec::new();
        for entry in entries {
            if let Err(error) = self.registry.remove(entry.handle) {
                failures.push(error.to_string());
            }
            if let Err(error) = entry.close_resources() {
                failures.push(error.to_string());
            }
        }
        *self.runtime.write().unwrap_or_else(|p| p.into_inner()) = None;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(MarketError::new(
                MarketErrorKind::ResourceReleaseFailed,
                failures.join("; "),
            ))
        }
    }
}

impl MarketAdminService for MarketPluginCore {
    fn create(&self, definition: MarketSourceDefinition) -> LocalResult<MarketSourceHandle> {
        self.ensure_accepting()?;
        let _guard = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        self.registry.validate_insert(
            &definition,
            self.config.max_sources,
            self.config.max_instruments,
            None,
        )?;
        let (source_id, generation) = self
            .registry
            .allocate_identity(&definition.source_key, self.config.max_sources)?;
        let entry = self.build_entry(definition, source_id, generation)?;
        let handle = entry.handle;
        self.registry.insert(entry)?;
        Ok(handle)
    }

    fn start(&self, source: MarketSourceHandle) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        let entry = self.registry.get(source)?;
        if matches!(
            entry.lifecycle(),
            ConnectorLifecycle::Running | ConnectorLifecycle::Starting
        ) {
            return Err(MarketError::new(
                MarketErrorKind::AlreadyExists,
                "connector is already running",
            ));
        }
        entry.set_lifecycle(ConnectorLifecycle::Starting);
        match entry.connector.start() {
            Ok(()) => {
                entry.set_lifecycle(ConnectorLifecycle::Running);
                Ok(self.next_operation(OperationState::Succeeded, "connector started"))
            }
            Err(error) => {
                entry.set_lifecycle(ConnectorLifecycle::Failed);
                let id = self.next_operation(OperationState::Failed, error.message.clone());
                Err(connector_error(&format!("start operation {}", id.0), error))
            }
        }
    }

    fn stop(&self, source: MarketSourceHandle, deadline: Instant) -> LocalResult<OperationId> {
        let entry = self.registry.get(source)?;
        entry.set_lifecycle(ConnectorLifecycle::Stopping);
        let stop_result = entry.connector.stop(deadline);
        let resource_result = entry.close_resources();
        match (stop_result, resource_result) {
            (Ok(()), Ok(())) => {
                entry.set_lifecycle(ConnectorLifecycle::Stopped);
                Ok(self.next_operation(OperationState::Succeeded, "connector stopped"))
            }
            (stop, resources) => {
                entry.set_lifecycle(ConnectorLifecycle::Failed);
                let mut details = Vec::new();
                if let Err(error) = stop {
                    details.push(error.to_string());
                }
                if let Err(error) = resources {
                    details.push(error.to_string());
                }
                let kind = if Instant::now() >= deadline {
                    MarketErrorKind::DeadlineExceeded
                } else {
                    MarketErrorKind::ResourceReleaseFailed
                };
                Err(MarketError::new(kind, details.join("; ")))
            }
        }
    }

    fn remove(&self, source: MarketSourceHandle) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        let _guard = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let entry = self.registry.get(source)?;
        if matches!(
            entry.lifecycle(),
            ConnectorLifecycle::Running
                | ConnectorLifecycle::Starting
                | ConnectorLifecycle::Stopping
        ) {
            return Err(MarketError::new(
                MarketErrorKind::ConnectorRejected,
                "stop connector before removal",
            ));
        }
        let entry = self.registry.remove(source)?;
        entry.close_resources()?;
        Ok(self.next_operation(OperationState::Succeeded, "connector removed"))
    }

    fn replace(
        &self,
        source: MarketSourceHandle,
        definition: MarketSourceDefinition,
    ) -> LocalResult<MarketSourceHandle> {
        self.ensure_accepting()?;
        let _guard = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let old = self.registry.get(source)?;
        if old.definition.source_key != definition.source_key {
            return Err(MarketError::new(
                MarketErrorKind::InvalidDefinition,
                "replace must preserve source_key",
            ));
        }
        self.registry.validate_insert(
            &definition,
            self.config.max_sources,
            self.config.max_instruments,
            Some(source.source_id),
        )?;
        let generation = source.generation.saturating_add(1);
        let new_entry = self.build_entry(definition, source.source_id, generation)?;
        if old.lifecycle() == ConnectorLifecycle::Running {
            new_entry.set_lifecycle(ConnectorLifecycle::Starting);
            new_entry
                .connector
                .start()
                .map_err(|error| connector_error("replace start", error))?;
            new_entry.set_lifecycle(ConnectorLifecycle::Running);
        }
        let handle = new_entry.handle;
        let old = self.registry.swap(source, new_entry)?;
        let mut cleanup_failures = Vec::new();
        if matches!(
            old.lifecycle(),
            ConnectorLifecycle::Running | ConnectorLifecycle::Starting
        ) {
            if let Err(error) = old
                .connector
                .stop(Instant::now() + self.config.stop_timeout)
            {
                cleanup_failures.push(error.to_string());
            }
        }
        if let Err(error) = old.close_resources() {
            cleanup_failures.push(error.to_string());
        }
        if !cleanup_failures.is_empty() {
            tracing::warn!(
                source_id = source.source_id.0,
                generation = source.generation,
                errors = %cleanup_failures.join("; "),
                "replacement committed but old connector cleanup was incomplete"
            );
        }
        Ok(handle)
    }

    fn list(&self) -> Arc<[MarketSourceSnapshot]> {
        self.registry.list()
    }
    fn operation(&self, id: OperationId) -> MarketOperationSnapshot {
        self.operations
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned()
            .unwrap_or(MarketOperationSnapshot {
                id,
                state: OperationState::Failed,
                detail: Arc::from("operation not found"),
            })
    }
}

impl MarketService for MarketPluginCore {
    fn resolve(&self, source_key: &str) -> LocalResult<MarketSourceHandle> {
        self.registry.resolve(source_key)
    }
    fn subscribe(
        &self,
        source: MarketSourceHandle,
        request: MarketSubscribeRequest,
    ) -> LocalResult<MarketSubscription> {
        self.ensure_accepting()?;
        self.registry
            .get(source)?
            .connector
            .subscribe(request)
            .map_err(|error| connector_error("subscribe", error))
    }
    fn unsubscribe(
        &self,
        source: MarketSourceHandle,
        subscription: MarketSubscription,
    ) -> LocalResult<OperationId> {
        self.registry
            .get(source)?
            .connector
            .unsubscribe(subscription)
            .map_err(|error| connector_error("unsubscribe", error))
    }
    fn request_snapshot(
        &self,
        source: MarketSourceHandle,
        asset_id: AssetId,
    ) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        self.registry
            .get(source)?
            .connector
            .request_snapshot(asset_id)
            .map_err(|error| connector_error("request_snapshot", error))
    }
    fn instruments(
        &self,
        source: MarketSourceHandle,
    ) -> LocalResult<Arc<[crate::InstrumentSnapshot]>> {
        Ok(self.registry.get(source)?.connector.instruments())
    }
    fn health(&self, source: MarketSourceHandle) -> LocalResult<crate::ConnectorHealthSnapshot> {
        Ok(self.registry.get(source)?.connector.health())
    }
    fn operation(
        &self,
        source: MarketSourceHandle,
        id: OperationId,
    ) -> LocalResult<ConnectorOperationSnapshot> {
        Ok(self.registry.get(source)?.connector.operation(id))
    }
}

pub struct MarketPluginLifecycle {
    core: Arc<MarketPluginCore>,
}
impl Plugin for MarketPluginLifecycle {
    fn validate(&self, _: &ValidationContext) -> Result<(), PluginError> {
        Ok(())
    }
    fn start(&mut self, context: &mut PluginContext) -> Result<(), PluginError> {
        self.core
            .activate(context.identity.clone(), context.events.clone());
        Ok(())
    }
    fn quiesce(&mut self, _: StopReason) -> Result<(), PluginError> {
        self.core
            .quiesce_all(Instant::now() + self.core.config.stop_timeout)
            .map_err(to_plugin_error)
    }
    fn stop(&mut self) -> Result<(), PluginError> {
        self.core.shutdown().map_err(to_plugin_error)
    }
}

fn to_plugin_error(error: MarketError) -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        PluginIdentity::new(MARKET_PLUGIN_TYPE, "market"),
        titan_plugin_engine::LifecycleState::Running,
        "market_plugin",
        error.to_string(),
    )
}

pub struct MarketPluginFactory {
    factories: Vec<Arc<dyn MarketConnectorFactory>>,
}
impl Default for MarketPluginFactory {
    fn default() -> Self {
        Self::new()
    }
}
impl MarketPluginFactory {
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
        }
    }
    pub fn with_factory(mut self, factory: Arc<dyn MarketConnectorFactory>) -> Self {
        self.factories.push(factory);
        self
    }
}

impl PluginFactory for MarketPluginFactory {
    fn manifest(&self) -> &'static PluginManifest {
        &MARKET_PLUGIN_MANIFEST
    }
    fn create(&self, init: PluginInit) -> Result<PluginBundle, PluginError> {
        let core = MarketPluginCore::new(MarketPluginConfig::from_snapshot(&init.config)?);
        for factory in &self.factories {
            core.register_factory(factory.clone())
                .map_err(to_plugin_error)?;
        }
        let admin: Arc<dyn MarketAdminService> = core.clone();
        let market: Arc<dyn MarketService> = core.clone();
        Ok(PluginBundle {
            lifecycle: Box::new(MarketPluginLifecycle { core }),
            service_exports: vec![
                ServiceExport {
                    service_key: ServiceKey {
                        id: ServiceId::new("titan.market", "admin"),
                        version: Version::new(1, 0, 0),
                        scope: ServiceScope::Global,
                    },
                    endpoint: boxed_typed_endpoint::<MarketAdminApi>(Arc::new(AdminEndpoint(
                        admin,
                    ))),
                },
                ServiceExport {
                    service_key: ServiceKey {
                        id: ServiceId::new("titan.market", "market"),
                        version: Version::new(1, 0, 0),
                        scope: ServiceScope::Global,
                    },
                    endpoint: boxed_typed_endpoint::<MarketApi>(Arc::new(MarketEndpoint(market))),
                },
            ],
            subscription_bindings: vec![],
        })
    }
}

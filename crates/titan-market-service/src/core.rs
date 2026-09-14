use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use titan_core_types::{ComponentIdentity, EventPublisher, ResourceScope};

use crate::{
    AssetId, ConnectorEntry, ConnectorLifecycle, ConnectorOperationSnapshot, ConnectorRegistry,
    LocalResult, MarketAdminService, MarketConnectorContext, MarketConnectorFactory, MarketError,
    MarketErrorKind, MarketEventPublisher, MarketOperationSnapshot, MarketService,
    MarketSourceDefinition, MarketSourceHandle, MarketSourceId, MarketSourceSnapshot,
    MarketSubscribeRequest, MarketSubscription, OperationId, OperationState, connector_error,
};

#[derive(Clone, Copy, Debug)]
pub struct MarketCoreConfig {
    pub max_sources: usize,
    pub max_instruments: usize,
    pub stop_timeout: Duration,
}

impl Default for MarketCoreConfig {
    fn default() -> Self {
        Self {
            max_sources: 16,
            max_instruments: 4096,
            stop_timeout: Duration::from_secs(5),
        }
    }
}

struct RuntimeBindings {
    identity: ComponentIdentity,
    publisher: EventPublisher,
}

pub struct MarketServiceCore {
    config: MarketCoreConfig,
    factories: RwLock<HashMap<Arc<str>, Arc<dyn MarketConnectorFactory>>>,
    registry: ConnectorRegistry,
    runtime: RwLock<Option<RuntimeBindings>>,
    next_operation_id: AtomicU64,
    operations: RwLock<HashMap<OperationId, MarketOperationSnapshot>>,
    accepting: AtomicBool,
    mutation: Mutex<()>,
}

impl MarketServiceCore {
    pub fn new(config: MarketCoreConfig) -> Arc<Self> {
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

    pub fn activate(&self, identity: ComponentIdentity, publisher: EventPublisher) {
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
                "market service is not accepting requests",
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
            MarketError::new(MarketErrorKind::RuntimeNotActive, "service has not started")
        })?;
        let handle = MarketSourceHandle {
            source_id,
            generation,
        };
        let market_stream = handle.market_stream_id().ok_or_else(|| {
            MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source stream id overflow",
            )
        })?;
        let control_stream = handle.control_stream_id().ok_or_else(|| {
            MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source stream id overflow",
            )
        })?;
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

impl MarketAdminService for MarketServiceCore {
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

impl MarketService for MarketServiceCore {
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

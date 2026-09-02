use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex, RwLock, Weak,
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

use crate::*;

pub const ACCOUNT_PLUGIN_TYPE: &str = "titan.account";
pub static ACCOUNT_PLUGIN_MANIFEST: LazyLock<PluginManifest> = LazyLock::new(|| PluginManifest {
    plugin_type: Arc::from(ACCOUNT_PLUGIN_TYPE),
    name: Arc::from("Titan Account Plugin"),
    version: Version::new(1, 0, 0),
    engine_api_version: titan_plugin_engine::CORE_RUNTIME_API_VERSION,
    abi_version: ApiVersion::new(1, 0),
    config_schema: Arc::new(serde_json::json!({"type":"object"})),
    provides: vec![
        ProvidedService {
            id: ServiceId::new("titan.account", "admin"),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        },
        ProvidedService {
            id: ServiceId::new("titan.account", "query"),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        },
        ProvidedService {
            id: ServiceId::new("titan.account", "execution"),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Global,
            call_mode: CallMode::Inline,
        },
    ],
    requires: vec![],
    publishes: ACCOUNT_EVENT_TYPES
        .iter()
        .map(|e| PublishedEvent {
            event_type: Arc::from(*e),
            schema_version: if *e == FILL_EVENT {
                FILL_EVENT_SCHEMA_VERSION
            } else {
                ACCOUNT_EVENT_SCHEMA_VERSION
            },
        })
        .collect(),
    subscribes: vec![],
    supported_execution_models: [ExecutionModel::Passive].into_iter().collect(),
    reload_policy: ReloadPolicy::WhenQuiescent,
});

#[derive(Clone, Copy, Debug)]
pub struct AccountPluginConfig {
    pub max_accounts: usize,
    pub max_instruments_per_account: usize,
    pub max_currencies_per_account: usize,
    pub command_queue_capacity: usize,
    pub stop_timeout: Duration,
}
impl Default for AccountPluginConfig {
    fn default() -> Self {
        Self {
            max_accounts: 32,
            max_instruments_per_account: 4096,
            max_currencies_per_account: 512,
            command_queue_capacity: 8192,
            stop_timeout: Duration::from_secs(5),
        }
    }
}
impl AccountPluginConfig {
    fn from_snapshot(s: &ConfigSnapshot) -> Result<Self, PluginError> {
        let section = s.value.get("account_plugin").unwrap_or(s.value.as_ref());
        let mut c = Self::default();
        if let Some(v) = section.get("max_accounts").and_then(|v| v.as_u64()) {
            c.max_accounts = v as usize;
        }
        if let Some(v) = section
            .get("max_instruments_per_account")
            .and_then(|v| v.as_u64())
        {
            c.max_instruments_per_account = v as usize;
        }
        if let Some(v) = section
            .get("max_currencies_per_account")
            .and_then(|v| v.as_u64())
        {
            c.max_currencies_per_account = v as usize;
        }
        if let Some(v) = section
            .get("command_queue_capacity")
            .and_then(|v| v.as_u64())
        {
            c.command_queue_capacity = v as usize;
        }
        if let Some(v) = section.get("stop_timeout_ms").and_then(|v| v.as_u64()) {
            c.stop_timeout = Duration::from_millis(v);
        }
        if c.max_accounts == 0
            || c.max_instruments_per_account == 0
            || c.max_currencies_per_account == 0
            || c.command_queue_capacity == 0
        {
            return Err(plugin_error("account capacities must be non-zero"));
        }
        Ok(c)
    }
}

struct RuntimeBindings {
    identity: PluginIdentity,
    publisher: EventPublisher,
}
pub struct AccountPluginCore {
    pub config: AccountPluginConfig,
    factories: AccountConnectorFactoryRegistry,
    registry: AccountRegistry,
    secret_provider: Arc<dyn SecretProvider>,
    runtime: RwLock<Option<RuntimeBindings>>,
    next_operation_id: AtomicU64,
    operations: RwLock<HashMap<OperationId, AccountOperationSnapshot>>,
    connector_operations: RwLock<HashMap<OperationId, (Weak<AccountEntry>, OperationId)>>,
    accepting: AtomicBool,
    cancel_accepting: AtomicBool,
    mutation: Mutex<()>,
}

impl AccountPluginCore {
    pub fn new(config: AccountPluginConfig) -> Arc<Self> {
        Self::with_secret_provider(config, Arc::new(UnavailableSecretProvider))
    }
    pub fn with_secret_provider(
        config: AccountPluginConfig,
        secret_provider: Arc<dyn SecretProvider>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            factories: Default::default(),
            registry: Default::default(),
            secret_provider,
            runtime: RwLock::new(None),
            next_operation_id: AtomicU64::new(1),
            operations: RwLock::new(HashMap::new()),
            connector_operations: RwLock::new(HashMap::new()),
            accepting: AtomicBool::new(false),
            cancel_accepting: AtomicBool::new(false),
            mutation: Mutex::new(()),
        })
    }
    pub fn register_factory(&self, f: Arc<dyn AccountConnectorFactory>) -> LocalResult<()> {
        self.factories.register(f)
    }
    pub fn activate(&self, identity: PluginIdentity, publisher: EventPublisher) {
        *self.runtime.write().unwrap_or_else(|p| p.into_inner()) = Some(RuntimeBindings {
            identity,
            publisher,
        });
        self.cancel_accepting.store(true, Ordering::Release);
        self.accepting.store(true, Ordering::Release);
    }
    fn ensure_accepting(&self) -> LocalResult<()> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AccountError::new(
                AccountErrorKind::RuntimeNotActive,
                "account plugin is not accepting requests",
            ))
        }
    }
    fn ensure_cancel_accepting(&self) -> LocalResult<()> {
        if self.cancel_accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AccountError::new(
                AccountErrorKind::RuntimeNotActive,
                "account plugin is not accepting cancellation requests",
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
                AccountOperationSnapshot {
                    id,
                    state,
                    detail: detail.into(),
                },
            );
        id
    }
    fn build_entry(
        &self,
        d: AccountDefinition,
        generation: u64,
        publisher_open: bool,
    ) -> LocalResult<Arc<AccountEntry>> {
        let factory = self.factories.get(&d.connector_type)?;
        let runtime = self.runtime.read().unwrap_or_else(|p| p.into_inner());
        let runtime = runtime.as_ref().ok_or_else(|| {
            AccountError::new(AccountErrorKind::RuntimeNotActive, "plugin has not started")
        })?;
        let handle = AccountHandle {
            account_id: d.account_id,
            generation,
        };
        let base = d.account_id.0.checked_mul(2).ok_or_else(|| {
            AccountError::new(
                AccountErrorKind::CapacityExceeded,
                "account source stream id overflow",
            )
        })?;
        let account_stream = SourceStreamId(base);
        let control_stream = SourceStreamId(base.checked_add(1).ok_or_else(|| {
            AccountError::new(
                AccountErrorKind::CapacityExceeded,
                "account source stream id overflow",
            )
        })?);
        let resources = ResourceScope::new(runtime.identity.clone());
        let publisher_admission = Arc::new(AtomicBool::new(publisher_open));
        let secret_active = Arc::new(AtomicBool::new(true));
        let context = AccountConnectorContext {
            account: handle,
            instruments: d.instruments.clone(),
            currencies: d.currencies.clone(),
            ownership: d.ownership.clone(),
            account_stream,
            control_stream,
            event_publisher: AccountEventPublisher::new(
                runtime.publisher.clone(),
                handle,
                account_stream,
                control_stream,
                publisher_admission.clone(),
            ),
            resources: resources.handle(),
            secrets: ScopedSecretResolver::new(
                d.credential_ref.clone(),
                self.secret_provider.clone(),
                secret_active.clone(),
            ),
            command_queue_capacity: self.config.command_queue_capacity,
        };
        let connector = factory
            .create(&d, context)
            .map_err(|e| connector_error("create", e))?;
        Ok(Arc::new(AccountEntry::new(
            handle,
            d,
            connector,
            resources,
            publisher_admission,
            secret_active,
        )))
    }
    fn require_ready(&self, h: AccountHandle) -> LocalResult<Arc<AccountEntry>> {
        let e = self.registry.get(h)?;
        let health = e.connector.health();
        if health.state == AccountLifecycle::Ready {
            e.set_lifecycle(AccountLifecycle::Ready);
            Ok(e)
        } else {
            Err(AccountError::new(
                AccountErrorKind::NotReady,
                "account connector is not ready",
            ))
        }
    }
    fn validate_asset(e: &AccountEntry, asset: AssetId) -> LocalResult<()> {
        if e.definition.instruments.iter().any(|b| b.asset_id == asset) {
            Ok(())
        } else {
            Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "command asset is not bound to account",
            ))
        }
    }
    fn validate_command_id(id: CommandId) -> LocalResult<()> {
        if id.0 == [0; 16] {
            Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "command id must be non-zero",
            ))
        } else {
            Ok(())
        }
    }
    pub fn quiesce_all(&self, deadline: Instant) -> LocalResult<()> {
        self.accepting.store(false, Ordering::Release);
        let mut failures = Vec::new();
        for e in self.registry.list_entries() {
            e.set_lifecycle(AccountLifecycle::Stopping);
            if let Err(x) = e.connector.stop(deadline) {
                failures.push(x.to_string());
                e.set_lifecycle(AccountLifecycle::Failed);
            } else {
                e.set_lifecycle(AccountLifecycle::Stopped);
            }
            e.close_publication();
            if let Err(x) = e.close_resources() {
                failures.push(x.to_string());
            }
        }
        self.cancel_accepting.store(false, Ordering::Release);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(AccountError::new(
                AccountErrorKind::ResourceReleaseFailed,
                failures.join("; "),
            ))
        }
    }
    pub fn shutdown(&self) -> LocalResult<()> {
        self.accepting.store(false, Ordering::Release);
        self.cancel_accepting.store(false, Ordering::Release);
        let mut failures = Vec::new();
        for e in self.registry.list_entries() {
            if let Err(x) = self.registry.remove(e.handle) {
                failures.push(x.to_string());
            }
            if let Err(x) = e.close_resources() {
                failures.push(x.to_string());
            }
        }
        *self.runtime.write().unwrap_or_else(|p| p.into_inner()) = None;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(AccountError::new(
                AccountErrorKind::ResourceReleaseFailed,
                failures.join("; "),
            ))
        }
    }
}

impl AccountAdminService for AccountPluginCore {
    fn create(&self, d: AccountDefinition) -> LocalResult<AccountHandle> {
        self.ensure_accepting()?;
        let _g = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        self.registry.validate_insert(
            &d,
            self.config.max_accounts,
            self.config.max_instruments_per_account,
            self.config.max_currencies_per_account,
            None,
        )?;
        let generation = self.registry.next_generation(&d.account_key, d.account_id);
        let e = self.build_entry(d, generation, true)?;
        let h = e.handle;
        self.registry.insert(e)?;
        Ok(h)
    }
    fn start(&self, h: AccountHandle) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        let e = self.registry.get(h)?;
        if !e.definition.enabled {
            return Err(AccountError::new(
                AccountErrorKind::NotReady,
                "disabled account cannot be started",
            ));
        }
        if matches!(
            e.lifecycle(),
            AccountLifecycle::Starting
                | AccountLifecycle::Connecting
                | AccountLifecycle::Reconciling
                | AccountLifecycle::Ready
        ) {
            return Err(AccountError::new(
                AccountErrorKind::AlreadyExists,
                "account connector is already active",
            ));
        }
        e.set_lifecycle(AccountLifecycle::Starting);
        match e.connector.start() {
            Ok(()) => {
                e.set_lifecycle(AccountLifecycle::Connecting);
                Ok(self.next_operation(OperationState::Succeeded, "account connector started"))
            }
            Err(x) => {
                e.set_lifecycle(AccountLifecycle::Failed);
                Err(connector_error("start", x))
            }
        }
    }
    fn stop(&self, h: AccountHandle, deadline: Instant) -> LocalResult<OperationId> {
        let e = self.registry.get(h)?;
        e.set_lifecycle(AccountLifecycle::Stopping);
        let stop = e.connector.stop(deadline);
        e.close_publication();
        let resources = e.close_resources();
        match (stop, resources) {
            (Ok(()), Ok(())) => {
                e.set_lifecycle(AccountLifecycle::Stopped);
                Ok(self.next_operation(OperationState::Succeeded, "account connector stopped"))
            }
            (a, b) => {
                e.set_lifecycle(AccountLifecycle::Failed);
                let details = [
                    a.err().map(|x| x.to_string()),
                    b.err().map(|x| x.to_string()),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("; ");
                Err(AccountError::new(
                    if Instant::now() >= deadline {
                        AccountErrorKind::DeadlineExceeded
                    } else {
                        AccountErrorKind::ResourceReleaseFailed
                    },
                    details,
                ))
            }
        }
    }
    fn remove(&self, h: AccountHandle) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        let _g = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let e = self.registry.get(h)?;
        if !matches!(
            e.lifecycle(),
            AccountLifecycle::Created | AccountLifecycle::Stopped | AccountLifecycle::Failed
        ) {
            return Err(AccountError::new(
                AccountErrorKind::ConnectorRejected,
                "stop account before removal",
            ));
        }
        let e = self.registry.remove(h)?;
        e.close_resources()?;
        Ok(self.next_operation(OperationState::Succeeded, "account removed"))
    }
    fn replace(&self, h: AccountHandle, d: AccountDefinition) -> LocalResult<AccountHandle> {
        self.ensure_accepting()?;
        let _g = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let old = self.registry.get(h)?;
        if old.definition.account_key != d.account_key || old.handle.account_id != d.account_id {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "replace must preserve account key and account id",
            ));
        }
        if d.definition_version <= old.definition.definition_version {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "replacement definition version must increase",
            ));
        }
        self.registry.validate_insert(
            &d,
            self.config.max_accounts,
            self.config.max_instruments_per_account,
            self.config.max_currencies_per_account,
            Some(h.account_id),
        )?;
        let new = self.build_entry(d, h.generation.saturating_add(1), false)?;
        let was_active = !matches!(
            old.lifecycle(),
            AccountLifecycle::Created | AccountLifecycle::Stopped
        );
        if was_active {
            new.set_lifecycle(AccountLifecycle::Starting);
            if let Err(x) = new.connector.start() {
                new.close_resources().ok();
                return Err(connector_error("replace start new connector", x));
            }
            new.set_lifecycle(AccountLifecycle::Reconciling);
            let deadline = Instant::now() + self.config.stop_timeout;
            while new.connector.health().state != AccountLifecycle::Ready
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            if new.connector.health().state != AccountLifecycle::Ready {
                let _ = new.connector.stop(deadline);
                new.close_resources().ok();
                return Err(AccountError::new(
                    AccountErrorKind::DeadlineExceeded,
                    "replacement candidate did not finish private-stream reconciliation",
                ));
            }
            old.set_lifecycle(AccountLifecycle::Stopping);
            if let Err(x) = old.connector.stop(deadline) {
                let _ = new.connector.stop(deadline);
                new.close_resources().ok();
                old.set_lifecycle(AccountLifecycle::Failed);
                return Err(connector_error("replace stop old connector", x));
            }
        }
        old.close_publication();
        let nh = new.handle;
        let old = self.registry.swap(h, new)?;
        let replacement = self.registry.get(nh)?;
        replacement
            .publisher_admission
            .store(true, Ordering::Release);
        if was_active {
            replacement.set_lifecycle(AccountLifecycle::Reconciling);
            if let Err(x) = replacement.connector.reconcile(ReconcileScope::Full) {
                replacement.set_lifecycle(AccountLifecycle::Invalidated);
                old.close_resources().ok();
                return Err(connector_error("activate replacement", x));
            }
        }
        old.close_resources()?;
        Ok(nh)
    }
    fn reconcile(&self, h: AccountHandle, scope: ReconcileScope) -> LocalResult<OperationId> {
        self.ensure_accepting()?;
        let e = self.registry.get(h)?;
        let connector_id = e
            .connector
            .reconcile(scope)
            .map_err(|x| connector_error("reconcile", x))?;
        e.set_lifecycle(AccountLifecycle::Reconciling);
        let id = self.next_operation(OperationState::Pending, "reconciliation queued");
        self.connector_operations
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, (Arc::downgrade(&e), connector_id));
        Ok(id)
    }
    fn list(&self) -> Arc<[AccountInstanceSnapshot]> {
        self.registry.list()
    }
    fn operation(&self, id: OperationId) -> AccountOperationSnapshot {
        if let Some((entry, connector_id)) = self
            .connector_operations
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned()
        {
            if let Some(entry) = entry.upgrade() {
                let snapshot = entry.connector.operation(connector_id);
                if snapshot.state != OperationState::Pending {
                    entry.set_lifecycle(if snapshot.state == OperationState::Succeeded {
                        AccountLifecycle::Ready
                    } else {
                        AccountLifecycle::Invalidated
                    });
                }
                return AccountOperationSnapshot {
                    id,
                    state: snapshot.state,
                    detail: snapshot.detail,
                };
            }
            return AccountOperationSnapshot {
                id,
                state: OperationState::Failed,
                detail: Arc::from("account instance no longer exists"),
            };
        }
        self.operations
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned()
            .unwrap_or(AccountOperationSnapshot {
                id,
                state: OperationState::Failed,
                detail: Arc::from("operation not found"),
            })
    }
}

impl AccountService for AccountPluginCore {
    fn resolve(&self, k: &str) -> LocalResult<AccountHandle> {
        self.registry.resolve(k)
    }
    fn orders(
        &self,
        h: AccountHandle,
        f: OrderFilter,
    ) -> LocalResult<AccountStateSnapshot<OrderSnapshot>> {
        self.registry
            .get(h)?
            .connector
            .orders(f)
            .map_err(|x| connector_error("orders", x))
    }
    fn positions(
        &self,
        h: AccountHandle,
        f: PositionFilter,
    ) -> LocalResult<AccountStateSnapshot<PositionSnapshot>> {
        self.registry
            .get(h)?
            .connector
            .positions(f)
            .map_err(|x| connector_error("positions", x))
    }
    fn balances(&self, h: AccountHandle) -> LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
        self.registry
            .get(h)?
            .connector
            .balances()
            .map_err(|x| connector_error("balances", x))
    }
    fn health(&self, h: AccountHandle) -> LocalResult<AccountConnectorHealthSnapshot> {
        Ok(self.registry.get(h)?.connector.health())
    }
    fn diagnostics(&self, h: AccountHandle) -> LocalResult<AccountConnectorDiagnosticSnapshot> {
        Ok(self.registry.get(h)?.connector.diagnostics())
    }
}

impl AccountExecutionService for AccountPluginCore {
    fn submit(
        &self,
        h: AccountHandle,
        c: SubmitOrderCommand,
    ) -> LocalResult<AccountCommandReceipt> {
        self.ensure_accepting()?;
        Self::validate_command_id(c.command_id)?;
        if c.quantity_lots <= 0 {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "submit quantity must be positive",
            ));
        }
        let e = self.require_ready(h)?;
        Self::validate_asset(&e, c.asset_id)?;
        e.connector
            .submit(c)
            .map_err(|x| connector_error("submit", x))
    }
    fn amend(&self, h: AccountHandle, c: AmendOrderCommand) -> LocalResult<AccountCommandReceipt> {
        self.ensure_accepting()?;
        Self::validate_command_id(c.command_id)?;
        if c.client_order_id.is_none() && c.venue_order_id.is_none() {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "amend requires an order id",
            ));
        }
        if c.quantity_lots.is_some_and(|v| v <= 0) {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "amend quantity must be positive",
            ));
        }
        let e = self.require_ready(h)?;
        Self::validate_asset(&e, c.asset_id)?;
        e.connector
            .amend(c)
            .map_err(|x| connector_error("amend", x))
    }
    fn cancel(
        &self,
        h: AccountHandle,
        c: CancelOrderCommand,
    ) -> LocalResult<AccountCommandReceipt> {
        self.ensure_cancel_accepting()?;
        Self::validate_command_id(c.command_id)?;
        if c.client_order_id.is_none() && c.venue_order_id.is_none() {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "cancel requires an order id",
            ));
        }
        let e = self.registry.get(h)?;
        Self::validate_asset(&e, c.asset_id)?;
        e.connector
            .cancel(c)
            .map_err(|x| connector_error("cancel", x))
    }
    fn cancel_all(
        &self,
        h: AccountHandle,
        c: CancelAllCommand,
    ) -> LocalResult<AccountCommandReceipt> {
        self.ensure_cancel_accepting()?;
        Self::validate_command_id(c.command_id)?;
        let e = self.registry.get(h)?;
        if let Some(a) = c.asset_id {
            Self::validate_asset(&e, a)?;
        }
        e.connector
            .cancel_all(c)
            .map_err(|x| connector_error("cancel_all", x))
    }
    fn cancel_all_after(
        &self,
        h: AccountHandle,
        c: CancelAllAfterCommand,
    ) -> LocalResult<AccountCommandReceipt> {
        self.ensure_cancel_accepting()?;
        Self::validate_command_id(c.command_id)?;
        if c.timeout_ms == 0 {
            return Err(AccountError::new(
                AccountErrorKind::InvalidDefinition,
                "cancel_all_after timeout must be positive",
            ));
        }
        self.registry
            .get(h)?
            .connector
            .cancel_all_after(c)
            .map_err(|x| connector_error("cancel_all_after", x))
    }
}

pub struct AccountPluginLifecycle {
    core: Arc<AccountPluginCore>,
}
impl Plugin for AccountPluginLifecycle {
    fn validate(&self, _: &ValidationContext) -> Result<(), PluginError> {
        Ok(())
    }
    fn start(&mut self, c: &mut PluginContext) -> Result<(), PluginError> {
        self.core.activate(c.identity.clone(), c.events.clone());
        Ok(())
    }
    fn quiesce(&mut self, _: StopReason) -> Result<(), PluginError> {
        self.core
            .quiesce_all(Instant::now() + self.core.config.stop_timeout)
            .map_err(|e| plugin_error(e.to_string()))
    }
    fn stop(&mut self) -> Result<(), PluginError> {
        self.core
            .shutdown()
            .map_err(|e| plugin_error(e.to_string()))
    }
}

fn plugin_error(message: impl Into<Arc<str>>) -> PluginError {
    PluginError::new(
        titan_plugin_engine::ErrorKind::PluginFailed,
        PluginIdentity::new(ACCOUNT_PLUGIN_TYPE, "account"),
        titan_plugin_engine::LifecycleState::Running,
        "account_plugin",
        message,
    )
}

pub struct AccountPluginFactory {
    factories: Vec<Arc<dyn AccountConnectorFactory>>,
    secret_provider: Arc<dyn SecretProvider>,
}
impl Default for AccountPluginFactory {
    fn default() -> Self {
        Self::new()
    }
}
impl AccountPluginFactory {
    pub fn new() -> Self {
        Self {
            factories: Vec::new(),
            secret_provider: Arc::new(UnavailableSecretProvider),
        }
    }
    pub fn with_factory(mut self, f: Arc<dyn AccountConnectorFactory>) -> Self {
        self.factories.push(f);
        self
    }
    pub fn with_secret_provider(mut self, p: Arc<dyn SecretProvider>) -> Self {
        self.secret_provider = p;
        self
    }
}
impl PluginFactory for AccountPluginFactory {
    fn manifest(&self) -> &'static PluginManifest {
        &ACCOUNT_PLUGIN_MANIFEST
    }
    fn create(&self, init: PluginInit) -> Result<PluginBundle, PluginError> {
        let core = AccountPluginCore::with_secret_provider(
            AccountPluginConfig::from_snapshot(&init.config)?,
            self.secret_provider.clone(),
        );
        for f in &self.factories {
            core.register_factory(f.clone())
                .map_err(|e| plugin_error(e.to_string()))?;
        }
        let admin: Arc<dyn AccountAdminService> = core.clone();
        let query: Arc<dyn AccountService> = core.clone();
        let execution: Arc<dyn AccountExecutionService> = core.clone();
        Ok(PluginBundle {
            lifecycle: Box::new(AccountPluginLifecycle { core }),
            service_exports: vec![
                export::<AccountAdminApi>("admin", Arc::new(AdminEndpoint(admin))),
                export::<AccountApi>("query", Arc::new(QueryEndpoint(query))),
                export::<AccountExecutionApi>("execution", Arc::new(ExecutionEndpoint(execution))),
            ],
            subscription_bindings: vec![],
        })
    }
}
fn export<S: titan_plugin_engine::Service>(
    name: &str,
    endpoint: Arc<impl titan_plugin_engine::TypedServiceEndpoint<S>>,
) -> ServiceExport {
    ServiceExport {
        service_key: ServiceKey {
            id: ServiceId::new("titan.account", name),
            version: Version::new(1, 0, 0),
            scope: ServiceScope::Global,
        },
        endpoint: boxed_typed_endpoint::<S>(endpoint),
    }
}

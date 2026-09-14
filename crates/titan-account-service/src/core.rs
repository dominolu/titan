use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use titan_core_types::{ComponentIdentity, EventPublisher, ResourceScope};

use crate::*;

#[derive(Clone, Copy, Debug)]
pub struct AccountCoreConfig {
    pub max_accounts: usize,
    pub max_instruments_per_account: usize,
    pub max_currencies_per_account: usize,
    pub stop_timeout: Duration,
}
impl Default for AccountCoreConfig {
    fn default() -> Self {
        Self {
            max_accounts: 32,
            max_instruments_per_account: 4096,
            max_currencies_per_account: 512,
            stop_timeout: Duration::from_secs(5),
        }
    }
}

struct RuntimeBindings {
    identity: ComponentIdentity,
    publisher: EventPublisher,
}
pub struct AccountServiceCore {
    pub config: AccountCoreConfig,
    factories: AccountConnectorFactoryRegistry,
    registry: AccountRegistry,
    secret_provider: Arc<dyn SecretProvider>,
    runtime: RwLock<Option<RuntimeBindings>>,
    next_operation_id: AtomicU64,
    operations: RwLock<HashMap<OperationId, AccountOperationSnapshot>>,
    accepting: AtomicBool,
    mutation: Mutex<()>,
}

impl AccountServiceCore {
    pub fn new(config: AccountCoreConfig) -> Arc<Self> {
        Self::with_secret_provider(config, Arc::new(UnavailableSecretProvider))
    }
    pub fn with_secret_provider(
        config: AccountCoreConfig,
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
            accepting: AtomicBool::new(false),
            mutation: Mutex::new(()),
        })
    }
    pub fn register_factory(&self, f: Arc<dyn AccountConnectorFactory>) -> LocalResult<()> {
        self.factories.register(f)
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
            Err(AccountError::new(
                AccountErrorKind::RuntimeNotActive,
                "account service is not accepting requests",
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
            AccountError::new(
                AccountErrorKind::RuntimeNotActive,
                "service has not started",
            )
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

impl AccountAdminService for AccountServiceCore {
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
                | AccountLifecycle::Bootstrapping
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
            new.set_lifecycle(AccountLifecycle::Bootstrapping);
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
                    "replacement candidate did not finish account bootstrap",
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
            replacement.set_lifecycle(AccountLifecycle::Ready);
        }
        old.close_resources()?;
        Ok(nh)
    }
    fn list(&self) -> Arc<[AccountInstanceSnapshot]> {
        self.registry.list()
    }
    fn operation(&self, id: OperationId) -> AccountOperationSnapshot {
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

impl AccountService for AccountServiceCore {
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

    fn execution_connector(
        &self,
        h: AccountHandle,
    ) -> LocalResult<Arc<dyn DirectExecutionConnector>> {
        let entry = self.require_ready(h)?;
        Ok(Arc::new(AccountConnectorExecution::new(
            entry.connector.clone(),
        )))
    }
}

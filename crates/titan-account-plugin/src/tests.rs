use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use semver::Version;
use titan_event_engine::{EventClass, EventEngine, EventEngineConfig, PoolKind};
use titan_plugin_engine::{
    ApiVersion, DispatchOutcome, EventControl, EventHandler, EventQos, EventView, ExecutionModel,
    ExecutionSpec, PluginEngine, PluginError, PluginIdentity, PluginSpec, ServiceKey, ServiceScope,
    StopReason, SubscriptionLimits, SubscriptionSpec, TraceContext,
};

use crate::*;

#[test]
fn fill_v2_preserves_last_and_cumulative_quantities() {
    let value = FillV2 {
        header: AccountEventHeaderV1 {
            account_id: 1,
            kind: event_kind::FILL,
            account_generation: 2,
            account_epoch: 3,
            account_version: 4,
            ..AccountEventHeaderV1::default()
        },
        asset_id: 9,
        last_fill_quantity_lots: 2,
        cumulative_filled_quantity_lots: 7,
        ..FillV2::default()
    };
    let mut encoded = vec![0; FillV2::ENCODED_LEN];
    value.encode_into(&mut encoded).unwrap();
    assert_eq!(FillV2::decode(&encoded).unwrap(), value);
    assert_eq!(
        account_event_layout_version(FILL_EVENT, FILL_EVENT_SCHEMA_VERSION),
        Some((event_kind::FILL, FillV2::ENCODED_LEN))
    );
}

struct TestSecrets;
impl SecretProvider for TestSecrets {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
        if reference.as_str() == "secret://account/main" {
            Ok(SecretValue::new(b"very-secret".to_vec()))
        } else {
            Err(AccountConnectorError::new(
                AccountErrorKind::CredentialUnavailable,
                "credential unavailable",
            ))
        }
    }
}

struct FakeConnector {
    context: AccountConnectorContext,
    running: AtomicBool,
    reconciling: AtomicBool,
    next_id: AtomicU64,
    calls: AtomicU64,
    operation_queries: AtomicU64,
    journal: Mutex<HashMap<CommandId, (SubmitOrderCommand, AccountCommandReceipt)>>,
}

impl FakeConnector {
    fn receipt(&self, id: CommandId, client: Option<ClientOrderId>) -> AccountCommandReceipt {
        AccountCommandReceipt {
            account: self.context.account,
            command_id: id,
            client_order_id: client,
            accepted_at: 123,
        }
    }
    fn empty<T>(&self) -> AccountStateSnapshot<T> {
        AccountStateSnapshot {
            account: self.context.account,
            state: if self.reconciling.load(Ordering::Acquire) {
                AccountSnapshotState::Reconciling
            } else if self.running.load(Ordering::Acquire) {
                AccountSnapshotState::Ready
            } else {
                AccountSnapshotState::Stopped
            },
            committed_epoch: self.running.load(Ordering::Acquire).then_some(1),
            committed_version: self.running.load(Ordering::Acquire).then_some(2),
            captured_at: 10,
            items: Arc::from([]),
        }
    }
}

impl AccountConnector for FakeConnector {
    fn start(&self) -> Result<(), AccountConnectorError> {
        self.running.store(true, Ordering::Release);
        if !self.context.event_publisher.is_open() {
            return Ok(());
        }
        let spoofed = OrderChangedV1 {
            header: AccountEventHeaderV1 {
                account_id: self.context.account.account_id.0.saturating_add(1),
                kind: event_kind::ORDER_CHANGED,
                account_generation: self.context.account.generation,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            self.context
                .event_publisher
                .publish_encoded(&spoofed, TraceContext::default())
                .is_err()
        );
        let header = AccountEventHeaderV1 {
            account_id: self.context.account.account_id.0,
            kind: event_kind::RECONCILE_STARTED,
            account_generation: self.context.account.generation,
            account_epoch: 1,
            account_version: 1,
            exchange_ts: 11,
            receive_ts: 12,
            ..Default::default()
        };
        self.context
            .event_publisher
            .publish_encoded(
                &ReconcileStartedV1(ReconcileV1 {
                    header,
                    scope: 0,
                    ..Default::default()
                }),
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        self.context
            .event_publisher
            .publish_encoded(
                &OrderChangedV1 {
                    header: AccountEventHeaderV1 {
                        kind: event_kind::ORDER_CHANGED,
                        account_version: 2,
                        ..header
                    },
                    asset_id: self.context.instruments[0].asset_id.0,
                    quantity_lots: 1,
                    command_id: Id128([7; 16]),
                    ..Default::default()
                },
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        self.context
            .event_publisher
            .publish_encoded(
                &ReconcileCompletedV1(ReconcileV1 {
                    header: AccountEventHeaderV1 {
                        kind: event_kind::RECONCILE_COMPLETED,
                        account_version: 3,
                        ..header
                    },
                    terminal_version: 3,
                    scope: 0,
                    success: 1,
                }),
                TraceContext::default(),
            )
            .map_err(|e| AccountConnectorError::rejected(e.to_string()))?;
        Ok(())
    }
    fn stop(&self, _: Instant) -> Result<(), AccountConnectorError> {
        self.running.store(false, Ordering::Release);
        Ok(())
    }
    fn submit(
        &self,
        c: SubmitOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if c.command_id.0[0] == 255 {
            return Err(AccountConnectorError::new(
                AccountErrorKind::QueueFull,
                "queue full",
            ));
        }
        let mut j = self.journal.lock().unwrap();
        if let Some((old, r)) = j.get(&c.command_id) {
            return if old == &c {
                Ok(r.clone())
            } else {
                Err(AccountConnectorError::new(
                    AccountErrorKind::CommandConflict,
                    "command conflict",
                ))
            };
        }
        let r = self.receipt(c.command_id, c.client_order_id);
        j.insert(c.command_id, (c, r.clone()));
        Ok(r)
    }
    fn amend(&self, c: AmendOrderCommand) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, c.client_order_id))
    }
    fn cancel(
        &self,
        c: CancelOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, c.client_order_id))
    }
    fn cancel_all(
        &self,
        c: CancelAllCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, None))
    }
    fn cancel_all_after(
        &self,
        c: CancelAllAfterCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.receipt(c.command_id, None))
    }
    fn reconcile(&self, _: ReconcileScope) -> Result<OperationId, AccountConnectorError> {
        self.reconciling.store(true, Ordering::Release);
        Ok(OperationId(self.next_id.fetch_add(1, Ordering::Relaxed)))
    }
    fn orders(
        &self,
        _: OrderFilter,
    ) -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn positions(
        &self,
        _: PositionFilter,
    ) -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn balances(&self) -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError> {
        Ok(self.empty())
    }
    fn health(&self) -> AccountConnectorHealthSnapshot {
        AccountConnectorHealthSnapshot {
            state: if self.running.load(Ordering::Acquire) {
                AccountLifecycle::Ready
            } else {
                AccountLifecycle::Stopped
            },
            message: Arc::from("fake"),
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot {
        AccountConnectorDiagnosticSnapshot {
            summary: Arc::from("fake"),
            external_order_count: 0,
            command_queue_depth: 0,
            account_epoch: 1,
            account_version: 2,
        }
    }
    fn operation(&self, id: OperationId) -> AccountConnectorOperationSnapshot {
        let state = if self.operation_queries.fetch_add(1, Ordering::AcqRel) == 0 {
            OperationState::Pending
        } else {
            self.reconciling.store(false, Ordering::Release);
            OperationState::Succeeded
        };
        AccountConnectorOperationSnapshot {
            id,
            state,
            detail: Arc::from(if state == OperationState::Pending {
                "reconciling"
            } else {
                "reconciled"
            }),
        }
    }
}

struct FakeFactory;
impl AccountConnectorFactory for FakeFactory {
    fn connector_type(&self) -> &str {
        "fake"
    }
    fn create(
        &self,
        _: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError> {
        let secret = context
            .secrets
            .resolve(&SecretRef::new("secret://account/main"))?;
        assert_eq!(secret.expose(), b"very-secret");
        assert_eq!(format!("{:?}", secret), "SecretValue(REDACTED)");
        assert!(
            context
                .secrets
                .resolve(&SecretRef::new("secret://another"))
                .is_err()
        );
        Ok(Arc::new(FakeConnector {
            context,
            running: AtomicBool::new(false),
            reconciling: AtomicBool::new(false),
            next_id: AtomicU64::new(50),
            calls: AtomicU64::new(0),
            operation_queries: AtomicU64::new(0),
            journal: Mutex::new(HashMap::new()),
        }))
    }
}

fn definition(key: &str, id: u32) -> AccountDefinition {
    AccountDefinition {
        account_key: Arc::from(key),
        account_id: AccountId(id),
        connector_type: Arc::from("fake"),
        credential_ref: SecretRef::new("secret://account/main"),
        connector_config: Arc::from([]),
        instruments: Arc::from([AccountInstrumentBinding {
            native_symbol: Arc::from("BTCUSDT"),
            asset_id: AssetId(1001),
            price_tick: "0.1".parse().unwrap(),
            quantity_lot: "0.001".parse().unwrap(),
            contract_multiplier: "1".parse().unwrap(),
        }]),
        currencies: Arc::from([AccountCurrencyBinding {
            native_currency: Arc::from("USDT"),
            currency_id: CurrencyId(10),
            amount_unit: "0.00000001".parse().unwrap(),
        }]),
        ownership: OrderOwnershipPolicy::ManagedOnly {
            client_id_prefix: Arc::from("titan-"),
        },
        shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
        enabled: true,
        definition_version: 1,
    }
}

fn spec() -> PluginSpec {
    PluginSpec {
        instance_id: Arc::from("account"),
        plugin_type: Arc::from(ACCOUNT_PLUGIN_TYPE),
        config: Arc::new(titan_plugin_engine::ConfigSnapshot::new(
            1,
            serde_json::json!({"account_plugin":{"max_accounts":2,"max_instruments_per_account":4,"max_currencies_per_account":4}}),
        )),
        enabled: true,
        execution: ExecutionSpec {
            model: ExecutionModel::Passive,
            cpu_affinity: None,
            callback_budget: None,
        },
        subscription_limits: SubscriptionLimits {
            max_capacity: 16,
            allowed_qos: BTreeSet::from([
                EventQos::ReliableOrdered,
                EventQos::BestEffort,
                EventQos::Latest,
            ]),
        },
        service_scopes: vec![
            (
                titan_plugin_engine::ServiceId::new("titan.account", "admin"),
                ServiceScope::Global,
            ),
            (
                titan_plugin_engine::ServiceId::new("titan.account", "query"),
                ServiceScope::Global,
            ),
            (
                titan_plugin_engine::ServiceId::new("titan.account", "execution"),
                ServiceScope::Global,
            ),
        ],
        required_service_scopes: vec![],
    }
}
fn key(name: &str) -> ServiceKey {
    ServiceKey {
        id: titan_plugin_engine::ServiceId::new("titan.account", name),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    }
}
fn admin(e: &PluginEngine, r: AccountAdminRequest) -> LocalResult<AccountAdminResponse> {
    *e.services()
        .bind(&key("admin"))
        .unwrap()
        .call(Box::new(r), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<AccountAdminResponse>>()
        .unwrap()
}
fn query(e: &PluginEngine, r: AccountRequest) -> LocalResult<AccountResponse> {
    *e.services()
        .bind(&key("query"))
        .unwrap()
        .call(Box::new(r), TraceContext::default())
        .unwrap()
        .downcast::<LocalResult<AccountResponse>>()
        .unwrap()
}
fn execution(
    e: &PluginEngine,
    r: AccountExecutionRequest,
) -> LocalResult<AccountExecutionResponse> {
    *e.services()
        .bind(&key("execution"))
        .unwrap()
        .call(
            Box::new(r),
            TraceContext {
                trace_id: 9,
                causation_id: 8,
            },
        )
        .unwrap()
        .downcast::<LocalResult<AccountExecutionResponse>>()
        .unwrap()
}

fn engine_with_plugin(event_engine: &EventEngine) -> PluginEngine {
    let mut p = PluginEngine::new(Arc::new(event_engine.handle()), ApiVersion::new(1, 0)).unwrap();
    p.register(
        Arc::new(
            AccountPluginFactory::new()
                .with_factory(Arc::new(FakeFactory))
                .with_secret_provider(Arc::new(TestSecrets)),
        ),
        Version::new(1, 0, 0),
        "test",
    )
    .unwrap();
    p.apply(&[spec()]).unwrap();
    p
}

#[test]
fn decimal_units_and_abi_are_exact_and_little_endian() {
    assert_eq!(
        "0.0010".parse::<DecimalUnit>().unwrap().to_string(),
        "0.001"
    );
    assert!("0.0000000000000000001".parse::<DecimalUnit>().is_err());
    let event = OrderChangedV1 {
        header: AccountEventHeaderV1 {
            account_id: 7,
            kind: event_kind::ORDER_CHANGED,
            account_generation: 9,
            account_epoch: 3,
            account_version: 4,
            exchange_ts: -5,
            receive_ts: 6,
            ..Default::default()
        },
        asset_id: 1001,
        price_ticks: -2,
        quantity_lots: 3,
        client_order_id: Id128([1; 16]),
        ..Default::default()
    };
    let mut bytes = vec![0; OrderChangedV1::ENCODED_LEN];
    event.encode_into(&mut bytes).unwrap();
    assert_eq!(&bytes[..4], &7u32.to_le_bytes());
    assert_eq!(&bytes[56..64], &(-2i64).to_le_bytes());
    assert_eq!(OrderChangedV1::decode(&bytes).unwrap(), event);
    let pos = PositionChangedV1::default();
    let mut p = vec![0; PositionChangedV1::ENCODED_LEN];
    pos.encode_into(&mut p).unwrap();
    assert_eq!(PositionChangedV1::decode(&p).unwrap(), pos);
}

struct RecordingHandler(std::sync::mpsc::Sender<(String, Vec<u8>)>);
impl EventHandler for RecordingHandler {
    fn handle(&self, e: EventView<'_>) -> Result<(), PluginError> {
        self.0
            .send((e.event_type.to_string(), e.payload.to_vec()))
            .unwrap();
        Ok(())
    }
}

#[test]
fn plugin_services_direct_events_generation_and_snapshots_work() {
    let mut c = EventEngineConfig::default();
    c.ingress.max_sources = 5000;
    c.subscribers.default_capacity = 16;
    c.subscribers.critical_reserve = 2;
    let ee = EventEngine::new(c).unwrap();
    let h = ee.handle();
    for t in ACCOUNT_EVENT_TYPES {
        h.register_event(t, 1, EventClass::Critical, PoolKind::SmallEvent)
            .unwrap();
    }
    ee.start().unwrap();
    let tx = h.begin_route_update(h.current_route_version()).unwrap();
    h.stage_subscription(
        tx,
        &PluginIdentity::new("test", "consumer"),
        &SubscriptionSpec {
            event_type: Arc::from(ORDER_CHANGED_EVENT),
            schema_version: 1,
            qos: EventQos::ReliableOrdered,
            capacity: 8,
            routing_keys: Arc::from([2001]),
        },
    )
    .unwrap();
    let (_, mut subscriptions) = h.commit_at_safe_point(tx).unwrap();
    let receiver = subscriptions.pop().unwrap().receiver;
    let (out_tx, out_rx) = std::sync::mpsc::channel();
    let consumer = std::thread::spawn(move || {
        let handler = RecordingHandler(out_tx);
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if receiver
                .dispatch_next(&handler, Duration::from_millis(10))
                .unwrap()
                == DispatchOutcome::Delivered
            {
                return;
            }
        }
        panic!("account event not delivered")
    });
    let mut pe = engine_with_plugin(&ee);
    assert!(
        matches!(admin(&pe,AccountAdminRequest::List).unwrap(),AccountAdminResponse::Accounts(v) if v.is_empty())
    );
    let handle = match admin(&pe, AccountAdminRequest::Create(definition("main", 2001))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(
        matches!(query(&pe,AccountRequest::Resolve(Arc::from("main"))).unwrap(),AccountResponse::Handle(h) if h==handle)
    );
    admin(&pe, AccountAdminRequest::Start(handle)).unwrap();
    let (event_type, payload) = out_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(event_type, ORDER_CHANGED_EVENT);
    assert_eq!(
        OrderChangedV1::decode(&payload)
            .unwrap()
            .header
            .account_generation,
        handle.generation
    );
    consumer.join().unwrap();
    let command = SubmitOrderCommand {
        command_id: Id128([1; 16]),
        client_order_id: Some(Id128([2; 16])),
        asset_id: AssetId(1001),
        side: 1,
        order_type: 1,
        time_in_force: 1,
        price_ticks: 10,
        quantity_lots: 2,
        trace: TraceContext::default(),
    };
    let receipt = execution(
        &pe,
        AccountExecutionRequest::Submit(handle, command.clone()),
    )
    .unwrap()
    .0;
    assert_eq!(receipt.account, handle);
    assert_eq!(
        execution(
            &pe,
            AccountExecutionRequest::Submit(handle, command.clone())
        )
        .unwrap()
        .0,
        receipt
    );
    let mut conflict = command;
    conflict.price_ticks = 11;
    assert_eq!(
        execution(&pe, AccountExecutionRequest::Submit(handle, conflict))
            .unwrap_err()
            .kind,
        AccountErrorKind::CommandConflict
    );
    let op = match admin(
        &pe,
        AccountAdminRequest::Reconcile(handle, ReconcileScope::Full),
    )
    .unwrap()
    {
        AccountAdminResponse::OperationId(id) => id,
        _ => panic!(),
    };
    assert!(
        matches!(admin(&pe,AccountAdminRequest::Operation(op)).unwrap(),AccountAdminResponse::Operation(v) if v.state==OperationState::Pending)
    );
    assert!(
        matches!(query(&pe,AccountRequest::Orders(handle,OrderFilter::default())).unwrap(),AccountResponse::Orders(v) if v.state==AccountSnapshotState::Reconciling && v.committed_epoch==Some(1))
    );
    assert!(
        matches!(admin(&pe,AccountAdminRequest::Operation(op)).unwrap(),AccountAdminResponse::Operation(v) if v.state==OperationState::Succeeded)
    );
    let mut active_replacement = definition("main", 2001);
    active_replacement.definition_version = 2;
    let active_replaced = match admin(
        &pe,
        AccountAdminRequest::Replace(handle, active_replacement),
    )
    .unwrap()
    {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(active_replaced.generation > handle.generation);
    assert_eq!(
        query(&pe, AccountRequest::Health(handle)).unwrap_err().kind,
        AccountErrorKind::StaleHandle
    );
    admin(
        &pe,
        AccountAdminRequest::Stop(active_replaced, Instant::now() + Duration::from_secs(1)),
    )
    .unwrap();
    admin(&pe, AccountAdminRequest::Remove(active_replaced)).unwrap();
    let recreated = match admin(&pe, AccountAdminRequest::Create(definition("main", 2001))).unwrap()
    {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(recreated.generation > active_replaced.generation);
    assert_eq!(
        query(&pe, AccountRequest::Health(handle)).unwrap_err().kind,
        AccountErrorKind::StaleHandle
    );
    let mut replacement = definition("main", 2001);
    replacement.definition_version = 2;
    let replaced = match admin(&pe, AccountAdminRequest::Replace(recreated, replacement)).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert!(replaced.generation > recreated.generation);
    pe.shutdown(StopReason::Shutdown).unwrap();
    ee.stop().unwrap();
}

#[test]
fn validation_capacity_redaction_and_error_passthrough_work() {
    assert_eq!(
        format!("{:?}", SecretRef::new("secret://sensitive/path")),
        "SecretRef(REDACTED)"
    );
    let ee = EventEngine::new(EventEngineConfig::default()).unwrap();
    for event_type in ACCOUNT_EVENT_TYPES {
        ee.handle()
            .register_event(event_type, 1, EventClass::Critical, PoolKind::SmallEvent)
            .unwrap();
    }
    ee.start().unwrap();
    let mut pe = engine_with_plugin(&ee);
    let mut invalid = definition("bad", 1);
    invalid.instruments = Arc::from([
        invalid.instruments[0].clone(),
        invalid.instruments[0].clone(),
    ]);
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(invalid))
            .unwrap_err()
            .kind,
        AccountErrorKind::InvalidDefinition
    );
    let h1 = match admin(&pe, AccountAdminRequest::Create(definition("one", 1))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    let mut duplicate = definition("two", 1);
    duplicate.account_id = AccountId(1);
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(duplicate))
            .unwrap_err()
            .kind,
        AccountErrorKind::AlreadyExists
    );
    let h2 = match admin(&pe, AccountAdminRequest::Create(definition("two", 2))).unwrap() {
        AccountAdminResponse::Handle(h) => h,
        _ => panic!(),
    };
    assert_eq!(
        admin(&pe, AccountAdminRequest::Create(definition("three", 3)))
            .unwrap_err()
            .kind,
        AccountErrorKind::CapacityExceeded
    );
    assert_eq!(
        execution(
            &pe,
            AccountExecutionRequest::Submit(
                h1,
                SubmitOrderCommand {
                    command_id: Id128([3; 16]),
                    client_order_id: None,
                    asset_id: AssetId(1001),
                    side: 1,
                    order_type: 1,
                    time_in_force: 1,
                    price_ticks: 1,
                    quantity_lots: 1,
                    trace: TraceContext::default()
                }
            )
        )
        .unwrap_err()
        .kind,
        AccountErrorKind::NotReady
    );
    admin(&pe, AccountAdminRequest::Start(h1)).unwrap();
    admin(&pe, AccountAdminRequest::Start(h2)).unwrap();
    let reconcile_id = |account| match admin(
        &pe,
        AccountAdminRequest::Reconcile(account, ReconcileScope::Full),
    )
    .unwrap()
    {
        AccountAdminResponse::OperationId(id) => id,
        _ => panic!(),
    };
    let first_operation = reconcile_id(h1);
    let second_operation = reconcile_id(h2);
    assert_ne!(first_operation, second_operation);
    assert!(matches!(
        admin(&pe, AccountAdminRequest::Operation(first_operation)).unwrap(),
        AccountAdminResponse::Operation(value) if value.state == OperationState::Pending
    ));
    let q = SubmitOrderCommand {
        command_id: Id128([255; 16]),
        client_order_id: None,
        asset_id: AssetId(1001),
        side: 1,
        order_type: 1,
        time_in_force: 1,
        price_ticks: 1,
        quantity_lots: 1,
        trace: TraceContext::default(),
    };
    assert_eq!(
        execution(&pe, AccountExecutionRequest::Submit(h1, q))
            .unwrap_err()
            .kind,
        AccountErrorKind::QueueFull
    );
    pe.shutdown(StopReason::Shutdown).unwrap();
    ee.stop().unwrap();
}

#[test]
fn duplicate_factory_is_rejected() {
    let core = AccountPluginCore::new(AccountPluginConfig::default());
    core.register_factory(Arc::new(FakeFactory)).unwrap();
    assert_eq!(
        core.register_factory(Arc::new(FakeFactory))
            .unwrap_err()
            .kind,
        AccountErrorKind::AlreadyExists
    );
}

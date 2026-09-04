use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use titan_account_plugin::*;
use titan_event_engine::*;
use titan_market_plugin::{self as market, *};
use titan_plugin_engine::{ApiVersion, EventQos, EventView, TraceContext};
use titan_runtime::{CallbackRegistry, StrategyEventKind, StrategyRuntimeContext};
use titan_runtime_abi::{
    BAR_COMPLETE, Bar, ORDER_COMMAND_CANCEL, ORDER_COMMAND_SUBMIT, OrderCommand,
};

use super::*;

static STARTS: AtomicUsize = AtomicUsize::new(0);
static TICKS: AtomicUsize = AtomicUsize::new(0);
static STOPS: AtomicUsize = AtomicUsize::new(0);
static FAIL_START: AtomicBool = AtomicBool::new(false);

unsafe extern "C" fn on_start(_: *mut StrategyRuntimeContext) -> i32 {
    STARTS.fetch_add(1, Ordering::SeqCst);
    if FAIL_START.load(Ordering::SeqCst) {
        -1
    } else {
        0
    }
}

unsafe extern "C" fn on_tick(context: *mut StrategyRuntimeContext) -> i32 {
    let context = unsafe { &mut *context };
    TICKS.fetch_add(1, Ordering::SeqCst);
    if context.command_capacity > 0 {
        unsafe {
            *context.commands_ptr = OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 1,
                time_in_force: 0,
                order_type: 0,
                local_account_no: 0,
                asset_no: 0,
                order_id: TICKS.load(Ordering::SeqCst) as u64,
                price: 10.0,
                qty: 2.0,
                ..OrderCommand::default()
            };
        }
        context.num_commands = 1;
    }
    0
}

unsafe extern "C" fn on_stop(_: *mut StrategyRuntimeContext) -> i32 {
    STOPS.fetch_add(1, Ordering::SeqCst);
    0
}

#[derive(Clone)]
struct FakeLoaderFactory {
    manifest: StrategyPackageManifest,
}
impl StrategyPackageLoaderFactory for FakeLoaderFactory {
    fn loader_type(&self) -> &str {
        "rust-static"
    }
    fn create(
        &self,
        _: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError> {
        Ok(Arc::new(FakeLoader {
            manifest: self.manifest.clone(),
        }))
    }
}
struct FakeLoader {
    manifest: StrategyPackageManifest,
}
impl StrategyPackageLoader for FakeLoader {
    fn inspect(&self, _: &StrategyPackageRef) -> Result<StrategyPackageManifest, StrategyError> {
        Ok(self.manifest.clone())
    }
    fn load(&self, _: StrategyLoadRequest, _: Instant) -> Result<StrategyArtifact, StrategyError> {
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Start, on_start);
        callbacks.set(StrategyEventKind::Tick, on_tick);
        callbacks.set(StrategyEventKind::Stop, on_stop);
        Ok(StrategyArtifact {
            id: StrategyArtifactId {
                digest: self.manifest.artifact_digest,
            },
            manifest: self.manifest.clone(),
            callbacks,
            state: StrategyStateMemory::default(),
            code_lease: StrategyCodeLease::default(),
        })
    }
}

struct FakeMarket;
impl MarketService for FakeMarket {
    fn resolve(&self, _: &str) -> market::LocalResult<MarketSourceHandle> {
        Ok(MarketSourceHandle {
            source_id: MarketSourceId(1),
            generation: 1,
        })
    }
    fn subscribe(
        &self,
        _: MarketSourceHandle,
        _: MarketSubscribeRequest,
    ) -> market::LocalResult<MarketSubscription> {
        unimplemented!()
    }
    fn unsubscribe(
        &self,
        _: MarketSourceHandle,
        _: MarketSubscription,
    ) -> market::LocalResult<market::OperationId> {
        unimplemented!()
    }
    fn request_snapshot(
        &self,
        _: MarketSourceHandle,
        _: market::AssetId,
    ) -> market::LocalResult<market::OperationId> {
        unimplemented!()
    }
    fn instruments(&self, _: MarketSourceHandle) -> market::LocalResult<Arc<[InstrumentSnapshot]>> {
        Ok(Arc::from([]))
    }
    fn health(&self, _: MarketSourceHandle) -> market::LocalResult<ConnectorHealthSnapshot> {
        Ok(ConnectorHealthSnapshot {
            state: ConnectorHealth::Running,
            message: Arc::from("ready"),
            observed_at: SystemTime::now(),
        })
    }
    fn operation(
        &self,
        _: MarketSourceHandle,
        _: market::OperationId,
    ) -> market::LocalResult<ConnectorOperationSnapshot> {
        unimplemented!()
    }
}

struct FakeAccount;
impl AccountService for FakeAccount {
    fn resolve(&self, _: &str) -> titan_account_plugin::LocalResult<AccountHandle> {
        Ok(AccountHandle {
            account_id: AccountId(7),
            generation: 1,
        })
    }
    fn orders(
        &self,
        _: AccountHandle,
        _: OrderFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<OrderSnapshot>> {
        unimplemented!()
    }
    fn positions(
        &self,
        _: AccountHandle,
        _: PositionFilter,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<PositionSnapshot>> {
        unimplemented!()
    }
    fn balances(
        &self,
        _: AccountHandle,
    ) -> titan_account_plugin::LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
        unimplemented!()
    }
    fn health(
        &self,
        _: AccountHandle,
    ) -> titan_account_plugin::LocalResult<AccountConnectorHealthSnapshot> {
        Ok(AccountConnectorHealthSnapshot {
            state: AccountLifecycle::Ready,
            message: Arc::from("ready"),
            observed_at: SystemTime::now(),
        })
    }
    fn diagnostics(
        &self,
        _: AccountHandle,
    ) -> titan_account_plugin::LocalResult<AccountConnectorDiagnosticSnapshot> {
        unimplemented!()
    }
}

struct FakeExecution {
    calls: Arc<AtomicUsize>,
    last_trace_id: Arc<AtomicU64>,
}
impl AccountExecutionService for FakeExecution {
    fn submit(
        &self,
        account: AccountHandle,
        command: SubmitOrderCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.last_trace_id
            .store(command.trace.trace_id, Ordering::SeqCst);
        Ok(AccountCommandReceipt {
            account,
            command_id: command.command_id,
            client_order_id: command.client_order_id,
            accepted_at: 1,
        })
    }
    fn amend(
        &self,
        _: AccountHandle,
        _: AmendOrderCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        unimplemented!()
    }
    fn cancel(
        &self,
        account: AccountHandle,
        command: CancelOrderCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.last_trace_id
            .store(command.trace.trace_id, Ordering::SeqCst);
        Ok(AccountCommandReceipt {
            account,
            command_id: command.command_id,
            client_order_id: command.client_order_id,
            accepted_at: 1,
        })
    }
    fn cancel_all(
        &self,
        _: AccountHandle,
        _: CancelAllCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        unimplemented!()
    }
    fn cancel_all_after(
        &self,
        _: AccountHandle,
        _: CancelAllAfterCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        unimplemented!()
    }
}

fn definition(version: u64) -> StrategyDefinition {
    StrategyDefinition {
        strategy_key: Arc::from("integration"),
        strategy_id: StrategyId(100),
        package: StrategyPackageRef {
            loader_type: Arc::from("rust-static"),
            uri: Arc::from("static://integration"),
            expected_digest: [7; 32],
            signature_ref: None,
        },
        entrypoint: Arc::from("integration"),
        parameters: Arc::from(br#"{"size":1}"#.as_slice()),
        parameter_schema_version: 1,
        markets: Arc::from([StrategyMarketBinding {
            local_market_no: 0,
            local_asset_no: 0,
            source_key: Arc::from("market"),
            asset_id: 1,
            data_mode: StrategyDataMode::Tick,
        }]),
        accounts: Arc::from([StrategyAccountBinding {
            local_account_no: 0,
            account_key: Arc::from("account"),
            tradable_assets: Arc::from([StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
        }]),
        subscriptions: Arc::from([StrategySubscriptionSpec {
            event_type: Arc::from(DEPTH_BATCH_EVENT),
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        }]),
        risk_scope: RiskScopeRef(Arc::from("scope")),
        runtime: StrategyRuntimeSpec {
            async_lane_capacity: 16,
            critical_reserve: 2,
            reliable_pending_capacity: 4,
            command_capacity: 4,
            state_f64_capacity: 4,
            state_i64_capacity: 4,
            ..StrategyRuntimeSpec::default()
        },
        recovery: StrategyRecoveryPolicy::Fresh,
        shutdown: StrategyShutdownPolicy::LeaveOwnedOrders,
        enabled: true,
        definition_version: version,
    }
}

fn market_tick_batch_payload() -> Vec<u8> {
    encode_depth_batch(
        MarketBatchHeaderV1 {
            asset_id: 1,
            stream_epoch: 1,
            first_update_sequence: 1,
            last_update_sequence: 1,
            ..MarketBatchHeaderV1::default()
        },
        &[DepthItemV1 {
            price_ticks: 100,
            quantity_lots: 1,
            side: 1,
            action: 1,
            ..DepthItemV1::default()
        }],
    )
    .unwrap()
}

fn wait_operation(core: &StrategyPluginCore, id: StrategyOperationId) -> StrategyOperationSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = core.operation(id);
        if value.state != StrategyOperationState::Pending {
            return value;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn lifecycle_gateway_replace_and_stale_handle_contract() {
    STARTS.store(0, Ordering::SeqCst);
    TICKS.store(0, Ordering::SeqCst);
    STOPS.store(0, Ordering::SeqCst);
    FAIL_START.store(false, Ordering::SeqCst);
    let mut event_config = EventEngineConfig::default();
    event_config.arena.small_event.slots = 64;
    event_config.arena.small_event.block_bytes = 128;
    event_config.arena.small_event.low_watermark = 4;
    let engine = EventEngine::new(event_config).unwrap();
    let events = engine.handle();
    events
        .register_event(
            DEPTH_BATCH_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
    engine.start().unwrap();

    let manifest = StrategyPackageManifest {
        strategy_type: Arc::from("native-test"),
        package_version: semver::Version::new(1, 0, 0),
        runtime_abi: ApiVersion::new(9, 0),
        parameter_schema: Arc::new(serde_json::json!({"type":"object","required":["size"]})),
        parameter_schema_version: 1,
        state_schema_version: 1,
        callbacks: StrategyCallbackMask(u32::MAX),
        capabilities: StrategyCapabilities(
            StrategyCapabilities::READ_TICK.0
                | StrategyCapabilities::READ_DEPTH.0
                | StrategyCapabilities::SUBMIT_ORDER.0,
        ),
        artifact_digest: [7; 32],
    };
    let loaders = Arc::new(StrategyPackageLoaderRegistry::default());
    loaders
        .register(Arc::new(FakeLoaderFactory {
            manifest: manifest.clone(),
        }))
        .unwrap();
    let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
    runtimes
        .register(Arc::new(NativeStrategyRuntimeFactory::new("native-test")))
        .unwrap();
    let execution_calls = Arc::new(AtomicUsize::new(0));
    let execution_trace = Arc::new(AtomicU64::new(0));
    let core = StrategyPluginCore::new(
        StrategyPluginConfig::default(),
        StrategyPluginDependencies {
            events: events.clone(),
            markets: Arc::new(FakeMarket),
            accounts: Arc::new(FakeAccount),
            execution: Arc::new(FakeExecution {
                calls: execution_calls.clone(),
                last_trace_id: execution_trace.clone(),
            }),
        },
        loaders,
        runtimes,
    )
    .unwrap();

    let handle = core.create(definition(1)).unwrap();
    assert_eq!(handle.generation, 1);
    let prepare = core.prepare(handle).unwrap();
    let start = core.start(handle).unwrap();
    assert_ne!(prepare, start);
    assert_eq!(
        wait_operation(&core, prepare).state,
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait_operation(&core, start).state,
        StrategyOperationState::Succeeded
    );
    let payload = market_tick_batch_payload();
    let mut publish = PublishRequest::new(DEPTH_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, &payload);
    publish.routing_key = 1;
    publish.trace = TraceContext {
        trace_id: 91,
        causation_id: 37,
    };
    events.try_publish(publish).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while execution_calls.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(execution_calls.load(Ordering::SeqCst), 1);
    assert_eq!(execution_trace.load(Ordering::SeqCst), 91);
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);

    let mut recovery_requested = definition(1);
    recovery_requested.strategy_key = Arc::from("recovery-requested");
    recovery_requested.strategy_id = StrategyId(101);
    recovery_requested.recovery = StrategyRecoveryPolicy::RequireCheckpoint;
    assert_eq!(
        core.create(recovery_requested).unwrap_err().kind,
        StrategyErrorKind::UnsupportedCapability
    );

    let pause = core.pause(handle, PauseReason::User).unwrap();
    assert_eq!(
        wait_operation(&core, pause).state,
        StrategyOperationState::Succeeded
    );
    let payload = market_tick_batch_payload();
    let mut paused = PublishRequest::new(DEPTH_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, &payload);
    paused.routing_key = 1;
    events.try_publish(paused).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);

    let starts_before_invalid_start = STARTS.load(Ordering::SeqCst);
    let invalid_start = core.start(handle).unwrap();
    assert_eq!(
        wait_operation(&core, invalid_start).state,
        StrategyOperationState::Failed
    );
    assert_eq!(STARTS.load(Ordering::SeqCst), starts_before_invalid_start);

    let resume = core.resume(handle).unwrap();
    assert_eq!(
        wait_operation(&core, resume).state,
        StrategyOperationState::Succeeded
    );
    FAIL_START.store(true, Ordering::SeqCst);
    assert!(core.replace(handle, definition(2)).is_err());
    FAIL_START.store(false, Ordering::SeqCst);
    assert_eq!(core.resolve("integration").unwrap(), handle);
    let old_after_rollback = core.state(handle).unwrap();
    assert_eq!(old_after_rollback.lifecycle, StrategyLifecycle::Running);
    assert!(old_after_rollback.command_gate_open);

    let replacement = core.replace(handle, definition(3)).unwrap();
    assert_eq!(replacement.generation, 2);
    assert_eq!(
        core.state(handle).unwrap_err().kind,
        StrategyErrorKind::StaleHandle
    );
    assert_eq!(core.resolve("integration").unwrap(), replacement);

    let stop = core
        .stop(replacement, Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        wait_operation(&core, stop).state,
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        core.state(replacement).unwrap().lifecycle,
        StrategyLifecycle::Stopped
    );
    let remove = core.remove(replacement).unwrap();
    assert_eq!(
        core.operation(remove).state,
        StrategyOperationState::Succeeded
    );
    assert!(STOPS.load(Ordering::SeqCst) >= 2);
    engine.stop().unwrap();
}

#[test]
fn definition_rejects_non_contiguous_bindings_before_loading() {
    let mut value = definition(1);
    value.markets = Arc::from([StrategyMarketBinding {
        local_market_no: 1,
        local_asset_no: 0,
        source_key: Arc::from("market"),
        asset_id: 1,
        data_mode: StrategyDataMode::Tick,
    }]);
    let config = StrategyPluginConfig::default();
    let error = super::plugin::validate_definition(&config, &value).unwrap_err();
    assert_eq!(error.kind, StrategyErrorKind::InvalidDefinition);
}

#[test]
fn definition_rejects_unimplemented_canonical_subscriptions() {
    let config = StrategyPluginConfig::default();
    let unsupported: &[(&str, u32)] = &[
        (POSITION_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
        (BALANCE_CHANGED_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
        (COMMAND_RESULT_EVENT, ACCOUNT_EVENT_SCHEMA_VERSION),
        (FUNDING_RATE_EVENT, MARKET_EVENT_SCHEMA_VERSION),
        (MARK_PRICE_EVENT, MARKET_EVENT_SCHEMA_VERSION),
        (TICKER_EVENT, MARKET_EVENT_SCHEMA_VERSION),
        (
            "titan.account.StreamStateChanged",
            ACCOUNT_EVENT_SCHEMA_VERSION,
        ),
    ];
    for (event_type, schema_version) in unsupported {
        let mut value = definition(1);
        value.subscriptions = Arc::from([StrategySubscriptionSpec {
            event_type: Arc::from(*event_type),
            schema_version: *schema_version,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        }]);
        let error = super::plugin::validate_definition(&config, &value).unwrap_err();
        assert_eq!(
            error.kind,
            StrategyErrorKind::UnsupportedCapability,
            "{event_type} should be rejected"
        );
    }
}

#[test]
fn manifest_rejects_unimplemented_command_capabilities() {
    let mut value = definition(1);
    value.parameters = Arc::from(b"{}".as_slice());
    for capabilities in [
        StrategyCapabilities::SCHEDULE_TIMER,
        StrategyCapabilities::AMEND_ORDER,
    ] {
        let manifest = StrategyPackageManifest {
            strategy_type: Arc::from("command-test"),
            package_version: semver::Version::new(1, 0, 0),
            runtime_abi: ApiVersion::new(9, 0),
            parameter_schema: Arc::new(serde_json::json!({"type":"object"})),
            parameter_schema_version: 1,
            state_schema_version: 1,
            callbacks: StrategyCallbackMask(u32::MAX),
            capabilities,
            artifact_digest: [7; 32],
        };
        let error =
            super::plugin::validate_manifest(&StrategyPluginConfig::default(), &value, &manifest)
                .unwrap_err();
        assert_eq!(error.kind, StrategyErrorKind::UnsupportedCapability);
    }
}

#[test]
fn adapter_fails_fast_on_unimplemented_canonical_facts() {
    let adapter = CanonicalStrategyEventAdapter::new(&[]);
    let mut payload = vec![0_u8; BalanceChangedV1::ENCODED_LEN];
    BalanceChangedV1 {
        header: AccountEventHeaderV1 {
            account_id: 7,
            ..AccountEventHeaderV1::default()
        },
        currency_id: 1,
        wallet_units: 1_000,
        available_units: 900,
        margin_units: 100,
        unrealized_pnl_units: 5,
    }
    .encode_into(&mut payload)
    .unwrap();
    let event = EventView {
        event_type: BALANCE_CHANGED_EVENT,
        schema_version: ACCOUNT_EVENT_SCHEMA_VERSION,
        payload: &payload,
        trace: TraceContext::default(),
    };
    let error = adapter
        .invoke(
            event,
            &CallbackRegistry::default(),
            &mut StrategyRuntimeContext::default(),
        )
        .unwrap_err();
    assert_eq!(error.reason_code.as_ref(), "unsupported_canonical_event");
}

#[test]
fn cancel_accepts_zero_quantity_and_ignores_submit_only_numeric_fields() {
    let strategy = StrategyHandle {
        strategy_id: StrategyId(11),
        generation: 2,
    };
    let gate = Arc::new(StrategyCommandGate::new(strategy));
    gate.open();
    let execution_calls = Arc::new(AtomicUsize::new(0));
    let gateway = StandardStrategyCommandGateway::new(
        strategy,
        StrategyCapabilities::CANCEL_ORDER,
        gate,
        &[ResolvedAccountBinding {
            local_account_no: 0,
            account: AccountHandle {
                account_id: AccountId(7),
                generation: 1,
            },
            tradable_assets: Arc::from([StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
        }],
        Arc::new(FakeExecution {
            calls: execution_calls.clone(),
            last_trace_id: Arc::new(AtomicU64::new(0)),
        }),
    );
    gateway
        .execute(
            strategy,
            OrderCommand {
                kind: ORDER_COMMAND_CANCEL,
                local_account_no: 0,
                asset_no: 0,
                order_id: 42,
                price: f64::NAN,
                qty: 0.0,
                ..OrderCommand::default()
            },
            TraceContext::default(),
        )
        .unwrap();
    assert_eq!(execution_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn gateway_rejects_invalid_submit_enum_fields_before_execution() {
    let strategy = StrategyHandle {
        strategy_id: StrategyId(12),
        generation: 1,
    };
    let gate = Arc::new(StrategyCommandGate::new(strategy));
    gate.open();
    let execution_calls = Arc::new(AtomicUsize::new(0));
    let gateway = StandardStrategyCommandGateway::new(
        strategy,
        StrategyCapabilities::SUBMIT_ORDER,
        gate,
        &[ResolvedAccountBinding {
            local_account_no: 0,
            account: AccountHandle {
                account_id: AccountId(7),
                generation: 1,
            },
            tradable_assets: Arc::from([StrategyTradableAsset {
                local_asset_no: 0,
                asset_id: 1,
            }]),
        }],
        Arc::new(FakeExecution {
            calls: execution_calls.clone(),
            last_trace_id: Arc::new(AtomicU64::new(0)),
        }),
    );
    let cases = [
        (
            "invalid_side",
            OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 0,
                time_in_force: 0,
                order_type: 0,
                local_account_no: 0,
                asset_no: 0,
                order_id: 7,
                price: 10.0,
                qty: 1.0,
                ..OrderCommand::default()
            },
        ),
        (
            "invalid_order_type",
            OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 1,
                time_in_force: 0,
                order_type: 3,
                local_account_no: 0,
                asset_no: 0,
                order_id: 8,
                price: 10.0,
                qty: 1.0,
                ..OrderCommand::default()
            },
        ),
        (
            "invalid_time_in_force",
            OrderCommand {
                kind: ORDER_COMMAND_SUBMIT,
                side: 1,
                time_in_force: 7,
                order_type: 0,
                local_account_no: 0,
                asset_no: 0,
                order_id: 9,
                price: 10.0,
                qty: 1.0,
                ..OrderCommand::default()
            },
        ),
    ];
    for (reason, command) in cases {
        let error = gateway
            .execute(strategy, command, TraceContext::default())
            .unwrap_err();
        assert_eq!(error.reason_code.as_ref(), reason);
    }
    assert_eq!(execution_calls.load(Ordering::SeqCst), 0);
}

struct ChainSecrets;
impl SecretProvider for ChainSecrets {
    fn resolve(&self, _: &SecretRef) -> Result<SecretValue, AccountConnectorError> {
        Ok(SecretValue::new(b"test-only".to_vec()))
    }
}

static CHAIN_TICKS: AtomicUsize = AtomicUsize::new(0);
static CHAIN_ORDERS: AtomicUsize = AtomicUsize::new(0);
static CHAIN_FILLS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn chain_on_tick(context: *mut StrategyRuntimeContext) -> i32 {
    let context = unsafe { &mut *context };
    CHAIN_TICKS.fetch_add(1, Ordering::SeqCst);
    unsafe {
        *context.commands_ptr = OrderCommand {
            kind: ORDER_COMMAND_SUBMIT,
            side: 1,
            local_account_no: 0,
            asset_no: 0,
            order_id: 44,
            price: 10.0,
            qty: 2.0,
            ..OrderCommand::default()
        };
    }
    context.num_commands = 1;
    0
}

unsafe extern "C" fn chain_on_order(_: *mut StrategyRuntimeContext) -> i32 {
    CHAIN_ORDERS.fetch_add(1, Ordering::SeqCst);
    0
}
unsafe extern "C" fn chain_on_filled(_: *mut StrategyRuntimeContext) -> i32 {
    CHAIN_FILLS.fetch_add(1, Ordering::SeqCst);
    0
}
unsafe extern "C" fn chain_noop(_: *mut StrategyRuntimeContext) -> i32 {
    0
}

struct ChainLoaderFactory(StrategyPackageManifest);
impl StrategyPackageLoaderFactory for ChainLoaderFactory {
    fn loader_type(&self) -> &str {
        "rust-static"
    }
    fn create(
        &self,
        _: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError> {
        Ok(Arc::new(ChainLoader(self.0.clone())))
    }
}
struct ChainLoader(StrategyPackageManifest);
impl StrategyPackageLoader for ChainLoader {
    fn inspect(&self, _: &StrategyPackageRef) -> Result<StrategyPackageManifest, StrategyError> {
        Ok(self.0.clone())
    }
    fn load(&self, _: StrategyLoadRequest, _: Instant) -> Result<StrategyArtifact, StrategyError> {
        let mut callbacks = CallbackRegistry::default();
        callbacks.set(StrategyEventKind::Start, chain_noop);
        callbacks.set(StrategyEventKind::Tick, chain_on_tick);
        callbacks.set(StrategyEventKind::Order, chain_on_order);
        callbacks.set(StrategyEventKind::Filled, chain_on_filled);
        callbacks.set(StrategyEventKind::Stop, chain_noop);
        Ok(StrategyArtifact {
            id: StrategyArtifactId {
                digest: self.0.artifact_digest,
            },
            manifest: self.0.clone(),
            callbacks,
            state: StrategyStateMemory::default(),
            code_lease: StrategyCodeLease::default(),
        })
    }
}

struct ChainAccountConnector {
    context: AccountConnectorContext,
    running: AtomicBool,
    submits: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    last_trace_id: Arc<AtomicU64>,
}

impl ChainAccountConnector {
    fn empty_snapshot<T>(&self) -> AccountStateSnapshot<T> {
        AccountStateSnapshot {
            account: self.context.account,
            state: AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(4),
            captured_at: 1,
            items: Arc::from([]),
        }
    }

    fn publish_facts(&self, command: &SubmitOrderCommand) -> Result<(), AccountConnectorError> {
        let header = |kind, account_version| AccountEventHeaderV1 {
            account_id: self.context.account.account_id.0,
            kind,
            account_generation: self.context.account.generation,
            account_epoch: 1,
            account_version,
            exchange_ts: 51,
            receive_ts: 52,
            ..Default::default()
        };
        let client_order_id = command.client_order_id.unwrap();
        let venue_order_id = Id128([12; 16]);
        let publish = |result: Result<(), titan_plugin_engine::PluginError>| {
            result.map_err(|error| AccountConnectorError::rejected(error.to_string()))
        };
        publish(self.context.event_publisher.publish_encoded(
            &OrderChangedV1 {
                header: header(event_kind::ORDER_CHANGED, 1),
                asset_id: command.asset_id.0,
                side: command.side,
                status: 2,
                price_ticks: command.price_ticks,
                quantity_lots: command.quantity_lots,
                filled_quantity_lots: command.quantity_lots,
                average_price_ticks: command.price_ticks,
                client_order_id,
                venue_order_id,
                command_id: command.command_id,
                ..Default::default()
            },
            command.trace,
        ))?;
        publish(self.context.event_publisher.publish_encoded(
            &FillV2 {
                header: header(event_kind::FILL, 2),
                asset_id: command.asset_id.0,
                side: command.side,
                price_ticks: command.price_ticks,
                last_fill_quantity_lots: command.quantity_lots,
                cumulative_filled_quantity_lots: command.quantity_lots,
                trade_id: Id128([13; 16]),
                venue_order_id,
                client_order_id,
                command_id: command.command_id,
                ..Default::default()
            },
            command.trace,
        ))?;
        publish(self.context.event_publisher.publish_encoded(
            &PositionChangedV1 {
                header: header(event_kind::POSITION_CHANGED, 3),
                asset_id: command.asset_id.0,
                quantity_lots: command.quantity_lots,
                entry_price_ticks: command.price_ticks,
                margin_currency_id: 1,
                ..Default::default()
            },
            command.trace,
        ))?;
        publish(self.context.event_publisher.publish_encoded(
            &BalanceChangedV1 {
                header: header(event_kind::BALANCE_CHANGED, 4),
                currency_id: 1,
                wallet_units: 1_000,
                available_units: 900,
                margin_units: 100,
                unrealized_pnl_units: 5,
            },
            command.trace,
        ))
    }
}

impl AccountConnector for ChainAccountConnector {
    fn start(&self) -> Result<(), AccountConnectorError> {
        self.running.store(true, Ordering::Release);
        Ok(())
    }
    fn stop(&self, _: Instant) -> Result<(), AccountConnectorError> {
        self.running.store(false, Ordering::Release);
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn submit(
        &self,
        command: SubmitOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        self.submits.fetch_add(1, Ordering::SeqCst);
        self.last_trace_id
            .store(command.trace.trace_id, Ordering::SeqCst);
        self.publish_facts(&command)?;
        Ok(AccountCommandReceipt {
            account: self.context.account,
            command_id: command.command_id,
            client_order_id: command.client_order_id,
            accepted_at: 50,
        })
    }
    fn amend(&self, _: AmendOrderCommand) -> Result<AccountCommandReceipt, AccountConnectorError> {
        Err(AccountConnectorError::rejected("unused"))
    }
    fn cancel(
        &self,
        _: CancelOrderCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        Err(AccountConnectorError::rejected("unused"))
    }
    fn cancel_all(
        &self,
        _: CancelAllCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        Err(AccountConnectorError::rejected("unused"))
    }
    fn cancel_all_after(
        &self,
        _: CancelAllAfterCommand,
    ) -> Result<AccountCommandReceipt, AccountConnectorError> {
        Err(AccountConnectorError::rejected("unused"))
    }
    fn reconcile(
        &self,
        _: ReconcileScope,
    ) -> Result<titan_account_plugin::OperationId, AccountConnectorError> {
        Ok(titan_account_plugin::OperationId(1))
    }
    fn orders(
        &self,
        _: OrderFilter,
    ) -> Result<AccountStateSnapshot<OrderSnapshot>, AccountConnectorError> {
        Ok(self.empty_snapshot())
    }
    fn positions(
        &self,
        _: PositionFilter,
    ) -> Result<AccountStateSnapshot<PositionSnapshot>, AccountConnectorError> {
        Ok(self.empty_snapshot())
    }
    fn balances(&self) -> Result<AccountStateSnapshot<BalanceSnapshot>, AccountConnectorError> {
        Ok(self.empty_snapshot())
    }
    fn health(&self) -> AccountConnectorHealthSnapshot {
        AccountConnectorHealthSnapshot {
            state: if self.running.load(Ordering::Acquire) {
                AccountLifecycle::Ready
            } else {
                AccountLifecycle::Stopped
            },
            message: Arc::from("chain fake"),
            observed_at: SystemTime::now(),
        }
    }
    fn diagnostics(&self) -> AccountConnectorDiagnosticSnapshot {
        AccountConnectorDiagnosticSnapshot {
            summary: Arc::from("chain fake"),
            external_order_count: 0,
            command_queue_depth: 0,
            account_epoch: 1,
            account_version: 4,
        }
    }
    fn operation(
        &self,
        id: titan_account_plugin::OperationId,
    ) -> AccountConnectorOperationSnapshot {
        AccountConnectorOperationSnapshot {
            id,
            state: titan_account_plugin::OperationState::Succeeded,
            detail: Arc::from("done"),
        }
    }
}

struct ChainAccountFactory {
    submits: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    last_trace_id: Arc<AtomicU64>,
}
impl AccountConnectorFactory for ChainAccountFactory {
    fn connector_type(&self) -> &str {
        "chain-account"
    }
    fn create(
        &self,
        _: &AccountDefinition,
        context: AccountConnectorContext,
    ) -> Result<Arc<dyn AccountConnector>, AccountConnectorError> {
        Ok(Arc::new(ChainAccountConnector {
            context,
            running: AtomicBool::new(false),
            submits: self.submits.clone(),
            stops: self.stops.clone(),
            last_trace_id: self.last_trace_id.clone(),
        }))
    }
}

#[test]
fn strategy_command_reaches_account_connector_and_account_facts_return_to_strategy() {
    use semver::Version;
    use titan_plugin_engine::{
        ConfigSnapshot, ExecutionModel, ExecutionSpec, PluginEngine, PluginSpec, ServiceId,
        ServiceKey, ServiceScope, StopReason, SubscriptionLimits,
    };

    CHAIN_TICKS.store(0, Ordering::SeqCst);
    CHAIN_ORDERS.store(0, Ordering::SeqCst);
    CHAIN_FILLS.store(0, Ordering::SeqCst);
    let event_engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    let events = event_engine.handle();
    events
        .register_event(
            DEPTH_BATCH_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
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
            .unwrap();
    }
    event_engine.start().unwrap();

    let submits = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let last_trace_id = Arc::new(AtomicU64::new(0));
    let mut plugins = PluginEngine::new(Arc::new(events.clone()), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(
                AccountPluginFactory::new()
                    .with_factory(Arc::new(ChainAccountFactory {
                        submits: submits.clone(),
                        stops: stops.clone(),
                        last_trace_id: last_trace_id.clone(),
                    }))
                    .with_secret_provider(Arc::new(ChainSecrets)),
            ),
            Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugins
        .apply(&[PluginSpec {
            instance_id: Arc::from("account"),
            plugin_type: Arc::from(ACCOUNT_PLUGIN_TYPE),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 16,
                allowed_qos: [EventQos::ReliableOrdered].into_iter().collect(),
            },
            service_scopes: vec![
                (
                    ServiceId::new("titan.account", "admin"),
                    ServiceScope::Global,
                ),
                (
                    ServiceId::new("titan.account", "query"),
                    ServiceScope::Global,
                ),
                (
                    ServiceId::new("titan.account", "execution"),
                    ServiceScope::Global,
                ),
            ],
            required_service_scopes: vec![],
        }])
        .unwrap();
    let service_key = |name| ServiceKey {
        id: ServiceId::new("titan.account", name),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    };
    let admin = plugins
        .services()
        .bind_typed::<AccountAdminApi>(&service_key("admin"))
        .unwrap();
    let account = match admin
        .call(
            AccountAdminRequest::Create(AccountDefinition {
                account_key: Arc::from("account"),
                account_id: titan_account_plugin::AccountId(7),
                connector_type: Arc::from("chain-account"),
                credential_ref: SecretRef::new("secret://chain"),
                connector_config: Arc::from([]),
                instruments: Arc::from([AccountInstrumentBinding {
                    native_symbol: Arc::from("BTCUSDT"),
                    asset_id: titan_account_plugin::AssetId(1),
                    price_tick: "1".parse().unwrap(),
                    quantity_lot: "1".parse().unwrap(),
                    contract_multiplier: "1".parse().unwrap(),
                }]),
                currencies: Arc::from([AccountCurrencyBinding {
                    native_currency: Arc::from("USDT"),
                    currency_id: CurrencyId(1),
                    amount_unit: "1".parse().unwrap(),
                }]),
                ownership: OrderOwnershipPolicy::ManagedOnly {
                    client_id_prefix: Arc::from("titan-"),
                },
                shutdown_order_policy: ShutdownOrderPolicy::LeaveOpen,
                enabled: true,
                definition_version: 1,
            }),
            TraceContext::default(),
        )
        .unwrap()
        .unwrap()
    {
        AccountAdminResponse::Handle(value) => value,
        _ => panic!("unexpected account create response"),
    };
    admin
        .call(AccountAdminRequest::Start(account), TraceContext::default())
        .unwrap()
        .unwrap();

    let account_service = PluginAccountService(
        plugins
            .services()
            .bind_typed::<AccountApi>(&service_key("query"))
            .unwrap(),
    );
    let execution_handle = plugins
        .services()
        .bind_typed::<AccountExecutionApi>(&service_key("execution"))
        .unwrap();
    let execution_service = PluginAccountExecutionService(execution_handle.clone());
    let manifest = StrategyPackageManifest {
        strategy_type: Arc::from("account-chain"),
        package_version: Version::new(1, 0, 0),
        runtime_abi: ApiVersion::new(9, 0),
        parameter_schema: Arc::new(serde_json::json!({"type":"object"})),
        parameter_schema_version: 1,
        state_schema_version: 1,
        callbacks: StrategyCallbackMask(u32::MAX),
        capabilities: StrategyCapabilities(
            StrategyCapabilities::READ_TICK.0
                | StrategyCapabilities::READ_DEPTH.0
                | StrategyCapabilities::READ_ACCOUNT.0
                | StrategyCapabilities::SUBMIT_ORDER.0,
        ),
        artifact_digest: [7; 32],
    };
    let loaders = Arc::new(StrategyPackageLoaderRegistry::default());
    loaders
        .register(Arc::new(ChainLoaderFactory(manifest)))
        .unwrap();
    let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
    runtimes
        .register(Arc::new(NativeStrategyRuntimeFactory::new("account-chain")))
        .unwrap();
    let strategy_core = StrategyPluginCore::new(
        StrategyPluginConfig::default(),
        StrategyPluginDependencies {
            events: events.clone(),
            markets: Arc::new(FakeMarket),
            accounts: Arc::new(account_service),
            execution: Arc::new(execution_service),
        },
        loaders,
        runtimes,
    )
    .unwrap();
    let mut strategy_definition = definition(1);
    strategy_definition.package.uri = Arc::from("static://account-chain");
    strategy_definition.entrypoint = Arc::from("account-chain");
    strategy_definition.parameters = Arc::from(b"{}".as_slice());
    strategy_definition.subscriptions = Arc::from([
        StrategySubscriptionSpec {
            event_type: Arc::from(DEPTH_BATCH_EVENT),
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        },
        StrategySubscriptionSpec {
            event_type: Arc::from(ORDER_CHANGED_EVENT),
            schema_version: ACCOUNT_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([7]),
            qos: EventQos::ReliableOrdered,
        },
        StrategySubscriptionSpec {
            event_type: Arc::from(FILL_EVENT),
            schema_version: FILL_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([7]),
            qos: EventQos::ReliableOrdered,
        },
    ]);
    let strategy = strategy_core.create(strategy_definition).unwrap();
    assert_eq!(
        wait_operation(&strategy_core, strategy_core.prepare(strategy).unwrap()).state,
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait_operation(&strategy_core, strategy_core.start(strategy).unwrap()).state,
        StrategyOperationState::Succeeded
    );
    let payload = market_tick_batch_payload();
    let mut trigger = PublishRequest::new(DEPTH_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, &payload);
    trigger.routing_key = 1;
    trigger.trace = TraceContext {
        trace_id: 777,
        causation_id: 700,
    };
    events.try_publish(trigger).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while CHAIN_ORDERS.load(Ordering::SeqCst) == 0 || CHAIN_FILLS.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "account facts did not return to strategy"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(submits.load(Ordering::SeqCst), 1);
    assert_eq!(last_trace_id.load(Ordering::SeqCst), 777);
    assert_eq!(CHAIN_TICKS.load(Ordering::SeqCst), 1);
    assert_eq!(CHAIN_ORDERS.load(Ordering::SeqCst), 1);
    assert_eq!(CHAIN_FILLS.load(Ordering::SeqCst), 1);

    strategy_core
        .quiesce(Instant::now() + Duration::from_secs(1))
        .unwrap();
    let submits_before_shutdown = submits.load(Ordering::SeqCst);
    let payload = market_tick_batch_payload();
    let mut after_quiesce =
        PublishRequest::new(DEPTH_BATCH_EVENT, MARKET_EVENT_SCHEMA_VERSION, &payload);
    after_quiesce.routing_key = 1;
    events.try_publish(after_quiesce).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(submits.load(Ordering::SeqCst), submits_before_shutdown);
    plugins.shutdown(StopReason::Shutdown).unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        execution_handle
            .call(
                AccountExecutionRequest::Submit(
                    account,
                    SubmitOrderCommand {
                        command_id: Id128([99; 16]),
                        client_order_id: None,
                        asset_id: titan_account_plugin::AssetId(1),
                        side: 1,
                        order_type: 1,
                        time_in_force: 1,
                        price_ticks: 1,
                        quantity_lots: 1,
                        trace: TraceContext::default(),
                    },
                ),
                TraceContext::default(),
            )
            .unwrap_err()
            .kind,
        titan_plugin_engine::ErrorKind::ServiceUnavailable
    );
    event_engine.stop().unwrap();
    assert_eq!(event_engine.arena().outstanding_blocks(), 0);
}

#[cfg(feature = "numba-loader")]
struct TriggerMarketConnector {
    context: MarketConnectorContext,
    running: AtomicBool,
    next_operation: AtomicU64,
}

#[cfg(feature = "numba-loader")]
impl TriggerMarketConnector {
    fn publish_tick(&self) {
        let header = MarketBatchHeaderV1 {
            asset_id: 1,
            stream_epoch: 1,
            first_update_sequence: 1,
            last_update_sequence: 1,
            ..MarketBatchHeaderV1::default()
        };
        let payload = encode_depth_batch(
            header,
            &[DepthItemV1 {
                price_ticks: 100,
                quantity_lots: 2,
                side: 1,
                action: 1,
                ..DepthItemV1::default()
            }],
        )
        .unwrap();
        self.context
            .event_publisher
            .publish_market(
                DEPTH_BATCH_EVENT,
                &payload,
                market::AssetId(1),
                10,
                11,
                TraceContext {
                    trace_id: 700,
                    causation_id: 0,
                },
            )
            .unwrap();
    }

    fn publish_bar(&self) {
        let payload = BarBatchV1 {
            timeframe_ns: 60_000_000_000,
            close_ts: 120_000_000_000,
            items: vec![BarRecordV1 {
                asset_id: 1,
                bar: Bar {
                    open_ts: 60_000_000_000,
                    close_ts: 120_000_000_000,
                    open: 100.0,
                    high: 105.0,
                    low: 99.0,
                    close: 104.0,
                    volume: 12.0,
                    quote_volume: 1_224.0,
                    buy_volume: 7.0,
                    trade_count: 8,
                    flags: BAR_COMPLETE,
                },
            }],
        }
        .encode()
        .unwrap();
        self.context
            .event_publisher
            .publish_market(
                BAR_BATCH_EVENT,
                &payload,
                market::AssetId(1),
                120_000_000_000,
                120_000_000_001,
                TraceContext {
                    trace_id: 701,
                    causation_id: 700,
                },
            )
            .unwrap();
    }
}

#[cfg(feature = "numba-loader")]
impl MarketConnector for TriggerMarketConnector {
    fn start(&self) -> Result<(), ConnectorError> {
        self.running.store(true, Ordering::Release);
        Ok(())
    }

    fn stop(&self, _: Instant) -> Result<(), ConnectorError> {
        self.running.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe(&self, _: MarketSubscribeRequest) -> Result<MarketSubscription, ConnectorError> {
        Ok(MarketSubscription {
            id: self.next_operation.fetch_add(1, Ordering::Relaxed),
        })
    }

    fn unsubscribe(&self, _: MarketSubscription) -> Result<market::OperationId, ConnectorError> {
        Ok(market::OperationId(
            self.next_operation.fetch_add(1, Ordering::Relaxed),
        ))
    }

    fn request_snapshot(&self, _: market::AssetId) -> Result<market::OperationId, ConnectorError> {
        Ok(market::OperationId(
            self.next_operation.fetch_add(1, Ordering::Relaxed),
        ))
    }

    fn instruments(&self) -> Arc<[InstrumentSnapshot]> {
        Arc::from([InstrumentSnapshot {
            native_symbol: Arc::from("BTCUSDT"),
            asset_id: market::AssetId(1),
            available: true,
        }])
    }

    fn health(&self) -> ConnectorHealthSnapshot {
        ConnectorHealthSnapshot {
            state: if self.running.load(Ordering::Acquire) {
                ConnectorHealth::Running
            } else {
                ConnectorHealth::Stopped
            },
            message: Arc::from("fake"),
            observed_at: SystemTime::now(),
        }
    }

    fn diagnostics(&self) -> ConnectorDiagnosticSnapshot {
        ConnectorDiagnosticSnapshot {
            summary: Arc::from("fake"),
        }
    }

    fn operation(&self, id: market::OperationId) -> ConnectorOperationSnapshot {
        ConnectorOperationSnapshot {
            id,
            state: market::OperationState::Succeeded,
            detail: Arc::from("fake"),
        }
    }
}

#[cfg(feature = "numba-loader")]
#[derive(Default)]
struct TriggerMarketFactory {
    connector: std::sync::Mutex<Option<Arc<TriggerMarketConnector>>>,
}

#[cfg(feature = "numba-loader")]
impl MarketConnectorFactory for TriggerMarketFactory {
    fn connector_type(&self) -> &str {
        "trigger"
    }

    fn create(
        &self,
        _: &MarketSourceDefinition,
        context: MarketConnectorContext,
    ) -> Result<Arc<dyn MarketConnector>, ConnectorError> {
        let connector = Arc::new(TriggerMarketConnector {
            context,
            running: AtomicBool::new(false),
            next_operation: AtomicU64::new(1),
        });
        *self.connector.lock().unwrap() = Some(connector.clone());
        Ok(connector)
    }
}

#[cfg(feature = "numba-loader")]
fn numba_package_digest(files: &[(&str, &[u8])]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut files = files.to_vec();
    files.sort_by_key(|(name, _)| *name);
    let mut digest = Sha256::new();
    for (name, contents) in files {
        digest.update(name.as_bytes());
        digest.update([0]);
        digest.update((contents.len() as u64).to_le_bytes());
        digest.update(contents);
    }
    digest.finalize().into()
}

#[cfg(feature = "numba-loader")]
#[test]
fn fake_market_connector_reaches_numba_over_primary_lane_without_python_hot_path() {
    use semver::Version;
    use titan_plugin_engine::{
        ConfigSnapshot, ExecutionModel, ExecutionSpec, PluginEngine, PluginSpec, ServiceId,
        ServiceKey, ServiceScope, StopReason, SubscriptionLimits,
    };
    use titan_python_host::EmbeddedPythonCompiler;

    let nonce = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "titan-numba-primary-e2e-{}-{nonce}",
        std::process::id()
    ));
    let package_root = root.join("e2e_strategy");
    std::fs::create_dir_all(&package_root).unwrap();
    let init = b"";
    let strategy = include_bytes!("../../../strategies/event_counter/strategy.py");
    std::fs::write(package_root.join("__init__.py"), init).unwrap();
    std::fs::write(package_root.join("strategy.py"), strategy).unwrap();
    let digest = numba_package_digest(&[("__init__.py", init), ("strategy.py", strategy)]);
    std::fs::write(
        package_root.join("strategy-manifest.json"),
        serde_json::to_vec(&serde_json::json!({
            "strategy_type": "numba-e2e",
            "package_version": "1.0.0",
            "runtime_abi": {"major": 9, "minor": 0},
            "parameter_schema": {"type": "object"},
            "parameter_schema_version": 1,
            "state_schema_version": 1,
            "callbacks": u32::MAX,
            "capabilities": StrategyCapabilities::READ_TICK.0
                | StrategyCapabilities::READ_DEPTH.0
                | StrategyCapabilities::READ_BAR.0,
            "artifact_digest": digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "content_files": ["__init__.py", "strategy.py"]
        }))
        .unwrap(),
    )
    .unwrap();

    let event_engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    let events = event_engine.handle();
    events
        .register_event(
            DEPTH_BATCH_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
    events
        .register_event(
            BAR_BATCH_EVENT,
            MARKET_EVENT_SCHEMA_VERSION,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
    event_engine.start().unwrap();
    let trigger_factory = Arc::new(TriggerMarketFactory::default());
    let mut plugins = PluginEngine::new(Arc::new(events.clone()), ApiVersion::new(1, 0)).unwrap();
    plugins
        .register(
            Arc::new(MarketPluginFactory::new().with_factory(trigger_factory.clone())),
            Version::new(1, 0, 0),
            "test",
        )
        .unwrap();
    plugins
        .apply(&[PluginSpec {
            instance_id: Arc::from("market"),
            plugin_type: Arc::from(MARKET_PLUGIN_TYPE),
            config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
            enabled: true,
            execution: ExecutionSpec {
                model: ExecutionModel::Passive,
                cpu_affinity: None,
                callback_budget: None,
            },
            subscription_limits: SubscriptionLimits {
                max_capacity: 16,
                allowed_qos: [
                    EventQos::Latest,
                    EventQos::ReliableOrdered,
                    EventQos::BestEffort,
                ]
                .into_iter()
                .collect(),
            },
            service_scopes: vec![
                (
                    ServiceId::new("titan.market", "admin"),
                    ServiceScope::Global,
                ),
                (
                    ServiceId::new("titan.market", "market"),
                    ServiceScope::Global,
                ),
            ],
            required_service_scopes: vec![],
        }])
        .unwrap();
    let admin = plugins
        .services()
        .bind_typed::<MarketAdminApi>(&ServiceKey {
            id: ServiceId::new("titan.market", "admin"),
            version: Version::new(1, 0, 0),
            scope: ServiceScope::Global,
        })
        .unwrap();
    let source = match admin
        .call(
            MarketAdminRequest::Create(MarketSourceDefinition {
                source_key: Arc::from("market"),
                connector_type: Arc::from("trigger"),
                connector_config: Arc::from([]),
                instruments: Arc::from([MarketInstrumentBinding {
                    native_symbol: Arc::from("BTCUSDT"),
                    asset_id: market::AssetId(1),
                    price_tick: "1".parse().unwrap(),
                    quantity_lot: "1".parse().unwrap(),
                }]),
                enabled: true,
                definition_version: 1,
            }),
            TraceContext::default(),
        )
        .unwrap()
        .unwrap()
    {
        MarketAdminResponse::Handle(value) => value,
        _ => panic!("unexpected market create response"),
    };
    admin
        .call(MarketAdminRequest::Start(source), TraceContext::default())
        .unwrap()
        .unwrap();
    let market_handle = plugins
        .services()
        .bind_typed::<MarketApi>(&ServiceKey {
            id: ServiceId::new("titan.market", "market"),
            version: Version::new(1, 0, 0),
            scope: ServiceScope::Global,
        })
        .unwrap();

    let sdk = std::fs::canonicalize(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python/titan-strategy-sdk"),
    )
    .unwrap();
    let compiler = EmbeddedPythonCompiler::default()
        .with_python_path(root.clone())
        .with_python_path(sdk);
    let loaders = Arc::new(StrategyPackageLoaderRegistry::default());
    loaders
        .register(Arc::new(InProcessNumbaLoaderFactory::new(Arc::new(
            compiler,
        ))))
        .unwrap();
    let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
    runtimes
        .register(Arc::new(NativeStrategyRuntimeFactory::new("numba-e2e")))
        .unwrap();
    let mut config = StrategyPluginConfig::default();
    config.allowed_artifact_roots = Arc::from([Arc::from(root.to_string_lossy().as_ref())]);
    config.allowed_event_types = [
        (Arc::from(DEPTH_BATCH_EVENT), MARKET_EVENT_SCHEMA_VERSION),
        (Arc::from(BAR_BATCH_EVENT), MARKET_EVENT_SCHEMA_VERSION),
    ]
    .into_iter()
    .collect();
    let strategy_core = StrategyPluginCore::new(
        config,
        StrategyPluginDependencies {
            events: events.clone(),
            markets: Arc::new(PluginMarketService(market_handle)),
            accounts: Arc::new(FakeAccount),
            execution: Arc::new(FakeExecution {
                calls: Arc::new(AtomicUsize::new(0)),
                last_trace_id: Arc::new(AtomicU64::new(0)),
            }),
        },
        loaders,
        runtimes,
    )
    .unwrap();
    let mut strategy_definition = definition(1);
    strategy_definition.strategy_key = Arc::from("numba-e2e");
    strategy_definition.strategy_id = StrategyId(701);
    strategy_definition.package = StrategyPackageRef {
        loader_type: Arc::from("numba-python"),
        uri: Arc::from(format!("file://{}", package_root.display())),
        expected_digest: digest,
        signature_ref: None,
    };
    strategy_definition.entrypoint = Arc::from("e2e_strategy.strategy:build");
    strategy_definition.parameters = Arc::from(b"{}".as_slice());
    strategy_definition.accounts = Arc::from([]);
    strategy_definition.markets = Arc::from([StrategyMarketBinding {
        local_market_no: 0,
        local_asset_no: 0,
        source_key: Arc::from("market"),
        asset_id: 1,
        data_mode: StrategyDataMode::Hybrid {
            signal_timeframe_ns: 60_000_000_000,
        },
    }]);
    strategy_definition.subscriptions = Arc::from([
        StrategySubscriptionSpec {
            event_type: Arc::from(DEPTH_BATCH_EVENT),
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        },
        StrategySubscriptionSpec {
            event_type: Arc::from(BAR_BATCH_EVENT),
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        },
    ]);
    let strategy_handle = strategy_core.create(strategy_definition).unwrap();
    wait_operation(
        &strategy_core,
        strategy_core.prepare(strategy_handle).unwrap(),
    );
    wait_operation(
        &strategy_core,
        strategy_core.start(strategy_handle).unwrap(),
    );

    trigger_factory
        .connector
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .publish_tick();
    let deadline = Instant::now() + Duration::from_secs(2);
    while strategy_core
        .diagnostics(strategy_handle)
        .unwrap()
        .callback_count
        == 0
    {
        assert!(Instant::now() < deadline, "Numba callback was not reached");
        std::thread::sleep(Duration::from_millis(1));
    }
    trigger_factory
        .connector
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .publish_bar();
    let deadline = Instant::now() + Duration::from_secs(2);
    while strategy_core
        .diagnostics(strategy_handle)
        .unwrap()
        .callback_count
        < 2
    {
        assert!(
            Instant::now() < deadline,
            "Numba Bar callback was not reached"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    strategy_core
        .quiesce(Instant::now() + Duration::from_secs(1))
        .unwrap();
    plugins.shutdown(StopReason::Shutdown).unwrap();
    event_engine.stop().unwrap();
    assert_eq!(event_engine.arena().outstanding_blocks(), 0);
    std::fs::remove_dir_all(root).unwrap();
}

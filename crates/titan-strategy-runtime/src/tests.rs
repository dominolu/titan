use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use titan_account_service::*;
use titan_core_types::{ApiVersion, EventQos, EventView, TraceContext};
use titan_event_engine::*;
use titan_market_service::{self as market, *};
use titan_runtime::{CallbackRegistry, StrategyEventKind, StrategyRuntimeContext};

use super::*;

static STARTS: AtomicUsize = AtomicUsize::new(0);
static TICKS: AtomicUsize = AtomicUsize::new(0);
static STOPS: AtomicUsize = AtomicUsize::new(0);
static FAIL_START: AtomicBool = AtomicBool::new(false);
static DEPTH_EPOCH: AtomicU64 = AtomicU64::new(0);
static DEPTH_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static DEPTH_ACTION: AtomicUsize = AtomicUsize::new(0);
static DIRECT_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn observe_depth(context: *mut StrategyRuntimeContext) -> i32 {
    let context = unsafe { &mut *context };
    let batch = unsafe {
        &*(context
            .payload_ptr
            .cast::<titan_runtime_abi::DepthBatchEvent>())
    };
    let item = unsafe { &*batch.items_ptr };
    DEPTH_EPOCH.store(batch.stream_epoch, Ordering::SeqCst);
    DEPTH_SEQUENCE.store(batch.last_update_sequence, Ordering::SeqCst);
    DEPTH_ACTION.store(usize::from(item.action), Ordering::SeqCst);
    0
}

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
    if let Some(submit) = context.execution_submit {
        let request = titan_runtime_abi::AbiNewOrderRequest {
            asset_no: 0,
            order_id: TICKS.load(Ordering::SeqCst) as u64,
            price: 10.0,
            qty: 2.0,
            side: 1,
            order_type: 0,
            time_in_force: 0,
            _reserved: [0; 5],
        };
        let mut task_id = 0;
        return unsafe {
            submit(
                context.execution_context,
                0,
                (&request as *const titan_runtime_abi::AbiNewOrderRequest).cast(),
                &mut task_id,
            )
        };
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
        callbacks.set(StrategyEventKind::Depth, on_tick);
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
        Ok(MarketSubscription { id: 1 })
    }
    fn unsubscribe(
        &self,
        _: MarketSourceHandle,
        _: MarketSubscription,
    ) -> market::LocalResult<market::OperationId> {
        Ok(market::OperationId(1))
    }
    fn request_snapshot(
        &self,
        _: MarketSourceHandle,
        _: market::AssetId,
    ) -> market::LocalResult<market::OperationId> {
        Ok(market::OperationId(2))
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
        Ok(ConnectorOperationSnapshot {
            id: market::OperationId(2),
            state: market::OperationState::Succeeded,
            detail: Arc::from("succeeded"),
        })
    }
}

struct FakeAccount;
impl AccountService for FakeAccount {
    fn resolve(&self, _: &str) -> titan_account_service::LocalResult<AccountHandle> {
        Ok(AccountHandle {
            account_id: AccountId(7),
            generation: 1,
        })
    }
    fn orders(
        &self,
        _: AccountHandle,
        _: OrderFilter,
    ) -> titan_account_service::LocalResult<AccountStateSnapshot<OrderSnapshot>> {
        Ok(AccountStateSnapshot {
            account: AccountHandle {
                account_id: AccountId(7),
                generation: 1,
            },
            state: AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(1),
            captured_at: 1,
            items: Arc::from([]),
        })
    }
    fn positions(
        &self,
        _: AccountHandle,
        _: PositionFilter,
    ) -> titan_account_service::LocalResult<AccountStateSnapshot<PositionSnapshot>> {
        Ok(AccountStateSnapshot {
            account: AccountHandle {
                account_id: AccountId(7),
                generation: 1,
            },
            state: AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(1),
            captured_at: 1,
            items: Arc::from([]),
        })
    }
    fn balances(
        &self,
        _: AccountHandle,
    ) -> titan_account_service::LocalResult<AccountStateSnapshot<BalanceSnapshot>> {
        Ok(AccountStateSnapshot {
            account: AccountHandle {
                account_id: AccountId(7),
                generation: 1,
            },
            state: AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(1),
            captured_at: 1,
            items: Arc::from([]),
        })
    }
    fn health(
        &self,
        _: AccountHandle,
    ) -> titan_account_service::LocalResult<AccountConnectorHealthSnapshot> {
        Ok(AccountConnectorHealthSnapshot {
            state: AccountLifecycle::Ready,
            message: Arc::from("ready"),
            observed_at: SystemTime::now(),
        })
    }
    fn diagnostics(
        &self,
        _: AccountHandle,
    ) -> titan_account_service::LocalResult<AccountConnectorDiagnosticSnapshot> {
        unimplemented!()
    }

    fn execution_connector(
        &self,
        _: AccountHandle,
    ) -> titan_account_service::LocalResult<Arc<dyn DirectExecutionConnector>> {
        Ok(Arc::new(FakeDirectExecution))
    }
}

struct FakeDirectExecution;

impl DirectExecutionConnector for FakeDirectExecution {
    fn submit(&self, request: DirectNewOrderRequest) -> ExecutionFuture {
        Box::pin(async move {
            DIRECT_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
            Ok(DirectOrderInfo {
                client_order_id: Some(format!("{:02x?}", request.client_order_id.0)),
                venue_order_id: Some("venue-1".into()),
                status: 1,
            })
        })
    }

    fn cancel(&self, _: DirectCancelOrderRequest) -> ExecutionFuture {
        Box::pin(async {
            DIRECT_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
            Ok(DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("venue-1".into()),
                status: 4,
            })
        })
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
            state_f64_capacity: 4,
            state_i64_capacity: 4,
            ..StrategyRuntimeSpec::default()
        },
        recovery: StrategyRecoveryPolicy::Fresh,
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

fn wait_operation(
    core: &StrategyServiceCore,
    id: StrategyOperationId,
) -> StrategyOperationSnapshot {
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
fn lifecycle_direct_execution_replace_and_stale_handle_contract() {
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
        runtime_abi: ApiVersion::new(12, 0),
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
    DIRECT_EXECUTIONS.store(0, Ordering::SeqCst);
    let mut execution_runtime = ExecutionRuntime::new(1, 16).unwrap();
    let core = StrategyServiceCore::new(
        StrategyCoreConfig::default(),
        StrategyServiceDependencies {
            events: events.clone(),
            markets: Arc::new(FakeMarket),
            accounts: Arc::new(FakeAccount),
            execution_dispatcher: Some(execution_runtime.dispatcher()),
            execution_observer: Some(Arc::new(TracingExecutionObserver)),
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
    while DIRECT_EXECUTIONS.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(DIRECT_EXECUTIONS.load(Ordering::SeqCst), 1);
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
    execution_runtime.shutdown(Instant::now() + Duration::from_secs(1));
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
    let config = StrategyCoreConfig::default();
    let error = super::service_core::validate_definition(&config, &value).unwrap_err();
    assert_eq!(error.kind, StrategyErrorKind::InvalidDefinition);
}

#[test]
fn definition_rejects_remaining_unimplemented_canonical_subscriptions() {
    let config = StrategyCoreConfig::default();
    let unsupported: &[(&str, u32)] = &[
        (FUNDING_RATE_EVENT, MARKET_EVENT_SCHEMA_VERSION),
        (MARK_PRICE_EVENT, MARKET_EVENT_SCHEMA_VERSION),
        (TICKER_EVENT, MARKET_EVENT_SCHEMA_VERSION),
    ];
    for (event_type, schema_version) in unsupported {
        let mut value = definition(1);
        value.subscriptions = Arc::from([StrategySubscriptionSpec {
            event_type: Arc::from(*event_type),
            schema_version: *schema_version,
            routing_keys: Arc::from([1]),
            qos: EventQos::ReliableOrdered,
        }]);
        let error = super::service_core::validate_definition(&config, &value).unwrap_err();
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
            runtime_abi: ApiVersion::new(12, 0),
            parameter_schema: Arc::new(serde_json::json!({"type":"object"})),
            parameter_schema_version: 1,
            state_schema_version: 1,
            callbacks: StrategyCallbackMask(u32::MAX),
            capabilities,
            artifact_digest: [7; 32],
        };
        let error = super::service_core::validate_manifest(
            &StrategyCoreConfig::default(),
            &value,
            &manifest,
        )
        .unwrap_err();
        assert_eq!(error.kind, StrategyErrorKind::UnsupportedCapability);
    }
}

#[test]
fn adapter_fails_fast_on_unimplemented_canonical_facts() {
    let adapter = CanonicalStrategyEventAdapter::new(&[]);
    let payload = [];
    let event = EventView {
        event_type: FUNDING_RATE_EVENT,
        schema_version: MARKET_EVENT_SCHEMA_VERSION,
        payload: &payload,
        metadata: titan_core_types::EventPublishMetadata::default(),
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
fn adapter_preserves_depth_source_epoch_sequence_flags_and_actions() {
    let adapter = CanonicalStrategyEventAdapter::new(&[ResolvedMarketBinding {
        local_market_no: 2,
        local_asset_no: 3,
        source: MarketSourceHandle {
            source_id: MarketSourceId(9),
            generation: 1,
        },
        asset_id: 77,
        data_mode: StrategyDataMode::Tick,
    }]);
    let payload = encode_depth_batch(
        MarketBatchHeaderV1 {
            asset_id: 77,
            kind: 1,
            flags: 1,
            stream_epoch: 5,
            first_update_sequence: 100,
            last_update_sequence: 101,
            exchange_ts: 10,
            receive_ts: 11,
            ..MarketBatchHeaderV1::default()
        },
        &[DepthItemV1 {
            price_ticks: 123,
            quantity_lots: 45,
            side: 1,
            action: 2,
            ..DepthItemV1::default()
        }],
    )
    .unwrap();
    let event = EventView {
        event_type: DEPTH_BATCH_EVENT,
        schema_version: MARKET_EVENT_SCHEMA_VERSION,
        payload: &payload,
        metadata: titan_core_types::EventPublishMetadata {
            source_id: 18,
            ..titan_core_types::EventPublishMetadata::default()
        },
        trace: TraceContext::default(),
    };
    let mut callbacks = CallbackRegistry::default();
    callbacks.set(StrategyEventKind::Depth, observe_depth);
    let mut context = StrategyRuntimeContext::default();
    let kind = adapter.invoke(event, &callbacks, &mut context).unwrap();
    assert_eq!(kind, StrategyEventKind::Depth);
    assert_eq!(DEPTH_EPOCH.load(Ordering::SeqCst), 5);
    assert_eq!(DEPTH_SEQUENCE.load(Ordering::SeqCst), 101);
    assert_eq!(DEPTH_ACTION.load(Ordering::SeqCst), 2);
}

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use titan_account_plugin::*;
use titan_event_engine::*;
use titan_market_plugin::{self as market, *};
use titan_plugin_engine::{ApiVersion, EventQos};
use titan_runtime::{CallbackRegistry, StrategyEventKind, StrategyRuntimeContext};
use titan_runtime_abi::{ORDER_COMMAND_CANCEL, ORDER_COMMAND_SUBMIT, OrderCommand};

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
    TICKS.fetch_add(1, Ordering::SeqCst);
    let context = unsafe { &mut *context };
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
}
impl AccountExecutionService for FakeExecution {
    fn submit(
        &self,
        account: AccountHandle,
        command: SubmitOrderCommand,
    ) -> titan_account_plugin::LocalResult<AccountCommandReceipt> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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

struct FakeRisk {
    calls: Arc<AtomicUsize>,
    reject: Arc<AtomicBool>,
}
impl RiskService for FakeRisk {
    fn ready(&self, _: &RiskScopeRef) -> bool {
        true
    }
    fn check_and_reserve(&self, _: &StrategyRiskRequest) -> crate::LocalResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject.load(Ordering::SeqCst) {
            Err(StrategyError::new(
                StrategyErrorKind::RiskRejected,
                "test_risk",
                "risk_rejected",
                "test risk rejection",
            ))
        } else {
            Ok(())
        }
    }
}

struct NoBoundaries;
impl StreamBoundaryProvider for NoBoundaries {
    fn committed_boundaries(&self, _: StrategyHandle) -> crate::LocalResult<Arc<[StreamBoundary]>> {
        Ok(Arc::from([]))
    }
}
struct FakeRecovery;
impl StrategyRecoveryCoordinator for FakeRecovery {
    fn synchronize(
        &self,
        _: StrategyHandle,
        _: &StrategyDefinition,
        _: &PrimaryAsyncLaneHandle,
        _: Instant,
    ) -> crate::LocalResult<()> {
        Ok(())
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
            event_type: Arc::from("test.tick"),
            schema_version: 1,
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
fn lifecycle_gateway_checkpoint_replace_and_stale_handle_contract() {
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
        .register_event("test.tick", 1, EventClass::Market, PoolKind::SmallEvent)
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
            StrategyCapabilities::READ_TICK.0 | StrategyCapabilities::SUBMIT_ORDER.0,
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
    let sink = Arc::new(SnapshotCollector::default());
    let store = Arc::new(InMemoryCheckpointStore::default());
    let checkpoint = Arc::new(CheckpointCoordinator::new(
        Some(store.clone()),
        Arc::new(NoBoundaries),
        sink,
    ));
    let risk_calls = Arc::new(AtomicUsize::new(0));
    let reject_risk = Arc::new(AtomicBool::new(false));
    let execution_calls = Arc::new(AtomicUsize::new(0));
    let core = StrategyPluginCore::new(
        StrategyPluginConfig::default(),
        StrategyPluginDependencies {
            events: events.clone(),
            markets: Arc::new(FakeMarket),
            accounts: Arc::new(FakeAccount),
            execution: Arc::new(FakeExecution {
                calls: execution_calls.clone(),
            }),
            risk: Arc::new(FakeRisk {
                calls: risk_calls.clone(),
                reject: reject_risk.clone(),
            }),
            checkpoint: checkpoint.clone(),
            recovery: Arc::new(FakeRecovery),
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
    let mut publish = PublishRequest::new("test.tick", 1, b"tick");
    publish.routing_key = 1;
    events.try_publish(publish).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while execution_calls.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(risk_calls.load(Ordering::SeqCst), 1);
    assert_eq!(execution_calls.load(Ordering::SeqCst), 1);
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);

    let checkpoint_operation = core.checkpoint(handle).unwrap();
    assert_eq!(
        core.operation(checkpoint_operation).state,
        StrategyOperationState::Succeeded
    );
    assert!(
        store
            .load_latest(handle.strategy_id)
            .unwrap()
            .unwrap()
            .verify()
    );
    let mut changed_parameters = definition(1);
    changed_parameters.recovery = StrategyRecoveryPolicy::RequireCheckpoint;
    changed_parameters.parameters = Arc::from(br#"{"size":2}"#.as_slice());
    assert_eq!(
        checkpoint
            .restore(&changed_parameters, &manifest)
            .unwrap_err()
            .kind,
        StrategyErrorKind::CheckpointFailed
    );

    let pause = core.pause(handle, PauseReason::User).unwrap();
    assert_eq!(
        wait_operation(&core, pause).state,
        StrategyOperationState::Succeeded
    );
    let mut paused = PublishRequest::new("test.tick", 1, b"paused");
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

    reject_risk.store(true, Ordering::SeqCst);
    let mut rejected = PublishRequest::new("test.tick", 1, b"risk-rejected");
    rejected.routing_key = 1;
    events.try_publish(rejected).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while core.state(replacement).unwrap().lifecycle != StrategyLifecycle::Failed {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!core.state(replacement).unwrap().command_gate_open);

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
fn cancel_accepts_zero_quantity_and_ignores_submit_only_numeric_fields() {
    let strategy = StrategyHandle {
        strategy_id: StrategyId(11),
        generation: 2,
    };
    let gate = Arc::new(StrategyCommandGate::new(strategy));
    gate.open();
    let execution_calls = Arc::new(AtomicUsize::new(0));
    let risk_calls = Arc::new(AtomicUsize::new(0));
    let gateway = StandardStrategyCommandGateway::new(
        strategy,
        RiskScopeRef(Arc::from("scope")),
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
        Arc::new(FakeRisk {
            calls: risk_calls.clone(),
            reject: Arc::new(AtomicBool::new(false)),
        }),
        Arc::new(FakeExecution {
            calls: execution_calls.clone(),
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
        )
        .unwrap();
    assert_eq!(risk_calls.load(Ordering::SeqCst), 1);
    assert_eq!(execution_calls.load(Ordering::SeqCst), 1);
}

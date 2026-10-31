//! Explicit, non-live tests against an original signed grid strategy.
//! Run by verify_observation_recovery_compat.py on the build server.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    time::{Duration, Instant},
};
use titan_account_service as account;
use titan_core_types::{ComponentIdentity, ResourceScope};
use titan_event_engine::{
    EventClass, EventEngine, EventEngineConfig, PoolKind, PrimaryAsyncLaneConfig,
    PrimaryAsyncLaneHandle, PrimarySubscriptionSpec, SafePointTicket,
};
use titan_market_service::BBO_EVENT;
use titan_strategy_runtime::*;

#[derive(Default)]
struct Store {
    fail_submit: AtomicBool,
    fail_flush: AtomicBool,
}
fn injected() -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::Internal,
        "test",
        "injected",
        "injected recovery failure",
    )
}
impl StrategyRecoveryStore for Store {
    fn submit(&self, _: StrategyRecoveryPoint) -> LocalResult<()> {
        if self.fail_submit.load(Ordering::Acquire) {
            Err(injected())
        } else {
            Ok(())
        }
    }
    fn flush(&self, _: Instant) -> LocalResult<()> {
        if self.fail_flush.load(Ordering::Acquire) {
            Err(injected())
        } else {
            Ok(())
        }
    }
}
struct Clock {
    block: AtomicBool,
    entered: SyncSender<()>,
    release: Mutex<Receiver<()>>,
}
impl StrategyClock for Clock {
    fn now_ns(&self) -> i64 {
        if self.block.swap(false, Ordering::AcqRel) {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
        1_000_000_000
    }
}
struct Metrics;
impl StrategyMetrics for Metrics {}
struct Harness {
    runtime: Arc<dyn StrategyRuntime>,
    lane: PrimaryAsyncLaneHandle,
    engine: EventEngine,
    store: Arc<Store>,
    clock: Arc<Clock>,
    clock_entered: Receiver<()>,
    clock_release: SyncSender<()>,
    _scope: ResourceScope,
    _root: tempfile::TempDir,
}
impl Harness {
    fn new() -> Self {
        let path = PathBuf::from(std::env::var("TITAN_TEST_PREVIOUS_STRATEGY_FILE").unwrap());
        let key = std::env::var("TITAN_TEST_PREVIOUS_PUBLIC_KEY").unwrap();
        let key: Vec<u8> = (0..key.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&key[offset..offset + 2], 16).unwrap())
            .collect();
        let key: [u8; 32] = key.try_into().unwrap();
        let keys = BTreeMap::from([(Arc::from("acceptance"), key)]);
        let root = tempfile::tempdir().unwrap();
        let inspect = NativeStrategyLoaderV13::new(
            root.path().into(),
            V13TrustPolicy {
                require_signature: true,
                ed25519_keys: BTreeMap::from([("acceptance".into(), key)]),
            },
        )
        .inspect(&path)
        .unwrap();
        let file = StrategyFileRef {
            loader_type: Arc::from("native-v13"),
            uri: Arc::from(format!("file://{}", path.display())),
            expected_digest: inspect.compiled_strategy_digest,
            signature_ref: None,
        };
        let loader = NativeV13LoaderFactory
            .create(StrategyLoaderContext {
                allowed_strategy_roots: Arc::from([Arc::from(
                    path.parent().unwrap().to_str().unwrap(),
                )]),
                require_signature: true,
                ed25519_keys: keys,
            })
            .unwrap();
        let compiled = loader
            .load(
                StrategyLoadRequest {
                    strategy_file: file.clone(),
                },
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap();
        assert!(compiled.native.manifest.business_observation.is_some());
        let definition = StrategyDefinition {
            strategy_key: Arc::from("observation-boundaries"),
            runtime_strategy_no: RuntimeStrategyNo(77),
            strategy_file: file,
            markets: Arc::from([]),
            accounts: Arc::from([]),
            risk_scope: RiskScopeRef(Arc::from("synthetic")),
            runtime: StrategyRuntimeSpec {
                checkpoint_interval: Duration::from_secs(60),
                ..Default::default()
            },
            recovery: StrategyRecoveryPolicy::Fresh,
            enabled: true,
            run_config_revision: 1,
        };
        let scope = ResourceScope::new(ComponentIdentity::new("test", "observation-boundaries"));
        let store = Arc::new(Store::default());
        let (entered_tx, entered_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let clock = Arc::new(Clock {
            block: AtomicBool::new(false),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let runtime = NativeV13RuntimeFactory
            .create(
                &definition,
                compiled.clone(),
                StrategyRuntimeBuildContext {
                    market_service: None,
                    strategy: StrategyHandle {
                        runtime_strategy_no: RuntimeStrategyNo(77),
                        generation: 1,
                    },
                    compiled_strategy_digest: compiled.id,
                    markets: Arc::from([ResolvedMarketBinding {
                        source_stream: titan_market_service::SourceStreamId(2),
                        local_market_no: 0,
                        local_asset_no: 0,
                        source: titan_market_service::MarketSourceHandle {
                            source_id: titan_market_service::MarketSourceId(1),
                            generation: 1,
                        },
                        asset_id: 1,
                        data_mode: StrategyDataMode::Tick,
                    }]),
                    accounts: Arc::from([ResolvedAccountBinding {
                        local_account_no: 0,
                        account: account::AccountHandle {
                            account_id: account::AccountId(1),
                            generation: 1,
                        },
                        tradable_assets: Arc::from([StrategyTradableAsset {
                            local_asset_no: 0,
                            asset_id: 1,
                        }]),
                    }]),
                    execution: Arc::from([]),
                    state_snapshot_sink: store.clone(),
                    clock: clock.clone(),
                    metrics: Arc::new(Metrics),
                    resources: scope.handle(),
                    activation: Arc::new(StrategyActivationGate::default()),
                },
            )
            .unwrap();
        runtime
            .seed_public_state(StrategyPublicStateSeedV13 {
                accounts: Arc::from([TitanAccountView {
                    state: 4,
                    account_epoch: 1,
                    ..Default::default()
                }]),
                positions: Arc::from([TitanPositionView::default()]),
                balances: Arc::from([TitanBalanceView {
                    currency_no: 1,
                    total_units: 1_000_000,
                    available_units: 1_000_000,
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .unwrap();
        let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
        engine
            .handle()
            .register_event(BBO_EVENT, 1, EventClass::Market, PoolKind::MarketBatch)
            .unwrap();
        engine.start().unwrap();
        let lane = engine
            .handle()
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from(BBO_EVENT),
                    schema_version: 1,
                    qos: titan_core_types::EventQos::Latest,
                    routing_keys: Arc::from([1]),
                }],
                PrimaryAsyncLaneConfig {
                    control_capacity: 4,
                    ..Default::default()
                },
                runtime.clone(),
            )
            .unwrap();
        runtime.attach_lane(lane.clone()).unwrap();
        Self {
            runtime,
            lane,
            engine,
            store,
            clock,
            clock_entered: entered_rx,
            clock_release: release_tx,
            _scope: scope,
            _root: root,
        }
    }
    fn wait(&self, id: StrategyOperationId) -> StrategyOperationSnapshot {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let operation = self.runtime.operation(id);
            if operation.state != StrategyOperationState::Pending {
                return operation;
            }
            assert!(
                Instant::now() < deadline,
                "operation stuck: {}",
                operation.detail
            );
            std::thread::yield_now();
        }
    }
    fn start(&self) {
        assert_eq!(
            self.wait(self.runtime.prepare().unwrap()).state,
            StrategyOperationState::Succeeded
        );
        let started = self.wait(self.runtime.start().unwrap());
        assert_eq!(
            started.state,
            StrategyOperationState::Succeeded,
            "{}",
            started.detail
        );
        assert!(self.runtime.business_observation().unwrap().available);
    }
    fn settle(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(ticket) = self.lane.submit_safe_point(|| Ok(())) {
                ticket.wait(deadline).unwrap();
                return;
            }
            assert!(Instant::now() < deadline, "control queue did not drain");
            std::thread::yield_now();
        }
    }
    fn fill_control_queue(&self) {
        for _ in 0..4 {
            self.lane.submit_safe_point(|| Ok(())).unwrap();
        }
    }
    fn hold_control(&self) -> (SyncSender<()>, SafePointTicket) {
        let (entered_tx, entered_rx) = sync_channel(1);
        let (release_tx, release_rx) = sync_channel(1);
        let ticket = self
            .lane
            .submit_safe_point(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(())
            })
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        (release_tx, ticket)
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.engine.stop();
    }
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_never_started_and_stopped_frames() {
    let never_started = Harness::new();
    assert!(
        !never_started
            .runtime
            .business_observation()
            .unwrap()
            .available
    );
    assert_eq!(
        never_started
            .wait(
                never_started
                    .runtime
                    .stop(Instant::now() + Duration::from_secs(5))
                    .unwrap()
            )
            .state,
        StrategyOperationState::Succeeded
    );
    assert!(
        !never_started
            .runtime
            .business_observation()
            .unwrap()
            .available
    );
    let running = Harness::new();
    running.start();
    let before = running
        .runtime
        .business_observation()
        .unwrap()
        .source
        .unwrap();
    let stop = running
        .runtime
        .stop(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(running.wait(stop).state, StrategyOperationState::Succeeded);
    assert_eq!(
        running.runtime.state().lifecycle,
        StrategyLifecycle::Stopped
    );
    let terminal = running.runtime.business_observation().unwrap();
    assert!(terminal.available);
    assert_ne!(
        terminal.source.as_ref().unwrap().publication_sequence,
        before.publication_sequence
    );
    assert_eq!(
        running
            .runtime
            .stop(Instant::now() + Duration::from_secs(5))
            .unwrap(),
        stop
    );
    running.runtime.fire_timer(1).unwrap();
    running.settle();
    assert_eq!(
        serde_json::to_value(terminal.source).unwrap(),
        serde_json::to_value(running.runtime.business_observation().unwrap().source).unwrap()
    );
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_late_result_after_lane_shutdown_keeps_stopped_frame() {
    let harness = Harness::new();
    harness.start();
    let stop = harness
        .runtime
        .stop(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(harness.wait(stop).state, StrategyOperationState::Succeeded);
    let terminal = harness.runtime.business_observation().unwrap();
    harness.engine.stop().unwrap();
    assert!(harness.runtime.execution_result(rejected_result()).is_ok());
    let after = harness.runtime.business_observation().unwrap();
    assert!(after.available);
    assert_eq!(after.data, terminal.data);
    assert_eq!(
        serde_json::to_value(after.source).unwrap(),
        serde_json::to_value(terminal.source).unwrap()
    );
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_checkpoint_and_stop_failure_invalidate() {
    for stop_failure in [false, true] {
        let harness = Harness::new();
        harness.start();
        let id = if stop_failure {
            harness.store.fail_flush.store(true, Ordering::Release);
            harness
                .runtime
                .stop(Instant::now() + Duration::from_secs(5))
                .unwrap()
        } else {
            harness.store.fail_submit.store(true, Ordering::Release);
            harness
                .runtime
                .freeze_state(StrategyStateSnapshotRequest { checkpoint_id: 7 })
                .unwrap()
        };
        assert_eq!(harness.wait(id).state, StrategyOperationState::Failed);
        assert_eq!(harness.runtime.state().lifecycle, StrategyLifecycle::Failed);
        assert!(!harness.runtime.state().activation_gate_open);
        assert!(!harness.runtime.business_observation().unwrap().available);
    }
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_control_rejection_finishes_operation_without_failing_strategy() {
    let harness = Harness::new();
    harness.start();
    let (release, ticket) = harness.hold_control();
    harness.fill_control_queue();
    assert!(
        harness
            .runtime
            .freeze_state(StrategyStateSnapshotRequest { checkpoint_id: 7 })
            .is_err()
    );
    assert_eq!(
        harness.runtime.operation(StrategyOperationId(3)).state,
        StrategyOperationState::Failed
    );
    assert!(harness.runtime.business_observation().unwrap().available);
    release.send(()).unwrap();
    ticket
        .wait(Instant::now() + Duration::from_secs(5))
        .unwrap();
    harness.settle();
    assert_eq!(
        harness.runtime.state().lifecycle,
        StrategyLifecycle::Running
    );
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_invalidation_survives_control_rejection() {
    let harness = Harness::new();
    harness.start();
    let (release, ticket) = harness.hold_control();
    harness.fill_control_queue();
    assert!(harness.runtime.invalidate(Arc::from("injected")).is_err());
    assert!(!harness.runtime.business_observation().unwrap().available);
    assert!(!harness.runtime.state().activation_gate_open);
    release.send(()).unwrap();
    ticket
        .wait(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_execution_rejection_while_callback_owns_state() {
    let harness = Harness::new();
    harness.start();
    harness.clock.block.store(true, Ordering::Release);
    harness.runtime.fire_timer(1).unwrap();
    harness
        .clock_entered
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    harness.fill_control_queue();
    let result = harness.runtime.execution_result(rejected_result());
    assert!(result.is_err());
    // The writer still owns its state lock. Reading must already be unavailable
    // and must not wait for that lock or for a subsequent callback.
    assert!(!harness.runtime.business_observation().unwrap().available);
    harness.clock_release.send(()).unwrap();
    harness.settle();
    assert_eq!(harness.runtime.state().lifecycle, StrategyLifecycle::Failed);
}

fn rejected_result() -> account::ObservedExecutionResult {
    account::ObservedExecutionResult {
        task_id: 1,
        account: account::AccountHandle {
            account_id: account::AccountId(1),
            generation: 1,
        },
        asset_id: account::AssetId(1),
        request_kind: account::ExecutionRequestKind::Submit,
        client_order_id: None,
        outcome: account::ObservedExecutionOutcome::Rejected(account::DirectExecutionError {
            exchange: "synthetic",
            code: "injected".into(),
            message: "injected".into(),
            outcome_unknown: false,
        }),
    }
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_invalidation_after_inflight_start_closes_reopened_gate() {
    invalidation_during_start(false);
}

#[test]
#[ignore = "requires explicit original signed synthetic grid fixture"]
fn observation_runtime_rejected_invalidation_cannot_be_undone_by_inflight_start() {
    invalidation_during_start(true);
}

fn invalidation_during_start(queue_full: bool) {
    let harness = Harness::new();
    assert_eq!(
        harness.wait(harness.runtime.prepare().unwrap()).state,
        StrategyOperationState::Succeeded
    );
    harness.clock.block.store(true, Ordering::Release);
    let start = harness.runtime.start().unwrap();
    harness
        .clock_entered
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    if queue_full {
        harness.fill_control_queue();
    }
    let invalidation = harness
        .runtime
        .invalidate(Arc::from("injected during Start"));
    assert_eq!(invalidation.is_err(), queue_full);
    assert!(!harness.runtime.business_observation().unwrap().available);
    harness.clock_release.send(()).unwrap();
    assert_eq!(harness.wait(start).state, StrategyOperationState::Failed);
    if let Ok(id) = invalidation {
        assert_eq!(harness.wait(id).state, StrategyOperationState::Succeeded);
    }
    harness.settle();
    assert_eq!(harness.runtime.state().lifecycle, StrategyLifecycle::Failed);
    assert!(!harness.runtime.state().activation_gate_open);
    assert!(!harness.runtime.business_observation().unwrap().available);
}

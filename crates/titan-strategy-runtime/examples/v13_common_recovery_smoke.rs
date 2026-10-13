//! Common host recovery with a native artifact; no exchange or account credentials.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use titan_account_service::{self as account, AccountEventPayload};
use titan_core_types::{ComponentIdentity, EventView, ResourceScope};
use titan_event_engine::{
    EventClass, EventEngine, EventEngineConfig, PoolKind, PrimaryAsyncLaneConfig,
    PrimarySubscriptionSpec,
};
use titan_market_service::BBO_EVENT;
use titan_strategy_runtime::*;

struct Clock;
impl StrategyClock for Clock {
    fn now_ns(&self) -> i64 {
        1_000_000_000
    }
}
struct Metrics;
impl StrategyMetrics for Metrics {}
#[derive(Default)]
struct Sink {
    snapshots: Mutex<Vec<StrategyPrivateStateSnapshot>>,
    fail_flush: bool,
}
impl StrategyStateSnapshotSink for Sink {
    fn submit(&self, snapshot: StrategyPrivateStateSnapshot) -> LocalResult<()> {
        self.snapshots.lock().unwrap().push(snapshot);
        Ok(())
    }
    fn flush(&self, _: Instant) -> LocalResult<()> {
        if self.fail_flush {
            return Err(StrategyError::new(
                StrategyErrorKind::Internal,
                "flush",
                "injected",
                "failed durable write",
            ));
        }
        Ok(())
    }
}
fn wait(
    runtime: &Arc<dyn StrategyRuntime>,
    operation: StrategyOperationId,
) -> StrategyOperationState {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = runtime.operation(operation);
        if result.state != StrategyOperationState::Pending {
            return result.state;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}
fn seed(epoch: u64, balance: i64) -> StrategyPublicStateSeedV13 {
    StrategyPublicStateSeedV13 {
        accounts: Arc::from([TitanAccountView {
            state: 4,
            account_epoch: epoch,
            account_sequence: 10,
            ..Default::default()
        }]),
        positions: Arc::from([
            TitanPositionView {
                position_side: 1,
                qty_lots: 2,
                average_price_ticks: 100,
                ..Default::default()
            },
            TitanPositionView {
                position_side: 2,
                qty_lots: -1,
                average_price_ticks: 110,
                ..Default::default()
            },
        ]),
        balances: Arc::from([TitanBalanceView {
            total_units: balance,
            available_units: balance,
            account_sequence: 10,
            ..Default::default()
        }]),
        ..Default::default()
    }
}
fn create(
    definition: &StrategyDefinition,
    artifact: StrategyArtifact,
    generation: u64,
    sink: Arc<Sink>,
    scope: &ResourceScope,
) -> (Arc<dyn StrategyRuntime>, Arc<StrategyActivationGate>) {
    let activation = Arc::new(StrategyActivationGate::default());
    let runtime = NativeV13RuntimeFactory
        .create(
            definition,
            artifact.clone(),
            StrategyRuntimeBuildContext {
                strategy: StrategyHandle {
                    strategy_id: StrategyId(42),
                    generation,
                },
                artifact_id: artifact.id,
                markets: Arc::from([]),
                accounts: Arc::from([ResolvedAccountBinding {
                    local_account_no: 0,
                    account: account::AccountHandle {
                        account_id: account::AccountId(8),
                        generation: if generation == 1 { 1 } else { 2 },
                    },
                    tradable_assets: Arc::from([StrategyTradableAsset {
                        local_asset_no: 0,
                        asset_id: 1,
                    }]),
                }]),
                execution: Arc::from([]),
                state_snapshot_sink: sink,
                clock: Arc::new(Clock),
                metrics: Arc::new(Metrics),
                resources: scope.handle(),
                activation: activation.clone(),
            },
        )
        .unwrap();
    (runtime, activation)
}
fn attach(runtime: &Arc<dyn StrategyRuntime>, engine: &EventEngine) {
    let lane = engine
        .handle()
        .register_primary_async_lane(
            &[PrimarySubscriptionSpec {
                event_type: Arc::from(BBO_EVENT),
                schema_version: 1,
                qos: titan_core_types::EventQos::Latest,
                routing_keys: Arc::from([1]),
            }],
            PrimaryAsyncLaneConfig::default(),
            runtime.clone(),
        )
        .unwrap();
    runtime.attach_lane(lane).unwrap();
}
fn main() {
    let path = std::fs::canonicalize(PathBuf::from(std::env::args_os().nth(1).expect("artifact")))
        .unwrap();
    let manifest = NativeArtifactLoaderV13::new(
        std::env::temp_dir().join("titan-common-recovery-inspect"),
        V13TrustPolicy::default(),
    )
    .inspect(&path)
    .unwrap();
    let package = StrategyPackageRef {
        loader_type: Arc::from("native-v13"),
        uri: Arc::from(format!("file://{}", path.display())),
        expected_digest: manifest.artifact_digest,
        signature_ref: None,
    };
    let loader = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_artifact_roots: Arc::from([Arc::from(
                path.parent().unwrap().to_string_lossy().as_ref(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap();
    let artifact = loader
        .load(
            StrategyLoadRequest {
                package: package.clone(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    let definition = StrategyDefinition {
        strategy_key: Arc::from("common-recovery"),
        strategy_id: StrategyId(42),
        package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: RiskScopeRef(Arc::from("mock")),
        runtime: Default::default(),
        recovery: StrategyRecoveryPolicy::Fresh,
        enabled: true,
        definition_version: 1,
    };
    let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    engine
        .handle()
        .register_event(BBO_EVENT, 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    engine.start().unwrap();
    let scopes: Vec<_> = (0..4)
        .map(|index| {
            ResourceScope::new(ComponentIdentity::new("test", format!("recovery-{index}")))
        })
        .collect();
    let sink = Arc::new(Sink::default());
    let (original, _) = create(&definition, artifact.clone(), 1, sink.clone(), &scopes[0]);
    original.seed_public_state(seed(1, 100)).unwrap();
    attach(&original, &engine);
    assert_eq!(
        wait(&original, original.prepare().unwrap()),
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait(&original, original.start().unwrap()),
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait(
            &original,
            original
                .freeze_state(StrategyStateSnapshotRequest { checkpoint_id: 17 })
                .unwrap()
        ),
        StrategyOperationState::Succeeded
    );
    let snapshot = sink.snapshots.lock().unwrap().last().unwrap().clone();
    let positions = &snapshot.recovery.as_ref().unwrap().public.positions;
    assert_eq!(positions.len(), 2);
    assert_eq!((positions[0].position_side, positions[0].qty_lots), (1, 2));
    assert_eq!((positions[1].position_side, positions[1].qty_lots), (2, -1));
    let recovered_sink = Arc::new(Sink::default());
    let (recovered, gate) = create(
        &definition,
        artifact.clone(),
        2,
        recovered_sink.clone(),
        &scopes[1],
    );
    recovered.seed_public_state(seed(2, 120)).unwrap();
    recovered.restore_state(snapshot.clone()).unwrap();
    assert!(!gate.is_open());
    assert_eq!(
        recovered_sink
            .snapshots
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .generation,
        2
    );
    attach(&recovered, &engine);
    assert_eq!(
        wait(&recovered, recovered.prepare().unwrap()),
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait(&recovered, recovered.start().unwrap()),
        StrategyOperationState::Succeeded
    );
    assert!(gate.is_open());

    let (failed, failed_gate) = create(
        &definition,
        artifact.clone(),
        3,
        Arc::new(Sink {
            fail_flush: true,
            ..Default::default()
        }),
        &scopes[2],
    );
    failed.seed_public_state(seed(2, 120)).unwrap();
    assert!(failed.restore_state(snapshot.clone()).is_err());
    assert_eq!(failed.state().lifecycle, StrategyLifecycle::Failed);
    assert!(!failed_gate.is_open());
    attach(&failed, &engine);
    assert_eq!(
        wait(&failed, failed.start().unwrap()),
        StrategyOperationState::Failed
    );
    assert!(!failed_gate.is_open());

    let (drifted, drifted_gate) = create(
        &definition,
        artifact,
        4,
        Arc::new(Sink::default()),
        &scopes[3],
    );
    drifted.seed_public_state(seed(2, 120)).unwrap();
    drifted.restore_state(snapshot).unwrap();
    let balance = account::BalanceChangedV1 {
        header: account::AccountEventHeaderV1 {
            account_id: 8,
            account_generation: 2,
            account_epoch: 2,
            account_version: 11,
            ..Default::default()
        },
        wallet_units: 121,
        ..Default::default()
    };
    let mut bytes = vec![0; account::BalanceChangedV1::ENCODED_LEN];
    balance.encode_into(&mut bytes).unwrap();
    drifted
        .handle(EventView {
            event_type: account::BALANCE_CHANGED_EVENT,
            schema_version: 1,
            payload: &bytes,
            metadata: Default::default(),
            trace: Default::default(),
        })
        .unwrap();
    attach(&drifted, &engine);
    assert_eq!(
        wait(&drifted, drifted.prepare().unwrap()),
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        wait(&drifted, drifted.start().unwrap()),
        StrategyOperationState::Failed
    );
    assert!(!drifted_gate.is_open());

    // A retired publisher can have the same epoch and a higher sequence: generation fences it.
    let old_balance = account::BalanceChangedV1 {
        header: account::AccountEventHeaderV1 {
            account_id: 8,
            account_generation: 1,
            account_epoch: 2,
            account_version: 999,
            ..Default::default()
        },
        wallet_units: 999,
        ..Default::default()
    };
    let mut old_bytes = vec![0; account::BalanceChangedV1::ENCODED_LEN];
    old_balance.encode_into(&mut old_bytes).unwrap();
    recovered
        .handle(EventView {
            event_type: account::BALANCE_CHANGED_EVENT,
            schema_version: 1,
            payload: &old_bytes,
            metadata: Default::default(),
            trace: Default::default(),
        })
        .unwrap();
    assert_eq!(
        wait(
            &recovered,
            recovered
                .freeze_state(StrategyStateSnapshotRequest { checkpoint_id: 18 })
                .unwrap()
        ),
        StrategyOperationState::Succeeded
    );
    assert_eq!(
        recovered_sink
            .snapshots
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .recovery
            .as_ref()
            .unwrap()
            .public
            .balances[0]
            .total_units,
        120
    );

    let epoch = account::StreamStateChangedV1(account::StreamStateV1 {
        header: account::AccountEventHeaderV1 {
            account_id: 8,
            account_generation: 2,
            account_epoch: 3,
            account_version: 12,
            ..Default::default()
        },
        state: 4,
        reason_code: 0,
    });
    let mut bytes = vec![0; account::StreamStateChangedV1::ENCODED_LEN];
    epoch.encode_into(&mut bytes).unwrap();
    assert!(
        recovered
            .handle(EventView {
                event_type: account::STREAM_STATE_CHANGED_EVENT,
                schema_version: 1,
                payload: &bytes,
                metadata: Default::default(),
                trace: Default::default()
            })
            .is_err()
    );
    assert_eq!(recovered.state().lifecycle, StrategyLifecycle::Failed);
    assert!(!gate.is_open());
    engine.stop().unwrap();
    println!(
        "V13 common recovery: session/balance rebase, durable checkpoint failure, startup drift, generation/epoch isolation passed"
    );
}

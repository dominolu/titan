//! Common host recovery with a native artifact; no exchange or account credentials.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use titan_account_service::{self as account};
use titan_core_types::{ComponentIdentity, ResourceScope};
use titan_event_engine::{
    EventClass, EventEngine, EventEngineConfig, PoolKind, PrimaryAsyncLaneConfig,
    PrimarySubscriptionSpec,
};
use titan_market_service::BBO_EVENT;
use titan_strategy_runtime::*;

struct Broker;
impl account::DirectExecutionConnector for Broker {
    fn submit(&self, _: account::DirectNewOrderRequest) -> account::ExecutionFuture { panic!("intent flush failure must prevent mock dispatch") }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture { unreachable!() }
}
struct Observer;
impl account::ExecutionObserver for Observer { fn observe(&self, _: account::ObservedExecutionResult) {} }
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
    let executor = Box::leak(Box::new(account::ExecutionRuntime::new(1, 16).unwrap()));
    let execution = executor.bind(account::AccountHandle { account_id: account::AccountId(8), generation: 1 }, Arc::new(Broker), Arc::new(Observer));
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
                execution: Arc::from([StrategyExecutionBinding { local_account_no: 0, assets: BTreeMap::from([(0, 1)]), handle: execution }]),
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
    let sink = Arc::new(Sink { fail_flush: true, ..Default::default() });
    let (original, activation) = create(&definition, artifact.clone(), 1, sink.clone(), &scopes[0]);
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
    original.fire_timer(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while sink.snapshots.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(!sink.snapshots.lock().unwrap().is_empty(), "timer must register an intent snapshot");
    std::thread::sleep(Duration::from_millis(20));
    println!("After timer command-intent flush failure: lifecycle={:?}, gate_open={}", original.state().lifecycle, activation.is_open());
    assert_eq!(original.state().lifecycle, StrategyLifecycle::Failed, "timer command-intent flush failure must fail closed");
    assert!(!activation.is_open());
}

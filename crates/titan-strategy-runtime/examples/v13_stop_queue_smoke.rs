use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use titan_core_types::{ComponentIdentity, ResourceScope};
use titan_event_engine::{
    EventClass, EventEngine, EventEngineConfig, PoolKind, PrimaryAsyncLaneConfig,
    PrimarySubscriptionSpec,
};
use titan_strategy_runtime::*;
struct Clock;
impl StrategyClock for Clock {
    fn now_ns(&self) -> i64 {
        1_000_000_000
    }
}
struct Metrics;
impl StrategyMetrics for Metrics {}
struct Sink;
impl StrategyStateSnapshotSink for Sink {
    fn submit(&self, _: StrategyPrivateStateSnapshot) -> LocalResult<()> {
        Ok(())
    }
    fn flush(&self, _: Instant) -> LocalResult<()> {
        Ok(())
    }
}
fn main() {
    let path = std::fs::canonicalize(PathBuf::from(std::env::args_os().nth(1).unwrap())).unwrap();
    let manifest = NativeArtifactLoaderV13::new(
        std::env::temp_dir().join("titan-review-queue-cache"),
        V13TrustPolicy::default(),
    )
    .inspect(&path)
    .unwrap();
    let package = StrategyPackageRef {
        loader_type: "native-v13".into(),
        uri: format!("file://{}", path.display()).into(),
        expected_digest: manifest.artifact_digest,
        signature_ref: None,
    };
    let artifact = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_artifact_roots: Arc::from([Arc::from(
                path.parent().unwrap().to_str().unwrap(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap()
        .load(
            StrategyLoadRequest {
                package: package.clone(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    let definition = StrategyDefinition {
        strategy_key: "review".into(),
        strategy_id: StrategyId(77),
        package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: RiskScopeRef("review".into()),
        runtime: Default::default(),
        recovery: StrategyRecoveryPolicy::Fresh,
        enabled: true,
        definition_version: 1,
    };
    let scope = ResourceScope::new(ComponentIdentity::new("review", "stop-queue"));
    let runtime = NativeV13RuntimeFactory
        .create(
            &definition,
            artifact.clone(),
            StrategyRuntimeBuildContext {
                strategy: StrategyHandle {
                    strategy_id: StrategyId(77),
                    generation: 1,
                },
                artifact_id: artifact.id,
                markets: Arc::from([]),
                accounts: Arc::from([]),
                execution: Arc::from([]),
                state_snapshot_sink: Arc::new(Sink),
                clock: Arc::new(Clock),
                metrics: Arc::new(Metrics),
                resources: scope.handle(),
                activation: Arc::new(StrategyActivationGate::default()),
            },
        )
        .unwrap();
    let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    engine
        .handle()
        .register_event(
            titan_market_service::BBO_EVENT,
            1,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
    engine.start().unwrap();
    let subscriptions = [PrimarySubscriptionSpec {
        event_type: titan_market_service::BBO_EVENT.into(),
        schema_version: 1,
        qos: titan_core_types::EventQos::Latest,
        routing_keys: Arc::from([1]),
    }];
    let lane = engine
        .handle()
        .register_primary_async_lane(
            &subscriptions,
            PrimaryAsyncLaneConfig {
                control_capacity: 1,
                ..Default::default()
            },
            runtime.clone(),
        )
        .unwrap();
    runtime.attach_lane(lane.clone()).unwrap();
    let (ready, wait) = mpsc::sync_channel(1);
    let (release, blocked) = mpsc::sync_channel(1);
    lane.submit_safe_point(move || {
        ready.send(()).unwrap();
        blocked.recv().unwrap();
        Ok(())
    })
    .unwrap();
    wait.recv_timeout(Duration::from_secs(2)).unwrap();
    let drained = Arc::new(AtomicBool::new(false));
    let done = drained.clone();
    lane.submit_safe_point(move || {
        done.store(true, Ordering::Release);
        Ok(())
    })
    .unwrap();
    let first = runtime.stop(Instant::now() + Duration::from_secs(1));
    assert!(first.is_err(), "fixture did not fill the control queue");
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !drained.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    let retry = runtime
        .stop(Instant::now() + Duration::from_secs(1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let result = runtime.operation(retry);
    println!(
        "first_error={:?}; retry_operation={}; retry_state={:?}; lifecycle={:?}",
        first.err().unwrap().reason_code,
        retry.0,
        result.state,
        runtime.state().lifecycle
    );
    engine.stop().unwrap();
    assert_eq!(
        result.state,
        StrategyOperationState::Succeeded,
        "Stop retry after queue recovery returned an operation that was never enqueued"
    );
}

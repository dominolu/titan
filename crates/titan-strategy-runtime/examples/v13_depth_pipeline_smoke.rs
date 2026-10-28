//! Synthetic public facts through the real arena, PRIMARY lane and native V13 callback.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use titan_core_types::{ComponentIdentity, EventQos, ResourceScope};
use titan_event_engine::*;
use titan_market_service as market;
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
struct Sink(Mutex<Option<StrategyRecoveryPoint>>);
impl StrategyRecoveryStore for Sink {
    fn submit(&self, value: StrategyRecoveryPoint) -> LocalResult<()> {
        *self.0.lock().unwrap() = Some(value);
        Ok(())
    }
}
fn wait(runtime: &Arc<dyn StrategyRuntime>, id: StrategyOperationId) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let op = runtime.operation(id);
        if op.state == StrategyOperationState::Succeeded {
            return;
        }
        assert_eq!(op.state, StrategyOperationState::Pending, "{}", op.detail);
        assert!(Instant::now() < end, "operation timed out");
        std::thread::yield_now();
    }
}
fn main() {
    let artifact =
        std::fs::canonicalize(PathBuf::from(std::env::args_os().nth(1).expect("artifact")))
            .unwrap();
    let loader = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_strategy_roots: Arc::from([Arc::from(
                artifact.parent().unwrap().to_str().unwrap(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap();
    let manifest = NativeStrategyLoaderV13::new(
        std::env::temp_dir().join("titan-depth-inspect"),
        V13TrustPolicy::default(),
    )
    .inspect(&artifact)
    .unwrap();
    let package = StrategyFileRef {
        loader_type: Arc::from("native-v13"),
        uri: Arc::from(format!("file://{}", artifact.display())),
        expected_digest: manifest.compiled_strategy_digest,
        signature_ref: None,
    };
    let loaded = loader
        .load(
            StrategyLoadRequest {
                strategy_file: package.clone(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    assert!(
        loaded
            .manifest
            .subscriptions
            .iter()
            .any(|s| s.event_type.as_ref() == market::STREAM_INVALIDATED_EVENT)
    );
    let definition = StrategyDefinition {
        strategy_key: Arc::from("depth-pipeline"),
        runtime_strategy_no: RuntimeStrategyNo(42),
        strategy_file: package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: RiskScopeRef(Arc::from("synthetic-no-orders")),
        runtime: StrategyRuntimeSpec {
            max_handler_duration: Duration::from_millis(100),
            ..Default::default()
        },
        recovery: StrategyRecoveryPolicy::Fresh,
        enabled: true,
        run_config_revision: 1,
    };
    let mut config = EventEngineConfig::default();
    config.arena.market_batch.slots = 16;
    config.arena.market_batch.low_watermark = 2;
    config.arena.small_event.slots = 16;
    config.arena.small_event.low_watermark = 2;
    config.arena.snapshot.slots = 4;
    config.arena.snapshot.low_watermark = 1;
    let engine = EventEngine::new(config).unwrap();
    let events = engine.handle();
    events
        .register_event(
            market::DEPTH_BATCH_EVENT,
            1,
            EventClass::Market,
            PoolKind::MarketBatch,
        )
        .unwrap();
    events
        .register_event(
            market::STREAM_INVALIDATED_EVENT,
            1,
            EventClass::Critical,
            PoolKind::SmallEvent,
        )
        .unwrap();
    engine.start().unwrap();
    let source_stream = events.allocate_source_streams(1).unwrap();
    let sink = Arc::new(Sink::default());
    let scope = ResourceScope::new(ComponentIdentity::new("test", "depth"));
    let runtime = NativeV13RuntimeFactory
        .create(
            &definition,
            loaded.clone(),
            StrategyRuntimeBuildContext {
                market_service: None,
                strategy: StrategyHandle {
                    runtime_strategy_no: RuntimeStrategyNo(42),
                    generation: 1,
                },
                compiled_strategy_digest: loaded.id,
                markets: Arc::from([ResolvedMarketBinding {
                    source_stream: market::SourceStreamId(source_stream),
                    local_market_no: 0,
                    local_asset_no: 0,
                    source: market::MarketSourceHandle {
                        source_id: market::MarketSourceId(1),
                        generation: 1,
                    },
                    asset_id: 1,
                    data_mode: StrategyDataMode::Tick,
                }]),
                accounts: Arc::from([]),
                execution: Arc::from([]),
                state_snapshot_sink: sink.clone(),
                clock: Arc::new(Clock),
                metrics: Arc::new(Metrics),
                resources: scope.handle(),
                activation: Arc::new(StrategyActivationGate::default()),
            },
        )
        .unwrap();
    let lane = events
        .register_primary_async_lane(
            &[
                PrimarySubscriptionSpec {
                    event_type: Arc::from(market::DEPTH_BATCH_EVENT),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([1]),
                },
                PrimarySubscriptionSpec {
                    event_type: Arc::from(market::STREAM_INVALIDATED_EVENT),
                    schema_version: 1,
                    qos: EventQos::ReliableOrdered,
                    routing_keys: Arc::from([1]),
                },
            ],
            PrimaryAsyncLaneConfig {
                max_handler_duration: Duration::from_millis(100),
                ..Default::default()
            },
            runtime.clone(),
        )
        .unwrap();
    runtime.attach_lane(lane.clone()).unwrap();
    wait(&runtime, runtime.prepare().unwrap());
    wait(&runtime, runtime.start().unwrap());
    let image: Vec<_> = (0..1000)
        .flat_map(|i| [(1, 10000 - i), (2, 10001 + i)])
        .map(|(side, price_ticks)| market::DepthItemV1 {
            side,
            price_ticks,
            quantity_lots: 10,
            action: 1,
            reserved: [0; 6],
        })
        .collect();
    let publish = |event_type: &'static str, sequence, payload: &[u8]| {
        let mut request = PublishRequest::new(event_type, 1, payload);
        request.source_id = source_stream;
        request.source_sequence = sequence;
        request.routing_key = 1;
        events.try_publish(request).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while lane.progress().committed_sequence < sequence {
            assert!(
                Instant::now() < deadline,
                "delivery timed out: {:?}",
                runtime.health().degraded_reason
            );
            std::thread::yield_now();
        }
    };
    let header = |epoch, seq, snapshot| market::MarketBatchHeaderV1 {
        asset_id: 1,
        kind: if snapshot { 1 } else { 2 },
        flags: u16::from(snapshot),
        stream_epoch: epoch,
        first_update_sequence: seq,
        last_update_sequence: seq,
        exchange_ts: 1_000_000_000,
        receive_ts: 1_000_000_000,
        ..Default::default()
    };
    let image_bytes = market::encode_depth_batch(header(1, 10, true), &image).unwrap();
    assert_eq!(image_bytes.len(), 48_052);
    publish(market::DEPTH_BATCH_EVENT, 1, &image_bytes);
    let read = |checkpoint_id| {
        wait(
            &runtime,
            runtime
                .freeze_state(StrategyStateSnapshotRequest { checkpoint_id })
                .unwrap(),
        );
        sink.0
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .state_bytes
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>()
    };
    assert_eq!(read(1), [1, 1, 1, 9901, 10100, 1]);
    let delete = market::DepthItemV1 {
        side: 1,
        price_ticks: 10000,
        quantity_lots: 0,
        action: 2,
        reserved: [0; 6],
    };
    publish(
        market::DEPTH_BATCH_EVENT,
        2,
        &market::encode_depth_batch(header(1, 11, false), &[delete]).unwrap(),
    );
    assert_eq!(read(2), [2, 1, 0, 9900, 10100, 1]);
    let mut invalid = 1_u32.to_le_bytes().to_vec();
    invalid.extend_from_slice(&0_u64.to_le_bytes());
    publish(market::STREAM_INVALIDATED_EVENT, 3, &invalid);
    assert_eq!(read(3), [3, 0, 1, 0, 0, 1]);
    publish(
        market::DEPTH_BATCH_EVENT,
        4,
        &market::encode_depth_batch(header(2, 20, true), &image).unwrap(),
    );
    assert_eq!(read(4), [4, 1, 1, 9901, 10100, 2]);
    let mut offline = OfflineV13Adapter::new(OfflineV13Config {
        compiled_strategy: loaded.native.clone(),
        strategy_instance_id: 43,
        generation: 1,
        command_capacity: 8,
        markets: vec![TitanMarketView::default()],
        positions: vec![],
        balances: vec![],
        accounts: vec![],
        active_orders: vec![],
    })
    .unwrap();
    offline.start(1_000_000_000).unwrap();
    let raw: Vec<_> = image
        .iter()
        .map(|item| TitanDepthView {
            price_ticks: item.price_ticks,
            qty_lots: item.quantity_lots,
            side: item.side,
            action: item.action,
            is_snapshot: 1,
            receive_ts_ns: 1_000_000_000,
            ..Default::default()
        })
        .collect();
    assert!(
        offline.on_depth(1_000_000_000, &raw).is_err(),
        "legacy replay must not guess metadata"
    );
    offline
        .on_depth_batch(
            1_000_000_000,
            DepthBatchInfoV1 {
                version: 1,
                valid: 1,
                reset: 1,
                stream_epoch: 1,
                publication_sequence: 1,
                ..Default::default()
            },
            &raw,
        )
        .unwrap();
    let words: Vec<_> = offline
        .state_bytes()
        .chunks_exact(8)
        .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    assert_eq!(words, [1, 1, 1, 9901, 10100, 1]);
    offline
        .on_depth_batch(
            1_000_000_000,
            DepthBatchInfoV1 {
                version: 1,
                reset: 1,
                publication_sequence: 2,
                ..Default::default()
            },
            &[],
        )
        .unwrap();
    let words: Vec<_> = offline
        .state_bytes()
        .chunks_exact(8)
        .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
        .collect();
    assert_eq!(words, [2, 0, 1, 0, 0, 1]);

    wait(
        &runtime,
        runtime
            .stop(Instant::now() + Duration::from_secs(5))
            .unwrap(),
    );
    events
        .unregister_primary_async_lane_before(lane.token(), Instant::now() + Duration::from_secs(5))
        .unwrap();
    engine.stop().unwrap();
    println!(
        "PASS: 48,052-byte snapshot -> arena -> PRIMARY -> V13 Top100; unchanged level backfill, invalidation, resnapshot; same offline reconstruction and legacy replay rejection"
    );
}

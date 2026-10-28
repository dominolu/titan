//! Live host/replay admission parity using synthetic accounts and an in-process broker.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use titan_account_service as account;
use titan_core_types::{ComponentIdentity, EventView, ResourceScope};
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
struct Sink;
impl StrategyRecoveryStore for Sink {
    fn submit(&self, _: StrategyRecoveryPoint) -> LocalResult<()> {
        Ok(())
    }
}
struct Broker(Arc<Mutex<Vec<&'static str>>>);
impl account::DirectExecutionConnector for Broker {
    fn submit(&self, request: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        self.0.lock().unwrap().push(if request.reduce_only {
            "reduce"
        } else {
            "open"
        });
        Box::pin(async {
            Ok(account::DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("synthetic".into()),
                status: 3,
            })
        })
    }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture {
        self.0.lock().unwrap().push("cancel");
        Box::pin(async {
            Ok(account::DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("synthetic".into()),
                status: 6,
            })
        })
    }
}
struct Observer;
impl account::ExecutionObserver for Observer {
    fn observe(&self, _: account::ObservedExecutionResult) {}
}
fn wait(runtime: &Arc<dyn StrategyRuntime>, id: StrategyOperationId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let op = runtime.operation(id);
        if op.state == StrategyOperationState::Succeeded {
            return;
        }
        assert_eq!(op.state, StrategyOperationState::Pending, "{}", op.detail);
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}
fn main() {
    let path = std::fs::canonicalize(PathBuf::from(
        std::env::args_os().nth(1).expect("fixture bundle"),
    ))
    .unwrap();
    let manifest = NativeStrategyLoaderV13::new(
        std::env::temp_dir().join("titan-depth-execution-inspect"),
        V13TrustPolicy::default(),
    )
    .inspect(&path)
    .unwrap();
    let package = StrategyFileRef {
        loader_type: Arc::from("native-v13"),
        uri: Arc::from(format!("file://{}", path.display())),
        expected_digest: manifest.compiled_strategy_digest,
        signature_ref: None,
    };
    let loader = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_strategy_roots: Arc::from([Arc::from(
                path.parent().unwrap().to_str().unwrap(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap();
    let loaded = loader
        .load(
            StrategyLoadRequest {
                strategy_file: package.clone(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    // 0: no snapshot, safe exit; 1: ready/open; 2: invalid/open;
    // 3: recovered/open; 4: invalid/oversize; 5: invalid/safe exit.
    for case in 0..6 {
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
                market::BBO_EVENT,
                1,
                EventClass::Market,
                PoolKind::MarketBatch,
            )
            .unwrap();
        engine.start().unwrap();
        let account = account::AccountHandle {
            account_id: account::AccountId(8),
            generation: 1,
        };
        let account_view = TitanAccountView {
            account_epoch: 1,
            account_sequence: 1,
            state: 4,
            ..Default::default()
        };
        let position = TitanPositionView {
            qty_lots: 5,
            account_sequence: 1,
            ..Default::default()
        };
        let order = TitanActiveOrderView {
            order_id: 7,
            qty_lots: 1,
            side: 1,
            status: 3,
            ..Default::default()
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut executor = account::ExecutionRuntime::new(1, 16).unwrap();
        let scope = ResourceScope::new(ComponentIdentity::new("test", "depth-execution"));
        let definition = StrategyDefinition {
            strategy_key: Arc::from("depth-execution"),
            runtime_strategy_no: RuntimeStrategyNo(42),
            strategy_file: package.clone(),
            markets: Arc::from([]),
            accounts: Arc::from([]),
            risk_scope: RiskScopeRef(Arc::from("synthetic")),
            runtime: StrategyRuntimeSpec {
                max_handler_duration: Duration::from_millis(100),
                ..Default::default()
            },
            recovery: StrategyRecoveryPolicy::Fresh,
            enabled: true,
            run_config_revision: 1,
        };
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
                        source_stream: market::SourceStreamId(1),
                        local_market_no: 0,
                        local_asset_no: 0,
                        source: market::MarketSourceHandle {
                            source_id: market::MarketSourceId(1),
                            generation: 1,
                        },
                        asset_id: 1,
                        data_mode: StrategyDataMode::Tick,
                    }]),
                    accounts: Arc::from([ResolvedAccountBinding {
                        local_account_no: 0,
                        account,
                        tradable_assets: Arc::from([StrategyTradableAsset {
                            local_asset_no: 0,
                            asset_id: 1,
                        }]),
                    }]),
                    execution: Arc::from([StrategyExecutionBinding {
                        local_account_no: 0,
                        assets: BTreeMap::from([(0, 1)]),
                        handle: executor.bind(
                            account,
                            Arc::new(Broker(calls.clone())),
                            Arc::new(Observer),
                        ),
                    }]),
                    state_snapshot_sink: Arc::new(Sink),
                    clock: Arc::new(Clock),
                    metrics: Arc::new(Metrics),
                    resources: scope.handle(),
                    activation: Arc::new(StrategyActivationGate::default()),
                },
            )
            .unwrap();
        runtime
            .seed_public_state(StrategyPublicStateSeedV13 {
                positions: Arc::from([position]),
                accounts: Arc::from([account_view]),
                active_orders: Arc::from([order]),
                order_boundaries: Arc::from([(0, 1)]),
                ..Default::default()
            })
            .unwrap();
        let lane = events
            .register_primary_async_lane(
                &[PrimarySubscriptionSpec {
                    event_type: Arc::from(market::BBO_EVENT),
                    schema_version: 1,
                    qos: titan_core_types::EventQos::ReliableOrdered,
                    routing_keys: Arc::from([1]),
                }],
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
        let mut offline = OfflineV13Adapter::new(OfflineV13Config {
            compiled_strategy: loaded.native.clone(),
            strategy_instance_id: 43,
            generation: 1,
            command_capacity: 8,
            markets: vec![TitanMarketView::default()],
            positions: vec![position],
            balances: vec![],
            accounts: vec![account_view],
            active_orders: vec![order],
        })
        .unwrap();
        offline.start(1_000_000_000).unwrap();
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
        let raw: Vec<_> = image
            .iter()
            .map(|v| TitanDepthView {
                price_ticks: v.price_ticks,
                qty_lots: v.quantity_lots,
                side: v.side,
                action: 1,
                receive_ts_ns: 1_000_000_000,
                ..Default::default()
            })
            .collect();
        let fact = |name: &'static str, sequence: u64, payload: &[u8]| {
            runtime.handle(EventView {
                event_type: name,
                schema_version: 1,
                payload,
                metadata: titan_core_types::EventPublishMetadata {
                    source_id: 1,
                    source_sequence: sequence,
                    routing_key: 1,
                    ..Default::default()
                },
                trace: Default::default(),
            })
        };
        for (stage, epoch) in [(1, 1), (3, 2)] {
            if case == 0 || (stage == 3 && case != 3) {
                continue;
            }
            let header = market::MarketBatchHeaderV1 {
                asset_id: 1,
                kind: 1,
                flags: 1,
                stream_epoch: epoch,
                first_update_sequence: epoch * 10,
                last_update_sequence: epoch * 10,
                receive_ts: 1_000_000_000,
                ..Default::default()
            };
            let bytes = market::encode_depth_batch(header, &image).unwrap();
            fact(market::DEPTH_BATCH_EVENT, stage, &bytes).unwrap();
            offline
                .on_depth_batch(
                    1_000_000_000,
                    DepthBatchInfoV1 {
                        version: 1,
                        valid: 1,
                        reset: 1,
                        stream_epoch: epoch,
                        first_update_sequence: epoch * 10,
                        last_update_sequence: epoch * 10,
                        publication_sequence: stage,
                        ..Default::default()
                    },
                    &raw,
                )
                .unwrap();
            if case >= 2 && stage == 1 {
                let mut bytes = 1_u32.to_le_bytes().to_vec();
                bytes.extend_from_slice(&1_u64.to_le_bytes());
                fact(market::STREAM_INVALIDATED_EVENT, 2, &bytes).unwrap();
                offline
                    .on_depth_batch(
                        1_000_000_000,
                        DepthBatchInfoV1 {
                            version: 1,
                            stream_epoch: 1,
                            publication_sequence: 2,
                            reset: 1,
                            ..Default::default()
                        },
                        &[],
                    )
                    .unwrap();
            }
        }
        let mode = match case {
            0 | 5 => 2,
            4 => 3,
            _ => 1,
        };
        let payload = market::encode_depth_batch(
            market::MarketBatchHeaderV1 {
                asset_id: 1,
                kind: 4,
                ..Default::default()
            },
            &[market::DepthItemV1 {
                side: 1,
                price_ticks: mode,
                quantity_lots: 1,
                action: 1,
                reserved: [0; 6],
            }],
        )
        .unwrap();
        let online = fact(market::BBO_EVENT, 4, &payload);
        let replay = offline.on_ticks(
            1_000_000_000,
            &[TitanTickView {
                asset_no: 0,
                side: 1,
                price_ticks: mode,
                qty_lots: 1,
                ..Default::default()
            }],
        );
        let accepted = !matches!(case, 2 | 4);
        assert_eq!(online.is_ok(), accepted, "online case {case}");
        assert_eq!(replay.is_ok(), accepted, "offline case {case}");
        let expected = if mode == 2 { 2 } else { 1 };
        if accepted {
            assert_eq!(replay.unwrap().len(), expected);
            let deadline = Instant::now() + Duration::from_secs(5);
            while calls.lock().unwrap().len() < expected {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            if mode == 2 {
                assert!(calls.lock().unwrap().contains(&"cancel"));
                assert!(calls.lock().unwrap().contains(&"reduce"));
            }
            assert_ne!(runtime.state().lifecycle, StrategyLifecycle::Failed);
        } else {
            assert!(calls.lock().unwrap().is_empty());
            assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Failed);
        }
        events
            .unregister_primary_async_lane_before(
                lane.token(),
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        engine.stop().unwrap();
        executor
            .shutdown(Instant::now() + Duration::from_secs(5))
            .unwrap();
        println!("case {case}: online/replay accepted={accepted}");
    }
    println!(
        "PASS: unavailable/ready/invalidated/recovered admission parity; cancel + proven reduction retained; opening and excess reduction rejected"
    );
}

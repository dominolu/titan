//! No exchange connection: test stop -> cancel -> flat fill -> position confirmation.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use titan_account_service as account;
use titan_core_types::{ComponentIdentity, EventView, ResourceScope};
use titan_event_engine::{
    EventClass, EventEngine, EventEngineConfig, PoolKind, PrimaryAsyncLaneConfig,
    PrimarySubscriptionSpec,
};
use titan_market_service::{BBO_EVENT, MarketSourceHandle, MarketSourceId};
use titan_strategy_runtime::*;

struct Clock(AtomicI64);
impl StrategyClock for Clock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}
struct Metrics;
impl StrategyMetrics for Metrics {}
#[derive(Default)]
struct Sink(Mutex<Vec<StrategyRecoveryPoint>>);
impl StrategyRecoveryStore for Sink {
    fn submit(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()> {
        self.0.lock().unwrap().push(snapshot);
        Ok(())
    }
    fn flush(&self, _: Instant) -> LocalResult<()> {
        Ok(())
    }
}
struct Observer;
impl account::ExecutionObserver for Observer {
    fn observe(&self, _: account::ObservedExecutionResult) {}
}
enum Request {
    Submit(account::DirectNewOrderRequest),
    Cancel,
}
struct Broker(mpsc::Sender<Request>);
impl account::DirectExecutionConnector for Broker {
    fn submit(&self, request: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        self.0.send(Request::Submit(request)).unwrap();
        Box::pin(async {
            Ok(account::DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("simulated".into()),
                status: 1,
            })
        })
    }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture {
        self.0.send(Request::Cancel).unwrap();
        Box::pin(async {
            Ok(account::DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("simulated".into()),
                status: 4,
            })
        })
    }
}
fn wait(runtime: &Arc<dyn StrategyRuntime>, id: StrategyOperationId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let state = runtime.operation(id);
        if state.state == StrategyOperationState::Succeeded {
            return;
        }
        assert_eq!(
            state.state,
            StrategyOperationState::Pending,
            "{}",
            state.detail
        );
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}
fn fact<T: account::AccountEventPayload>(
    runtime: &Arc<dyn StrategyRuntime>,
    kind: &str,
    version: u32,
    value: T,
) {
    let mut bytes = vec![0; T::ENCODED_LEN];
    value.encode_into(&mut bytes).unwrap();
    runtime
        .handle(EventView {
            event_type: kind,
            schema_version: version,
            payload: &bytes,
            metadata: Default::default(),
            trace: Default::default(),
        })
        .unwrap();
}
fn order(
    runtime: &Arc<dyn StrategyRuntime>,
    request: &account::DirectNewOrderRequest,
    status: u8,
    filled: i64,
    version: u64,
) {
    fact(
        runtime,
        account::ORDER_CHANGED_EVENT,
        1,
        account::OrderChangedV1 {
            header: header(version),
            asset_id: 1,
            side: if request.side == 1 { 1 } else { 2 },
            order_type: request.order_type,
            time_in_force: request.time_in_force,
            status,
            price_ticks: request.price_ticks,
            quantity_lots: request.quantity_lots,
            filled_quantity_lots: filled,
            client_order_id: request.client_order_id,
            ..Default::default()
        },
    );
}
fn header(version: u64) -> account::AccountEventHeaderV1 {
    account::AccountEventHeaderV1 {
        account_id: 8,
        account_generation: 1,
        account_epoch: 1,
        account_version: version,
        receive_ts: 1_000_000_000,
        ..Default::default()
    }
}
fn fill(
    runtime: &Arc<dyn StrategyRuntime>,
    request: &account::DirectNewOrderRequest,
    price: i64,
    version: u64,
) {
    fact(
        runtime,
        account::FILL_EVENT,
        2,
        account::FillV2 {
            header: header(version),
            asset_id: 1,
            side: if request.side == 1 { 1 } else { 2 },
            price_ticks: price,
            last_fill_quantity_lots: request.quantity_lots,
            cumulative_filled_quantity_lots: request.quantity_lots,
            client_order_id: request.client_order_id,
            ..Default::default()
        },
    );
}
fn position(runtime: &Arc<dyn StrategyRuntime>, qty: i64, version: u64) {
    fact(
        runtime,
        account::POSITION_CHANGED_EVENT,
        1,
        account::PositionChangedV1 {
            header: header(version),
            asset_id: 1,
            quantity_lots: qty,
            entry_price_ticks: if qty == 0 { 0 } else { 95 },
            ..Default::default()
        },
    );
}
fn main() {
    let path = std::fs::canonicalize(PathBuf::from(
        std::env::args_os().nth(1).expect("grid compiled_strategy"),
    ))
    .unwrap();
    let manifest = NativeStrategyLoaderV13::new(
        std::env::temp_dir().join("grid-stop-cache"),
        V13TrustPolicy::default(),
    )
    .inspect(&path)
    .unwrap();
    assert!(
        manifest
            .capabilities
            .contains(StrategyCapabilitiesV13::STOP_BARRIER)
    );
    let package = StrategyFileRef {
        loader_type: "native-v13".into(),
        uri: format!("file://{}", path.display()).into(),
        expected_digest: manifest.compiled_strategy_digest,
        signature_ref: None,
    };
    let compiled_strategy = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_strategy_roots: Arc::from([Arc::from(
                path.parent().unwrap().to_str().unwrap(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap()
        .load(
            StrategyLoadRequest {
                strategy_file: package.clone(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    offline_stop(compiled_strategy.native.clone());
    let definition = StrategyDefinition {
        strategy_key: "grid-stop".into(),
        runtime_strategy_no: RuntimeStrategyNo(42),
        strategy_file: package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: RiskScopeRef("offline".into()),
        runtime: Default::default(),
        recovery: StrategyRecoveryPolicy::Fresh,
        enabled: true,
        run_config_revision: 1,
    };
    let account = account::AccountHandle {
        account_id: account::AccountId(8),
        generation: 1,
    };
    let mut execution = account::ExecutionRuntime::new(1, 16).unwrap();
    let (tx, rx) = mpsc::channel();
    let clock = Arc::new(Clock(AtomicI64::new(1_000_000_000)));
    let scope = ResourceScope::new(ComponentIdentity::new("strategy", "grid-stop"));
    let sink = Arc::new(Sink::default());
    let source = MarketSourceHandle {
        source_id: MarketSourceId(1),
        generation: 1,
    };
    let runtime = NativeV13RuntimeFactory
        .create(
            &definition,
            compiled_strategy.clone(),
            StrategyRuntimeBuildContext {
                strategy: StrategyHandle {
                    runtime_strategy_no: RuntimeStrategyNo(42),
                    generation: 1,
                },
                compiled_strategy_digest: compiled_strategy.id,
                markets: Arc::from([ResolvedMarketBinding {
                    source_stream: titan_market_service::SourceStreamId(2),
                    local_market_no: 0,
                    local_asset_no: 0,
                    source,
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
                    assets: [(0, 1)].into_iter().collect(),
                    handle: execution.bind(account, Arc::new(Broker(tx)), Arc::new(Observer)),
                }]),
                state_snapshot_sink: sink.clone(),
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
                account_sequence: 1,
                ..Default::default()
            }]),
            positions: Arc::from([TitanPositionView {
                account_sequence: 1,
                ..Default::default()
            }]),
            balances: Arc::from([TitanBalanceView {
                total_units: 100000,
                available_units: 100000,
                ..Default::default()
            }]),
            order_boundaries: Arc::from([(0, 1)]),
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
                event_type: BBO_EVENT.into(),
                schema_version: 1,
                qos: titan_core_types::EventQos::Latest,
                routing_keys: Arc::from([1]),
            }],
            PrimaryAsyncLaneConfig::default(),
            runtime.clone(),
        )
        .unwrap();
    runtime.attach_lane(lane.clone()).unwrap();
    wait(&runtime, runtime.prepare().unwrap());
    wait(&runtime, runtime.start().unwrap());
    let header = titan_market_service::MarketBatchHeaderV1 {
        asset_id: 1,
        kind: 4,
        item_count: 2,
        receive_ts: clock.now_ns(),
        ..Default::default()
    };
    let mut payload = vec![
        0;
        titan_market_service::MarketBatchHeaderV1::ENCODED_LEN
            + 2 * titan_market_service::DepthItemV1::ENCODED_LEN
    ];
    header.encode_into_slice(&mut payload).unwrap();
    for (i, (side, price)) in [(1, 99), (2, 101)].into_iter().enumerate() {
        let offset = titan_market_service::MarketBatchHeaderV1::ENCODED_LEN
            + i * titan_market_service::DepthItemV1::ENCODED_LEN;
        titan_market_service::DepthItemV1 {
            price_ticks: price,
            quantity_lots: 10,
            side,
            action: 1,
            ..Default::default()
        }
        .encode_into_slice(
            &mut payload[offset..offset + titan_market_service::DepthItemV1::ENCODED_LEN],
        )
        .unwrap();
    }
    runtime
        .handle(EventView {
            event_type: BBO_EVENT,
            schema_version: 1,
            payload: &payload,
            metadata: titan_core_types::EventPublishMetadata {
                source_id: 2,
                ..Default::default()
            },
            trace: Default::default(),
        })
        .unwrap();
    let mut requests = Vec::new();
    while requests.len() < 2 {
        if let Request::Submit(request) = rx.recv_timeout(Duration::from_secs(3)).unwrap() {
            requests.push(request);
        }
    }
    let buy = requests.iter().find(|r| r.side == 1).unwrap();
    let sell = requests.iter().find(|r| r.side == -1).unwrap();
    order(&runtime, buy, 1, 0, 1);
    order(&runtime, sell, 1, 0, 1);
    let checkpoint = runtime
        .freeze_state(StrategyStateSnapshotRequest { checkpoint_id: 17 })
        .unwrap();
    wait(&runtime, checkpoint);
    recovery_probe(
        &definition,
        compiled_strategy.clone(),
        sink.0.lock().unwrap().last().unwrap().clone(),
    );
    assert!(runtime.pause_supported());
    wait(&runtime, runtime.pause(PauseReason::User).unwrap());
    assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Paused);
    assert!(!runtime.state().activation_gate_open);
    runtime
        .handle(EventView {
            event_type: BBO_EVENT,
            schema_version: 1,
            payload: &payload,
            metadata: titan_core_types::EventPublishMetadata {
                source_id: 2,
                ..Default::default()
            },
            trace: Default::default(),
        })
        .unwrap();
    order(&runtime, buy, 3, 2, 2);
    fill(&runtime, buy, 95, 3);
    position(&runtime, 2, 4);
    assert_eq!(
        runtime.state().lifecycle,
        StrategyLifecycle::Paused,
        "paused fills must remain consumable without opening risk"
    );
    wait(&runtime, runtime.resume().unwrap());
    let stop = runtime
        .stop(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        runtime
            .stop(Instant::now() + Duration::from_secs(30))
            .unwrap(),
        stop,
        "repeated stop must preserve the original operation/deadline"
    );
    lane.submit_safe_point(|| Ok(()))
        .unwrap()
        .wait(Instant::now() + Duration::from_secs(3))
        .unwrap();
    order(&runtime, sell, 4, 0, 5);
    assert_eq!(
        runtime.state().lifecycle,
        StrategyLifecycle::Stopping,
        "no active orders does not imply flat inventory"
    );
    assert_eq!(
        runtime.operation(stop).state,
        StrategyOperationState::Pending
    );
    clock.0.store(1_600_000_000, Ordering::Release);
    runtime.fire_timer(1).unwrap();
    lane.submit_safe_point(|| Ok(()))
        .unwrap()
        .wait(Instant::now() + Duration::from_secs(3))
        .unwrap();
    let close = loop {
        if let Request::Submit(request) = rx.recv_timeout(Duration::from_secs(3)).unwrap() {
            break request;
        }
    };
    assert!(close.reduce_only);
    assert_eq!(close.quantity_lots, 2);
    order(&runtime, &close, 3, 2, 6);
    fill(&runtime, &close, 98, 7);
    assert_eq!(
        runtime.state().lifecycle,
        StrategyLifecycle::Stopping,
        "fill must wait for authoritative position projection"
    );
    position(&runtime, 0, 8);
    wait(&runtime, stop);
    assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Stopped);
    engine.stop().unwrap();
    execution
        .shutdown(Instant::now() + Duration::from_secs(3))
        .unwrap();
    println!(
        "V13 grid stop barrier passed: cancel confirmation, reduce-only exit, position confirmation"
    );
}

fn recovery_probe(
    definition: &StrategyDefinition,
    compiled_strategy: LoadedStrategy,
    snapshot: StrategyRecoveryPoint,
) {
    let outcomes: Vec<_> = snapshot
        .orders_list
        .iter()
        .map(|order| TitanActiveOrderView {
            order_id: order.order_id,
            account_no: order.account_no,
            asset_no: order.asset_no,
            side: order.side,
            order_type: order.order_type,
            time_in_force: order.time_in_force,
            price_ticks: order.price_ticks,
            qty_lots: order.qty_lots,
            cumulative_filled_lots: order.cumulative_filled_lots,
            reduce_only: order.reduce_only,
            status: 6,
            account_sequence: 10,
            ..Default::default()
        })
        .collect();
    assert_eq!(outcomes.len(), 2);
    for scenario in 0..3 {
        let activation = Arc::new(StrategyActivationGate::default());
        let scope = ResourceScope::new(ComponentIdentity::new("strategy", "grid-recovery"));
        let sink = Arc::new(Sink::default());
        let runtime = NativeV13RuntimeFactory
            .create(
                definition,
                compiled_strategy.clone(),
                StrategyRuntimeBuildContext {
                    strategy: StrategyHandle {
                        runtime_strategy_no: RuntimeStrategyNo(42),
                        generation: 2,
                    },
                    compiled_strategy_digest: compiled_strategy.id,
                    markets: Arc::from([ResolvedMarketBinding {
                        source_stream: titan_market_service::SourceStreamId(2),
                        local_market_no: 0,
                        local_asset_no: 0,
                        source: MarketSourceHandle {
                            source_id: MarketSourceId(1),
                            generation: 1,
                        },
                        asset_id: 1,
                        data_mode: StrategyDataMode::Tick,
                    }]),
                    accounts: Arc::from([ResolvedAccountBinding {
                        local_account_no: 0,
                        account: account::AccountHandle {
                            account_id: account::AccountId(8),
                            generation: 2,
                        },
                        tradable_assets: Arc::from([StrategyTradableAsset {
                            local_asset_no: 0,
                            asset_id: 1,
                        }]),
                    }]),
                    execution: Arc::from([]),
                    state_snapshot_sink: sink.clone(),
                    clock: Arc::new(Clock(AtomicI64::new(1_600_000_000))),
                    metrics: Arc::new(Metrics),
                    resources: scope.handle(),
                    activation: activation.clone(),
                },
            )
            .unwrap();
        let mut results = outcomes.clone();
        if scenario == 1 {
            results.clear();
        }
        if scenario == 2 {
            results[0].cumulative_filled_lots = 1;
        }
        runtime
            .seed_public_state(StrategyPublicStateSeedV13 {
                accounts: Arc::from([TitanAccountView {
                    state: 4,
                    account_epoch: 2,
                    account_sequence: 10,
                    ..Default::default()
                }]),
                positions: Arc::from([TitanPositionView {
                    account_sequence: 10,
                    ..Default::default()
                }]),
                balances: Arc::from([TitanBalanceView {
                    total_units: 100000,
                    available_units: 100000,
                    ..Default::default()
                }]),
                order_boundaries: Arc::from([(0, 10)]),
                recovery_orders: results.into(),
                ..Default::default()
            })
            .unwrap();
        let restored = runtime.restore_state(snapshot.clone());
        assert!(
            !activation.is_open(),
            "recovery cannot reopen the execution gate"
        );
        if scenario == 0 {
            restored.unwrap();
            assert!(
                sink.0
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap()
                    .orders_list
                    .iter()
                    .all(|order| order.status == 6)
            );
        } else {
            assert!(
                restored.is_err(),
                "missing outcomes and unconsumed fills must fail closed"
            );
        }
    }
    println!(
        "V13 grid recovery passed: proven cancels, missing results rejected, changed fills rejected, gate closed"
    );
}

fn offline_state(
    adapter: &mut OfflineV13Adapter,
    qty: i64,
    sequence: u64,
    active: Vec<TitanActiveOrderView>,
) {
    adapter.update_public_state(
        vec![TitanMarketView {
            best_bid_ticks: 99,
            best_ask_ticks: 101,
            ..Default::default()
        }],
        vec![TitanPositionView {
            qty_lots: qty,
            account_sequence: sequence,
            ..Default::default()
        }],
        vec![TitanBalanceView {
            available_units: 100000,
            total_units: 100000,
            ..Default::default()
        }],
        vec![TitanAccountView {
            state: 4,
            account_epoch: 1,
            account_sequence: sequence,
            ..Default::default()
        }],
        active,
    );
}

fn offline_stop(compiled_strategy: Arc<LoadedNativeStrategyV13>) {
    let mut adapter = OfflineV13Adapter::new(OfflineV13Config {
        compiled_strategy,
        strategy_instance_id: 91,
        generation: 1,
        command_capacity: 64,
        markets: vec![],
        positions: vec![],
        balances: vec![],
        accounts: vec![],
        active_orders: vec![],
    })
    .unwrap();
    offline_state(&mut adapter, 0, 1, vec![]);
    adapter.start(1_000_000_000).unwrap();
    let commands = adapter
        .on_ticks(
            1_000_000_000,
            &[TitanTickView {
                receive_ts_ns: 1_000_000_000,
                ..Default::default()
            }],
        )
        .unwrap();
    let mut active = vec![];
    for command in commands {
        if let StagedCommandV13::Submit { order_id, request } = command {
            active.push(TitanActiveOrderView {
                order_id,
                side: request.side,
                price_ticks: request.price_ticks,
                qty_lots: request.qty_lots,
                status: 2,
                order_type: 1,
                time_in_force: 4,
                created_ts_ns: 1_000_000_000,
                ..Default::default()
            });
        }
    }
    assert_eq!(active.len(), 2);
    let buy = *active.iter().find(|order| order.side == 1).unwrap();
    let sell = *active.iter().find(|order| order.side == 2).unwrap();
    offline_state(&mut adapter, 2, 2, vec![sell]);
    adapter
        .on_fills(
            1_100_000_000,
            &[TitanFillView {
                order_id: buy.order_id,
                side: 1,
                fill_price_ticks: 95,
                fill_qty_lots: 2,
                cumulative_filled_lots: 2,
                account_sequence: 2,
                final_fill: 1,
                ..Default::default()
            }],
        )
        .unwrap();
    adapter.stop(1_100_000_000).unwrap();
    assert!(!adapter.is_stopped());
    offline_state(&mut adapter, 2, 2, vec![]);
    adapter
        .on_orders(
            1_100_000_000,
            &[TitanOrderEventView {
                order_id: sell.order_id,
                price_ticks: sell.price_ticks,
                qty_lots: sell.qty_lots,
                status: 6,
                ..Default::default()
            }],
        )
        .unwrap();
    assert!(
        !adapter.is_stopped(),
        "offline adapter must remain callable while inventory exists"
    );
    let commands = adapter
        .on_timer(1_600_000_000, &TitanTimerView::default())
        .unwrap();
    let (exit_id, request) = match commands.first().unwrap() {
        StagedCommandV13::Submit { order_id, request } => (*order_id, *request),
        _ => panic!("expected reduce-only exit"),
    };
    assert_eq!(request.reduce_only, 1);
    offline_state(&mut adapter, 0, 3, vec![]);
    adapter
        .on_fills(
            1_700_000_000,
            &[TitanFillView {
                order_id: exit_id,
                side: 2,
                fill_price_ticks: 98,
                fill_qty_lots: 2,
                cumulative_filled_lots: 2,
                account_sequence: 3,
                final_fill: 1,
                ..Default::default()
            }],
        )
        .unwrap();
    assert!(adapter.is_stopped());
    println!("V13 offline grid stop barrier passed");
}

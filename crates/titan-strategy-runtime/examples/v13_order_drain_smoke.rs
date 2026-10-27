//! Deterministic native runtime regression: REST terminal -> Stop -> Order -> Fill.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
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
    snapshots: Mutex<Vec<StrategyRecoveryPoint>>,
    fail_flush: std::sync::atomic::AtomicBool,
}
impl StrategyRecoveryStore for Sink {
    fn submit(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()> {
        self.snapshots.lock().unwrap().push(snapshot);
        Ok(())
    }
    fn flush(&self, _: Instant) -> LocalResult<()> {
        if self.fail_flush.load(std::sync::atomic::Ordering::Acquire) {
            Err(StrategyError::new(
                StrategyErrorKind::Internal,
                "checkpoint",
                "checkpoint_write_failed",
                "injected durable-write failure",
            ))
        } else {
            Ok(())
        }
    }
}
struct Broker;
impl account::DirectExecutionConnector for Broker {
    fn submit(&self, _: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        Box::pin(async {
            Ok(account::DirectOrderInfo {
                client_order_id: None,
                venue_order_id: Some("venue-1".into()),
                status: 3,
            })
        })
    }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture {
        unreachable!("smoke strategy has no cancel callback")
    }
}
struct Observer {
    sender: mpsc::Sender<account::ObservedExecutionResult>,
    target: Mutex<Option<std::sync::Weak<dyn StrategyRuntime>>>,
}
impl account::ExecutionObserver for Observer {
    fn observe(&self, value: account::ObservedExecutionResult) {
        self.sender.send(value.clone()).unwrap();
        if matches!(
            value.outcome,
            account::ObservedExecutionOutcome::Rejected(_)
        ) && let Some(target) = self
            .target
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
        {
            let _ = target.execution_result(value);
        }
    }
}
struct PendingBroker;
impl account::DirectExecutionConnector for PendingBroker {
    fn submit(&self, _: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        Box::pin(std::future::pending())
    }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture {
        Box::pin(std::future::pending())
    }
}
fn wait(runtime: &Arc<dyn StrategyRuntime>, id: StrategyOperationId) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let value = runtime.operation(id);
        if value.state == StrategyOperationState::Succeeded {
            break;
        }
        assert_eq!(
            value.state,
            StrategyOperationState::Pending,
            "{}",
            value.detail
        );
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}
fn fact<T: account::AccountEventPayload>(
    runtime: &Arc<dyn StrategyRuntime>,
    event_type: &str,
    version: u32,
    value: T,
) -> Result<(), titan_core_types::CoreError> {
    let mut payload = vec![0; T::ENCODED_LEN];
    value.encode_into(&mut payload).unwrap();
    runtime.handle(EventView {
        event_type,
        schema_version: version,
        payload: &payload,
        metadata: Default::default(),
        trace: Default::default(),
    })
}
fn main() {
    let compiled_strategy = std::fs::canonicalize(PathBuf::from(
        std::env::args_os().nth(1).expect("compiled_strategy"),
    ))
    .unwrap();
    run(compiled_strategy.clone(), false, false, false);
    run(compiled_strategy.clone(), true, false, false);
    run(compiled_strategy.clone(), false, true, false);
    run(compiled_strategy, false, false, true);
}
fn run(compiled_strategy: PathBuf, fail_flush: bool, queue_overload: bool, intent_failure: bool) {
    let manifest = NativeStrategyLoaderV13::new(
        std::env::temp_dir().join("titan-drain-inspect"),
        V13TrustPolicy::default(),
    )
    .inspect(&compiled_strategy)
    .unwrap();
    let package = StrategyFileRef {
        loader_type: Arc::from("native-v13"),
        uri: Arc::from(format!("file://{}", compiled_strategy.display())),
        expected_digest: manifest.compiled_strategy_digest,
        signature_ref: None,
    };
    let loader = NativeV13LoaderFactory
        .create(StrategyLoaderContext {
            allowed_strategy_roots: Arc::from([Arc::from(
                compiled_strategy
                    .parent()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref(),
            )]),
            require_signature: false,
            ed25519_keys: BTreeMap::new(),
        })
        .unwrap();
    let compiled_strategy = loader
        .load(
            StrategyLoadRequest {
                strategy_file: package.clone(),
            },
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
    let definition = StrategyDefinition {
        strategy_key: Arc::from("drain-smoke"),
        runtime_strategy_no: RuntimeStrategyNo(42),
        strategy_file: package,
        markets: Arc::from([]),
        accounts: Arc::from([]),
        risk_scope: RiskScopeRef(Arc::from("offline")),
        runtime: Default::default(),
        recovery: StrategyRecoveryPolicy::Fresh,
        enabled: true,
        run_config_revision: 1,
    };
    let handle = StrategyHandle {
        runtime_strategy_no: RuntimeStrategyNo(42),
        generation: 1,
    };
    let account = account::AccountHandle {
        account_id: account::AccountId(8),
        generation: 1,
    };
    let mut execution =
        account::ExecutionRuntime::new(1, if queue_overload { 1 } else { 2 }).unwrap();
    let (tx, rx) = mpsc::channel();
    let observer = Arc::new(Observer {
        sender: tx,
        target: Mutex::new(None),
    });
    if queue_overload {
        execution
            .bind(account, Arc::new(PendingBroker), observer.clone())
            .submit(account::DirectNewOrderRequest {
                position_side: 0,
                asset_id: account::AssetId(1),
                side: 1,
                order_type: 0,
                time_in_force: 0,
                price_ticks: 100,
                quantity_lots: 1,
                reduce_only: false,
                client_order_id: account::Id128::default(),
            })
            .unwrap();
    }
    let sink = Arc::new(Sink {
        snapshots: Mutex::new(Vec::new()),
        fail_flush: std::sync::atomic::AtomicBool::new(intent_failure),
    });
    let scope = ResourceScope::new(ComponentIdentity::new("strategy", "drain-smoke"));
    let runtime = NativeV13RuntimeFactory
        .create(
            &definition,
            compiled_strategy.clone(),
            StrategyRuntimeBuildContext {
                strategy: handle,
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
                    account,
                    tradable_assets: Arc::from([StrategyTradableAsset {
                        local_asset_no: 0,
                        asset_id: 1,
                    }]),
                }]),
                execution: Arc::from([StrategyExecutionBinding {
                    local_account_no: 0,
                    assets: [(0, 1)].into_iter().collect(),
                    handle: execution.bind(account, Arc::new(Broker), observer.clone()),
                }]),
                state_snapshot_sink: sink.clone(),
                clock: Arc::new(Clock),
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
            positions: Arc::from([TitanPositionView::default()]),
            order_boundaries: Arc::from([(0, 1)]),
            ..Default::default()
        })
        .unwrap();
    *observer.target.lock().unwrap() = Some(Arc::downgrade(&runtime));
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
                capacity: 64,
                critical_reserve: 8,
                reliable_pending_capacity: 16,
                snapshot_staging_capacity: 64,
                control_capacity: if queue_overload { 1 } else { 64 },
                ..Default::default()
            },
            runtime.clone(),
        )
        .unwrap();
    runtime.attach_lane(lane.clone()).unwrap();
    wait(&runtime, runtime.prepare().unwrap());
    wait(&runtime, runtime.start().unwrap());
    let mut release = None;
    if queue_overload {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        lane.submit_safe_point(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        lane.submit_safe_point(|| Ok(())).unwrap();
        release = Some(release_tx);
    }
    let header = titan_market_service::MarketBatchHeaderV1 {
        asset_id: 1,
        kind: 4,
        item_count: 1,
        receive_ts: 1_000_000_000,
        ..Default::default()
    };
    let item = titan_market_service::DepthItemV1 {
        price_ticks: 2,
        quantity_lots: 1,
        side: 1,
        action: 1,
        ..Default::default()
    };
    let mut payload = vec![
        0;
        titan_market_service::MarketBatchHeaderV1::ENCODED_LEN
            + titan_market_service::DepthItemV1::ENCODED_LEN
    ];
    header.encode_into_slice(&mut payload).unwrap();
    item.encode_into_slice(&mut payload[titan_market_service::MarketBatchHeaderV1::ENCODED_LEN..])
        .unwrap();
    let tick_result = runtime.handle(EventView {
        event_type: BBO_EVENT,
        schema_version: 1,
        payload: &payload,
        metadata: titan_core_types::EventPublishMetadata {
            source_id: 2,
            ..Default::default()
        },
        trace: Default::default(),
    });
    if intent_failure {
        assert!(tick_result.is_err());
        assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Failed);
        assert!(
            rx.try_recv().is_err(),
            "no order may escape before durable intent flush"
        );
        assert_eq!(execution.active_tasks(), 0);
        engine.stop().unwrap();
        execution.shutdown(Instant::now()).unwrap();
        println!("V13 failed intent durability prevents external dispatch");
        return;
    }
    if queue_overload {
        assert!(tick_result.is_err());
        assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Failed);
        release.take().unwrap().send(()).unwrap();
        engine.stop().unwrap();
        assert!(execution.shutdown(Instant::now()).is_err());
        println!("V13 synchronous local rejection with a full result queue fails without deadlock");
        return;
    }
    tick_result.unwrap();
    let accepted = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let client_id = accepted.client_order_id.unwrap();
    runtime.execution_result(accepted).unwrap();
    lane.submit_safe_point(|| Ok(()))
        .unwrap()
        .wait(Instant::now() + Duration::from_secs(3))
        .unwrap();
    let stop = runtime
        .stop(Instant::now() + Duration::from_secs(3))
        .unwrap();
    lane.submit_safe_point(|| Ok(()))
        .unwrap()
        .wait(Instant::now() + Duration::from_secs(3))
        .unwrap();
    assert_eq!(
        runtime.state().lifecycle,
        StrategyLifecycle::Stopping,
        "REST terminal acceptance must wait for canonical fills"
    );
    let header = account::AccountEventHeaderV1 {
        account_id: 8,
        account_generation: 1,
        account_epoch: 1,
        account_version: 2,
        receive_ts: 1_000_000_001,
        ..Default::default()
    };
    fact(
        &runtime,
        account::ORDER_CHANGED_EVENT,
        1,
        account::OrderChangedV1 {
            header,
            asset_id: 1,
            side: 1,
            status: 3,
            quantity_lots: 2,
            filled_quantity_lots: 2,
            client_order_id: client_id,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        runtime.state().lifecycle,
        StrategyLifecycle::Stopping,
        "Order must not discard the following Fill"
    );
    sink.fail_flush
        .store(fail_flush, std::sync::atomic::Ordering::Release);
    let filled = fact(
        &runtime,
        account::FILL_EVENT,
        2,
        account::FillV2 {
            header: account::AccountEventHeaderV1 {
                account_version: 3,
                ..header
            },
            asset_id: 1,
            side: 1,
            last_fill_quantity_lots: 2,
            cumulative_filled_quantity_lots: 2,
            client_order_id: client_id,
            ..Default::default()
        },
    );
    if fail_flush {
        assert!(filled.is_err());
        assert_eq!(runtime.state().lifecycle, StrategyLifecycle::Failed);
        assert_eq!(
            runtime.operation(stop).state,
            StrategyOperationState::Failed
        );
    } else {
        filled.unwrap();
        wait(&runtime, stop);
    }
    assert_eq!(
        sink.snapshots.lock().unwrap().last().unwrap().orders_list[0].fill_count,
        1
    );
    engine.stop().unwrap();
    execution
        .shutdown(Instant::now() + Duration::from_secs(3))
        .unwrap();
    println!("V13 REST -> Stop -> Order -> Fill drain regression passed, fail_flush={fail_flush}");
}

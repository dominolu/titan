//! Offline Service -> Factory -> Instance recovery, using a compiled artifact and empty services.
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use titan_account_service::{self as account, AccountEventPayload};
use titan_event_engine::{EventClass, EventEngine, EventEngineConfig, PoolKind};
use titan_market_service::*;
use titan_strategy_runtime::*;

struct MockMarket;
impl MarketService for MockMarket {
    fn resolve(&self, _: &str) -> titan_market_service::LocalResult<MarketSourceHandle> {
        Ok(MarketSourceHandle {
            source_id: MarketSourceId(1),
            generation: 1,
        })
    }
    fn subscribe(
        &self,
        _: MarketSourceHandle,
        _: MarketSubscribeRequest,
    ) -> titan_market_service::LocalResult<MarketSubscription> {
        Ok(MarketSubscription { id: 1 })
    }
    fn unsubscribe(
        &self,
        _: MarketSourceHandle,
        _: MarketSubscription,
    ) -> titan_market_service::LocalResult<OperationId> {
        Ok(OperationId(1))
    }
    fn request_snapshot(
        &self,
        _: MarketSourceHandle,
        _: AssetId,
    ) -> titan_market_service::LocalResult<OperationId> {
        Ok(OperationId(1))
    }
    fn instruments(
        &self,
        _: MarketSourceHandle,
    ) -> titan_market_service::LocalResult<Arc<[InstrumentSnapshot]>> {
        Ok(Arc::from([InstrumentSnapshot {
            native_symbol: Arc::from("offline"),
            asset_id: AssetId(1),
            available: true,
        }]))
    }
    fn health(
        &self,
        _: MarketSourceHandle,
    ) -> titan_market_service::LocalResult<ConnectorHealthSnapshot> {
        Ok(ConnectorHealthSnapshot {
            state: ConnectorHealth::Running,
            message: Arc::from("offline mock"),
            observed_at: std::time::SystemTime::now(),
        })
    }
    fn operation(
        &self,
        _: MarketSourceHandle,
        id: OperationId,
    ) -> titan_market_service::LocalResult<ConnectorOperationSnapshot> {
        Ok(ConnectorOperationSnapshot {
            id,
            state: OperationState::Succeeded,
            detail: Arc::from("offline mock"),
        })
    }
}

// BBO-only artifact with a bound account: host facts must be routed even without
// strategy account callbacks. No connector can contact an exchange in this test.
struct MockAccounts;
impl MockAccounts {
    fn handle() -> account::AccountHandle {
        account::AccountHandle {
            account_id: account::AccountId(8),
            generation: 1,
        }
    }
    fn snapshot<T>() -> account::AccountStateSnapshot<T> {
        account::AccountStateSnapshot {
            account: Self::handle(),
            state: account::AccountSnapshotState::Ready,
            committed_epoch: Some(1),
            committed_version: Some(1),
            captured_at: 1,
            items: Arc::from([]),
        }
    }
}
impl account::AccountService for MockAccounts {
    fn resolve(&self, _: &str) -> account::LocalResult<account::AccountHandle> {
        Ok(Self::handle())
    }
    fn orders(
        &self,
        _: account::AccountHandle,
        _: account::OrderFilter,
    ) -> account::LocalResult<account::AccountStateSnapshot<account::OrderSnapshot>> {
        Ok(Self::snapshot())
    }
    fn positions(
        &self,
        _: account::AccountHandle,
        _: account::PositionFilter,
    ) -> account::LocalResult<account::AccountStateSnapshot<account::PositionSnapshot>> {
        Ok(Self::snapshot())
    }
    fn balances(
        &self,
        _: account::AccountHandle,
    ) -> account::LocalResult<account::AccountStateSnapshot<account::BalanceSnapshot>> {
        Ok(Self::snapshot())
    }
    fn health(
        &self,
        _: account::AccountHandle,
    ) -> account::LocalResult<account::AccountConnectorHealthSnapshot> {
        Ok(account::AccountConnectorHealthSnapshot {
            state: account::AccountLifecycle::Ready,
            message: Arc::from("offline"),
            observed_at: std::time::SystemTime::now(),
        })
    }
    fn diagnostics(
        &self,
        _: account::AccountHandle,
    ) -> account::LocalResult<account::AccountConnectorDiagnosticSnapshot> {
        unreachable!()
    }
    fn execution_connector(
        &self,
        _: account::AccountHandle,
    ) -> account::LocalResult<Arc<dyn account::DirectExecutionConnector>> {
        Ok(Arc::new(MockBroker))
    }
}
struct MockBroker;
impl account::DirectExecutionConnector for MockBroker {
    fn submit(&self, _: account::DirectNewOrderRequest) -> account::ExecutionFuture {
        panic!("unexpected submission")
    }
    fn cancel(&self, _: account::DirectCancelOrderRequest) -> account::ExecutionFuture {
        panic!("unexpected cancellation")
    }
}
struct Observer;
impl account::ExecutionObserver for Observer {
    fn observe(&self, _: account::ObservedExecutionResult) {}
}

fn wait(service: &StrategyServiceCore, operation: StrategyOperationId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = service.operation(operation);
        match snapshot.state {
            StrategyOperationState::Succeeded => return,
            StrategyOperationState::Failed => panic!("operation failed: {}", snapshot.detail),
            StrategyOperationState::Pending => {
                assert!(Instant::now() < deadline, "operation timed out")
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn main() {
    let artifact =
        std::fs::canonicalize(PathBuf::from(std::env::args_os().nth(1).expect("artifact")))
            .unwrap();
    let root = PathBuf::from(std::env::args_os().nth(2).expect("verification directory"));
    std::fs::create_dir_all(&root).unwrap();
    let loaded = NativeArtifactLoaderV13::new(root.join("inspect"), V13TrustPolicy::default())
        .inspect(&artifact)
        .unwrap();
    let engine = EventEngine::new(EventEngineConfig::default()).unwrap();
    engine
        .handle()
        .register_event(BBO_EVENT, 1, EventClass::Market, PoolKind::MarketBatch)
        .unwrap();
    for event_type in [
        account::ORDER_CHANGED_EVENT,
        account::FILL_EVENT,
        account::POSITION_CHANGED_EVENT,
        account::BALANCE_CHANGED_EVENT,
        account::STREAM_STATE_CHANGED_EVENT,
        account::STREAM_INVALIDATED_EVENT,
    ] {
        engine
            .handle()
            .register_event(
                event_type,
                if event_type == account::FILL_EVENT {
                    account::FILL_EVENT_SCHEMA_VERSION
                } else {
                    account::ACCOUNT_EVENT_SCHEMA_VERSION
                },
                EventClass::Critical,
                PoolKind::SmallEvent,
            )
            .unwrap();
    }
    let execution = account::ExecutionRuntime::new(1, 16).unwrap();
    engine.start().unwrap();
    let mut generation = 0;
    for recovery in [
        StrategyRecoveryPolicy::Fresh,
        StrategyRecoveryPolicy::RequireCheckpoint,
    ] {
        let loaders = Arc::new(StrategyPackageLoaderRegistry::default());
        loaders.register(Arc::new(NativeV13LoaderFactory)).unwrap();
        let runtimes = Arc::new(StrategyRuntimeFactoryRegistry::default());
        runtimes
            .register(Arc::new(NativeV13RuntimeFactory))
            .unwrap();
        let service = StrategyServiceCore::new(
            StrategyCoreConfig {
                checkpoint_root: Some(root.join("checkpoints")),
                require_artifact_signature: false,
                allowed_artifact_roots: Arc::from([Arc::from(
                    artifact.parent().unwrap().to_string_lossy().as_ref(),
                )]),
                allowed_event_types: [(Arc::from(BBO_EVENT), 1)].into_iter().collect(),
                ..Default::default()
            },
            StrategyServiceDependencies {
                events: engine.handle(),
                markets: Arc::new(MockMarket),
                accounts: Arc::new(MockAccounts),
                execution_dispatcher: Some(execution.dispatcher()),
                execution_observer: Some(Arc::new(Observer)),
            },
            loaders,
            runtimes,
        )
        .unwrap();
        let strategy = service
            .create(StrategyDefinition {
                strategy_key: Arc::from("recovery-smoke"),
                strategy_id: StrategyId(77),
                package: StrategyPackageRef {
                    loader_type: Arc::from("native-v13"),
                    uri: Arc::from(format!("file://{}", artifact.display())),
                    expected_digest: loaded.artifact_digest,
                    signature_ref: None,
                },
                markets: Arc::from([StrategyMarketBinding {
                    local_market_no: 0,
                    local_asset_no: 0,
                    source_key: Arc::from("offline"),
                    asset_id: 1,
                    data_mode: StrategyDataMode::Tick,
                }]),
                accounts: Arc::from([StrategyAccountBinding {
                    local_account_no: 0,
                    account_key: Arc::from("offline"),
                    tradable_assets: Arc::from([StrategyTradableAsset {
                        local_asset_no: 0,
                        asset_id: 1,
                    }]),
                }]),
                risk_scope: RiskScopeRef(Arc::from("offline")),
                runtime: StrategyRuntimeSpec {
                    async_lane_capacity: 64,
                    critical_reserve: 8,
                    reliable_pending_capacity: 16,
                    timer_interval: Some(Duration::from_millis(10)),
                    ..Default::default()
                },
                recovery,
                enabled: true,
                definition_version: 1,
            })
            .unwrap();
        assert!(strategy.generation > generation);
        generation = strategy.generation;
        wait(&service, service.prepare(strategy).unwrap());
        wait(&service, service.start(strategy).unwrap());
        if recovery == StrategyRecoveryPolicy::RequireCheckpoint {
            let invalidated = account::StreamInvalidatedV1(account::StreamStateV1 {
                header: account::AccountEventHeaderV1 {
                    account_id: 8,
                    account_generation: 1,
                    account_epoch: 1,
                    account_version: 2,
                    ..Default::default()
                },
                state: account::AccountLifecycle::Invalidated as u8,
                reason_code: 1,
            });
            let mut bytes = vec![0; account::StreamInvalidatedV1::ENCODED_LEN];
            invalidated.encode_into(&mut bytes).unwrap();
            let mut request = titan_event_engine::PublishRequest::new(
                account::STREAM_INVALIDATED_EVENT,
                1,
                &bytes,
            );
            request.routing_key = 8;
            engine.handle().try_publish(request).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while service.state(strategy).unwrap().lifecycle != StrategyLifecycle::Paused
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            let state = service.state(strategy).unwrap();
            assert_eq!(
                state.lifecycle,
                StrategyLifecycle::Paused,
                "BBO-only service strategy missed host invalidation"
            );
            assert!(!state.activation_gate_open);
        }

        wait(
            &service,
            service
                .stop(strategy, Instant::now() + Duration::from_secs(5))
                .unwrap(),
        );
        assert_eq!(
            service.state(strategy).unwrap().lifecycle,
            StrategyLifecycle::Stopped
        );
        service
            .quiesce(Instant::now() + Duration::from_secs(5))
            .unwrap();
    }
    engine.stop().unwrap();
    println!("V13 Service -> Factory -> Instance recovery passed, generation={generation}");
}

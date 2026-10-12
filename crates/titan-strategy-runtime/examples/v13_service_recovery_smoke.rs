//! Offline Service -> Factory -> Instance recovery, using a compiled artifact and empty services.
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use titan_account_service::{AccountCoreConfig, AccountServiceCore};
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
                accounts: AccountServiceCore::new(AccountCoreConfig::default()),
                execution_dispatcher: None,
                execution_observer: None,
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
                accounts: Arc::from([]),
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

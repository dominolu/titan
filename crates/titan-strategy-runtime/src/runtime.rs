use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::Instant,
};

use titan_account_service::ExecutionHandle;
use titan_core_types::{EventHandler, ResourceScopeHandle};
use titan_event_engine::PrimaryAsyncLaneHandle;

use crate::*;

#[derive(Default)]
pub struct StrategyActivationGate(AtomicBool);
impl StrategyActivationGate {
    pub fn open(&self) { self.0.store(true, Ordering::Release); }
    pub fn close(&self) { self.0.store(false, Ordering::Release); }
    pub fn is_open(&self) -> bool { self.0.load(Ordering::Acquire) }
}

pub trait StrategyClock: Send + Sync { fn now_ns(&self) -> i64; }
pub trait StrategyMetrics: Send + Sync { fn callback_duration(&self, _kind: V13EventKind, _duration_ns: u64) {} }

#[derive(Clone, Debug)]
pub struct StrategyPrivateStateSnapshot {
    pub checkpoint_id: u64,
    pub strategy: StrategyHandle,
    pub generation: u64,
    pub event_committed_sequence: u64,
    pub artifact_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub abi_version: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_alignment: u32,
    pub state_bytes: Arc<[u8]>,
    pub public_state_identity: [u8; 32],
    pub checksum: [u8; 32],
}

pub trait StrategyStateSnapshotSink: Send + Sync {
    fn submit(&self, snapshot: StrategyPrivateStateSnapshot) -> LocalResult<()>;
}

#[derive(Clone, Debug, Default)]
pub struct StrategyPublicStateSeedV13 {
    pub positions: Arc<[TitanPositionView]>,
    pub balances: Arc<[TitanBalanceView]>,
    pub accounts: Arc<[TitanAccountView]>,
    pub active_orders: Arc<[TitanActiveOrderView]>,
}

pub struct StrategyRuntimeBuildContext {
    pub strategy: StrategyHandle,
    pub artifact_id: StrategyArtifactId,
    pub markets: Arc<[ResolvedMarketBinding]>,
    pub accounts: Arc<[ResolvedAccountBinding]>,
    pub execution: Arc<[StrategyExecutionBinding]>,
    pub state_snapshot_sink: Arc<dyn StrategyStateSnapshotSink>,
    pub clock: Arc<dyn StrategyClock>,
    pub metrics: Arc<dyn StrategyMetrics>,
    pub resources: ResourceScopeHandle,
    pub activation: Arc<StrategyActivationGate>,
}

#[derive(Clone)]
pub struct StrategyExecutionBinding {
    pub local_account_no: u32,
    pub assets: BTreeMap<u64, u32>,
    pub handle: ExecutionHandle,
}

pub trait StrategyRuntimeFactory: Send + Sync {
    fn strategy_type(&self) -> &str;
    fn create(&self, definition: &StrategyDefinition, artifact: StrategyArtifact,
              context: StrategyRuntimeBuildContext) -> Result<Arc<dyn StrategyRuntime>, StrategyError>;
}

pub trait StrategyRuntime: EventHandler + Send + Sync {
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()>;
    fn seed_public_state(&self, seed: StrategyPublicStateSeedV13) -> LocalResult<()>;
    fn fire_timer(&self, timer_id: u64) -> LocalResult<()>;
    fn prepare(&self) -> LocalResult<StrategyOperationId>;
    fn start(&self) -> LocalResult<StrategyOperationId>;
    fn pause(&self, reason: PauseReason) -> LocalResult<StrategyOperationId>;
    fn resume(&self) -> LocalResult<StrategyOperationId>;
    fn invalidate(&self, reason: Arc<str>) -> LocalResult<StrategyOperationId>;
    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId>;
    fn freeze_state(&self, request: StrategyStateSnapshotRequest) -> LocalResult<StrategyOperationId>;
    fn state(&self) -> StrategyRuntimeStateSnapshot;
    fn health(&self) -> StrategyRuntimeHealthSnapshot;
    fn diagnostics(&self) -> StrategyRuntimeDiagnosticSnapshot;
    fn operation(&self, id: StrategyOperationId) -> StrategyOperationSnapshot;
}

#[derive(Default)]
pub struct StrategyRuntimeFactoryRegistry {
    factories: std::sync::RwLock<BTreeMap<Arc<str>, Arc<dyn StrategyRuntimeFactory>>>,
}
impl StrategyRuntimeFactoryRegistry {
    pub fn register(&self, factory: Arc<dyn StrategyRuntimeFactory>) -> LocalResult<()> {
        let key: Arc<str> = Arc::from(factory.strategy_type());
        let mut factories = self.factories.write().unwrap_or_else(|p| p.into_inner());
        if factories.insert(key, factory).is_some() {
            return Err(StrategyError::new(StrategyErrorKind::AlreadyExists, "register_runtime_factory",
                "strategy_type_conflict", "strategy runtime type is already registered"));
        }
        Ok(())
    }
    pub fn get(&self, strategy_type: &str) -> LocalResult<Arc<dyn StrategyRuntimeFactory>> {
        self.factories.read().unwrap_or_else(|p| p.into_inner()).get(strategy_type).cloned()
            .ok_or_else(|| StrategyError::new(StrategyErrorKind::LoadFailed, "create_runtime",
                "runtime_factory_not_registered", "strategy runtime factory is not registered"))
    }
}

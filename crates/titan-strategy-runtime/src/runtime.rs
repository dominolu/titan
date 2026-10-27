use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

use serde::{Deserialize, Serialize};
use titan_account_service::ExecutionHandle;
use titan_account_service::ObservedExecutionResult;
use titan_core_types::{EventHandler, ResourceScopeHandle};
use titan_event_engine::PrimaryAsyncLaneHandle;

use crate::*;

#[derive(Default)]
pub struct StrategyActivationGate(AtomicU8);
impl StrategyActivationGate {
    pub fn open(&self) -> bool {
        // 0 = closed, 1 = open, 2 = permanently invalidated for this run.
        matches!(
            self.0
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire),
            Ok(_) | Err(1)
        )
    }
    pub fn close(&self) {
        let _ = self
            .0
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire);
    }
    pub fn invalidate(&self) {
        self.0.store(2, Ordering::Release);
    }
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == 1
    }
}

#[cfg(test)]
mod activation_tests {
    use super::*;
    #[test]
    fn invalidation_cannot_be_undone_by_a_late_open_or_close() {
        let gate = Arc::new(StrategyActivationGate::default());
        assert!(gate.open());
        gate.close();
        assert!(!gate.is_open());
        assert!(gate.open());
        gate.invalidate();
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let gate = gate.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        gate.close();
                        assert!(!gate.open());
                        assert!(!gate.is_open());
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
    }
}

pub trait StrategyClock: Send + Sync {
    fn now_ns(&self) -> i64;
}
pub trait StrategyMetrics: Send + Sync {
    fn callback_duration(&self, _kind: V13EventKind, _duration_ns: u64) {}
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StrategyRecoveryPoint {
    pub checkpoint_id: u64,
    pub strategy: StrategyHandle,
    pub strategy_instance_id: u64,
    #[serde(alias = "strategy_id")]
    pub strategy_type: Arc<str>,
    #[serde(alias = "strategy_version")]
    pub source_version: Arc<str>,
    pub generation: u64,
    pub event_committed_sequence: u64,
    #[serde(alias = "artifact_digest")]
    pub compiled_strategy_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub abi_version: u32,
    pub state_schema_version: u32,
    pub state_schema_hash: [u8; 32],
    pub state_alignment: u32,
    pub state_bytes: Arc<[u8]>,
    #[serde(default)]
    pub orders_list: Arc<[StrategyOrderRecordV13]>,
    #[serde(default)]
    pub orders_list_checksum: [u8; 32],
    pub public_state_identity: [u8; 32],
    pub checksum: [u8; 32],
    #[serde(default)]
    pub recovery: Option<crate::RecoveryBaseline>,
}
impl StrategyRecoveryPoint {
    pub(crate) fn native_snapshot(&self) -> StrategyStateSnapshotV13 {
        StrategyStateSnapshotV13 {
            checkpoint_id: self.checkpoint_id,
            strategy_instance_id: self.strategy_instance_id,
            generation: self.generation,
            event_committed_sequence: self.event_committed_sequence,
            strategy_type: self.strategy_type.clone(),
            source_version: self.source_version.clone(),
            compiled_strategy_digest: self.compiled_strategy_digest,
            binding_digest: self.binding_digest,
            abi_version: self.abi_version,
            state_schema_version: self.state_schema_version,
            state_schema_hash: self.state_schema_hash,
            state_alignment: self.state_alignment,
            state_bytes: self.state_bytes.clone(),
            public_state_identity: self.public_state_identity,
            checksum: self.checksum,
        }
    }
}

/// Runtime-owned order lifecycle history for Strategy ABI V13. Each order ID owns one record
/// updated from confirmed events. Strategy-private Slot/role relationships remain in the
/// checkpointed typed state and are never duplicated here.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct StrategyOrderRecordV13 {
    pub order_id: u64,
    pub asset_no: u32,
    pub account_no: u32,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub cumulative_filled_lots: i64,
    pub last_fill_price_ticks: i64,
    pub submit_ts_ns: i64,
    pub last_fill_ts_ns: i64,
    pub last_event_ts_ns: i64,
    pub account_sequence: u64,
    pub fill_count: u64,
    pub side: u8,
    pub order_type: u8,
    pub time_in_force: u8,
    pub status: u8,
    pub reduce_only: u8,
    pub error_code: u8,
    pub position_side: u8,
    pub reserved: [u8; 1],
}

pub trait StrategyRecoveryStore: Send + Sync {
    fn latest(&self, _strategy: StrategyHandle) -> LocalResult<Option<StrategyRecoveryPoint>> {
        Ok(None)
    }
    fn submit(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()>;
    fn check_health(&self) -> LocalResult<()> {
        Ok(())
    }
    fn flush(&self, _deadline: Instant) -> LocalResult<()> {
        self.check_health()
    }
    fn enabled(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StrategyPublicStateSeedV13 {
    pub positions: Arc<[TitanPositionView]>,
    pub balances: Arc<[TitanBalanceView]>,
    pub accounts: Arc<[TitanAccountView]>,
    pub active_orders: Arc<[TitanActiveOrderView]>,
    /// Per-account committed version of the open-order projection. This lets the runtime
    /// distinguish an order absent from the snapshot from one created after the snapshot.
    pub order_boundaries: Arc<[(u32, u64)]>,
    #[serde(default)]
    pub recovery_orders: Arc<[TitanActiveOrderView]>,
}

pub struct StrategyRuntimeBuildContext {
    pub strategy: StrategyHandle,
    pub compiled_strategy_digest: CompiledStrategyDigest,
    pub markets: Arc<[ResolvedMarketBinding]>,
    pub accounts: Arc<[ResolvedAccountBinding]>,
    pub execution: Arc<[StrategyExecutionBinding]>,
    pub state_snapshot_sink: Arc<dyn StrategyRecoveryStore>,
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
    fn runtime_type(&self) -> &str;
    fn create(
        &self,
        definition: &StrategyDefinition,
        compiled_strategy: LoadedStrategy,
        context: StrategyRuntimeBuildContext,
    ) -> Result<Arc<dyn StrategyRuntime>, StrategyError>;
}

pub trait StrategyRuntime: EventHandler + Send + Sync {
    fn execution_risk(&self) -> LocalResult<Vec<titan_account_service::ExecutionRiskEvidence>> {
        Ok(Vec::new())
    }
    fn pause_supported(&self) -> bool {
        false
    }
    fn release_safe(&self) -> bool {
        false
    }
    fn recovery_status(&self) -> LocalResult<serde_json::Value> {
        Ok(serde_json::json!({"available":false,"reason":"recovery metadata unavailable"}))
    }
    fn business_observation(&self) -> LocalResult<BusinessObservationSnapshot> {
        Ok(BusinessObservationSnapshot::unavailable(
            "business observation unsupported",
        ))
    }
    fn attach_lane(&self, lane: PrimaryAsyncLaneHandle) -> LocalResult<()>;
    fn seed_public_state(&self, seed: StrategyPublicStateSeedV13) -> LocalResult<()>;
    fn restore_state(&self, snapshot: StrategyRecoveryPoint) -> LocalResult<()>;
    fn fire_timer(&self, timer_id: u64) -> LocalResult<()>;
    fn advance_lifecycle(&self) -> LocalResult<()> {
        Ok(())
    }
    fn execution_result(&self, result: ObservedExecutionResult) -> LocalResult<()>;
    fn prepare(&self) -> LocalResult<StrategyOperationId>;
    fn start(&self) -> LocalResult<StrategyOperationId>;
    fn pause(&self, reason: PauseReason) -> LocalResult<StrategyOperationId>;
    fn resume(&self) -> LocalResult<StrategyOperationId>;
    fn invalidate(&self, reason: Arc<str>) -> LocalResult<StrategyOperationId>;
    fn stop(&self, deadline: Instant) -> LocalResult<StrategyOperationId>;
    fn freeze_state(
        &self,
        request: StrategyStateSnapshotRequest,
    ) -> LocalResult<StrategyOperationId>;
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
        let key: Arc<str> = Arc::from(factory.runtime_type());
        let mut factories = self.factories.write().unwrap_or_else(|p| p.into_inner());
        if factories.insert(key, factory).is_some() {
            return Err(StrategyError::new(
                StrategyErrorKind::AlreadyExists,
                "register_runtime_factory",
                "strategy_type_conflict",
                "strategy runtime type is already registered",
            ));
        }
        Ok(())
    }
    pub fn get(&self, runtime_type: &str) -> LocalResult<Arc<dyn StrategyRuntimeFactory>> {
        self.factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(runtime_type)
            .cloned()
            .ok_or_else(|| {
                StrategyError::new(
                    StrategyErrorKind::LoadFailed,
                    "create_runtime",
                    "runtime_factory_not_registered",
                    "strategy runtime factory is not registered",
                )
            })
    }
}

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use titan_account_plugin::AccountHandle;
use titan_event_engine::SubscriberRuntimeMode;
use titan_market_plugin::MarketSourceHandle;
use titan_plugin_engine::{ApiVersion, EventQos};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct StrategyId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StrategyHandle {
    pub strategy_id: StrategyId,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StrategyArtifactId {
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct StrategyOperationId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyPackageRef {
    pub loader_type: Arc<str>,
    pub uri: Arc<str>,
    pub expected_digest: [u8; 32],
    pub signature_ref: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDataMode {
    Tick,
    Bar { timeframe_ns: i64 },
    Hybrid { signal_timeframe_ns: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyMarketBinding {
    pub local_market_no: u32,
    pub local_asset_no: u32,
    pub source_key: Arc<str>,
    pub asset_id: u32,
    pub data_mode: StrategyDataMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyTradableAsset {
    pub local_asset_no: u32,
    pub asset_id: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyAccountBinding {
    pub local_account_no: u32,
    pub account_key: Arc<str>,
    pub tradable_assets: Arc<[StrategyTradableAsset]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategySubscriptionSpec {
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub routing_keys: Arc<[u64]>,
    pub qos: EventQos,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct RiskScopeRef(pub Arc<str>);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallbackBudget {
    pub soft_budget: Duration,
    pub stall_threshold: Duration,
    pub max_consecutive_violations: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyRuntimeSpec {
    pub async_lane_capacity: usize,
    pub critical_reserve: usize,
    pub reliable_pending_capacity: usize,
    pub worker_policy: SubscriberRuntimeMode,
    pub command_capacity: usize,
    pub timer_capacity: usize,
    pub state_f64_capacity: usize,
    pub state_i64_capacity: usize,
    pub callback_budget: CallbackBudget,
    pub cpu_affinity: Option<usize>,
    pub startup_timeout: Duration,
    pub stop_timeout: Duration,
}

impl Default for StrategyRuntimeSpec {
    fn default() -> Self {
        Self {
            async_lane_capacity: 16_384,
            critical_reserve: 2_048,
            reliable_pending_capacity: 1_024,
            worker_policy: SubscriberRuntimeMode::SpinSleep,
            command_capacity: 64,
            timer_capacity: 64,
            state_f64_capacity: 1_024,
            state_i64_capacity: 1_024,
            callback_budget: CallbackBudget {
                soft_budget: Duration::from_micros(100),
                stall_threshold: Duration::from_millis(5),
                max_consecutive_violations: 3,
            },
            cpu_affinity: None,
            startup_timeout: Duration::from_secs(30),
            stop_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyRecoveryPolicy {
    Fresh,
    RestoreLatestCheckpoint,
    RequireCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyShutdownPolicy {
    LeaveOwnedOrders,
    CancelOwnedOrders,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyDefinition {
    pub strategy_key: Arc<str>,
    pub strategy_id: StrategyId,
    pub package: StrategyPackageRef,
    pub entrypoint: Arc<str>,
    pub parameters: Arc<[u8]>,
    pub parameter_schema_version: u32,
    pub markets: Arc<[StrategyMarketBinding]>,
    pub accounts: Arc<[StrategyAccountBinding]>,
    pub subscriptions: Arc<[StrategySubscriptionSpec]>,
    pub risk_scope: RiskScopeRef,
    pub runtime: StrategyRuntimeSpec,
    pub recovery: StrategyRecoveryPolicy,
    pub shutdown: StrategyShutdownPolicy,
    pub enabled: bool,
    pub definition_version: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct StrategyCallbackMask(pub u32);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct StrategyCapabilities(pub u64);

impl StrategyCapabilities {
    pub const READ_TICK: Self = Self(1 << 0);
    pub const READ_BAR: Self = Self(1 << 1);
    pub const READ_DEPTH: Self = Self(1 << 2);
    pub const READ_ACCOUNT: Self = Self(1 << 3);
    pub const READ_RISK: Self = Self(1 << 4);
    pub const SUBMIT_ORDER: Self = Self(1 << 5);
    pub const CANCEL_ORDER: Self = Self(1 << 6);
    pub const AMEND_ORDER: Self = Self(1 << 7);
    pub const SCHEDULE_TIMER: Self = Self(1 << 8);
    pub const CHECKPOINT_STATE: Self = Self(1 << 9);

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StrategyPackageManifest {
    pub strategy_type: Arc<str>,
    pub package_version: semver::Version,
    pub runtime_abi: ApiVersion,
    pub parameter_schema: Arc<serde_json::Value>,
    pub parameter_schema_version: u32,
    pub state_schema_version: u32,
    pub callbacks: StrategyCallbackMask,
    pub capabilities: StrategyCapabilities,
    pub artifact_digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct ResolvedMarketBinding {
    pub local_market_no: u32,
    pub local_asset_no: u32,
    pub source: MarketSourceHandle,
    pub asset_id: u32,
    pub data_mode: StrategyDataMode,
}

#[derive(Clone, Debug)]
pub struct ResolvedAccountBinding {
    pub local_account_no: u32,
    pub account: AccountHandle,
    pub tradable_assets: Arc<[StrategyTradableAsset]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PauseReason {
    User,
    CallbackBudget,
    DependencyUnavailable,
    SubscriberLagging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyLifecycle {
    Defined,
    Preparing,
    WaitingDependencies,
    Ready,
    Running,
    Pausing,
    Paused,
    Recovering,
    Invalidated,
    Quiescing,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyOperationState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug)]
pub struct StrategyOperationSnapshot {
    pub id: StrategyOperationId,
    pub strategy: Option<StrategyHandle>,
    pub state: StrategyOperationState,
    pub detail: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct StrategyRuntimeStateSnapshot {
    pub handle: StrategyHandle,
    pub lifecycle: StrategyLifecycle,
    pub command_gate_open: bool,
    pub activation_gate_open: bool,
}

#[derive(Clone, Debug)]
pub struct StrategyRuntimeHealthSnapshot {
    pub lifecycle: StrategyLifecycle,
    pub healthy: bool,
    pub degraded_reason: Option<Arc<str>>,
    pub callback_budget_violations: u64,
    pub heartbeat_at: SystemTime,
}

#[derive(Clone, Debug)]
pub struct StrategyFlightRecord {
    pub sequence: u64,
    pub observed_at: SystemTime,
    pub category: Arc<str>,
    pub detail: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct StrategyRuntimeDiagnosticSnapshot {
    pub summary: Arc<str>,
    pub callback_count: u64,
    pub command_count: u64,
    pub last_error_code: Option<Arc<str>>,
    pub lane_progress: titan_event_engine::LaneProgress,
    pub flight_records: Arc<[StrategyFlightRecord]>,
}

#[derive(Clone, Debug)]
pub struct StrategyInstanceSnapshot {
    pub handle: StrategyHandle,
    pub strategy_key: Arc<str>,
    pub definition_version: u64,
    pub artifact_id: StrategyArtifactId,
    pub lifecycle: StrategyLifecycle,
}

#[derive(Clone, Copy, Debug)]
pub struct StrategyStateSnapshotRequest {
    pub checkpoint_id: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StrategyEventHeaderV1 {
    pub strategy_id: u32,
    pub kind: u16,
    pub flags: u16,
    pub strategy_generation: u64,
    pub strategy_version: u64,
    pub occurred_at: i64,
    pub observed_at: i64,
    pub operation_id: u64,
}

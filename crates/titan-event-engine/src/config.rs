use serde::{Deserialize, Serialize};

use crate::ConfigError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolConfig {
    pub slots: usize,
    pub block_bytes: usize,
    pub low_watermark: usize,
}

impl PoolConfig {
    fn validate(&self, name: &'static str) -> Result<(), ConfigError> {
        if self.slots == 0 || self.block_bytes == 0 {
            return Err(ConfigError::InvalidPool {
                pool: name,
                reason: "slots and block_bytes must be positive",
            });
        }
        if self.low_watermark >= self.slots {
            return Err(ConfigError::InvalidPool {
                pool: name,
                reason: "low_watermark must be below slots",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ArenaConfig {
    pub small_event: PoolConfig,
    pub market_batch: PoolConfig,
    pub snapshot: PoolConfig,
}

impl Default for ArenaConfig {
    fn default() -> Self {
        Self {
            small_event: PoolConfig {
                slots: 32_768,
                block_bytes: 256,
                low_watermark: 4_096,
            },
            market_batch: PoolConfig {
                slots: 8_192,
                block_bytes: 16_384,
                low_watermark: 1_024,
            },
            snapshot: PoolConfig {
                slots: 512,
                block_bytes: 262_144,
                low_watermark: 64,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct IngressConfig {
    pub critical_capacity: usize,
    pub market_capacity: usize,
    pub max_sources: usize,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            critical_capacity: 8_192,
            market_capacity: 65_536,
            max_sources: 1_024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SubscriberConfig {
    pub max_count: usize,
    pub default_capacity: usize,
    pub critical_reserve: usize,
    pub lagging_high_watermark_ratio: f64,
    pub recovery_low_watermark_ratio: f64,
    pub runtime_mode: SubscriberRuntimeMode,
    pub spin_iterations: usize,
    pub idle_sleep_us: u64,
    /// CPU ids assigned to subscribers in subscription order, wrapping when necessary.
    pub cpu_affinity: Vec<usize>,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            max_count: 64,
            default_capacity: 16_384,
            critical_reserve: 2_048,
            lagging_high_watermark_ratio: 0.80,
            recovery_low_watermark_ratio: 0.50,
            runtime_mode: SubscriberRuntimeMode::SpinSleep,
            spin_iterations: 256,
            idle_sleep_us: 10,
            cpu_affinity: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberRuntimeMode {
    /// Spin briefly, then sleep for `idle_sleep_us` when the queue remains empty.
    #[default]
    SpinSleep,
    /// Park the consumer thread and let producers actively wake it.
    Park,
    /// Continuously poll the queue. This mode requires dedicated subscriber CPU affinity.
    Dedicated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingAllocation {
    Shared,
    Guaranteed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PendingDispatchConfig {
    pub per_subscriber_capacity: usize,
    pub global_capacity: usize,
    pub allocation: PendingAllocation,
    pub guaranteed_per_critical_subscriber: usize,
    pub max_age_ms: u64,
    pub high_watermark_ratio: f64,
}

impl Default for PendingDispatchConfig {
    fn default() -> Self {
        Self {
            per_subscriber_capacity: 1_024,
            global_capacity: 8_192,
            allocation: PendingAllocation::Shared,
            guaranteed_per_critical_subscriber: 128,
            max_age_ms: 100,
            high_watermark_ratio: 0.80,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DrainBudgetConfig {
    pub max_items: usize,
    pub max_elapsed_ns: u64,
}

impl DrainBudgetConfig {
    pub const fn new(max_items: usize, max_elapsed_ns: u64) -> Self {
        Self {
            max_items,
            max_elapsed_ns,
        }
    }

    fn validate(self, class: &'static str) -> Result<(), ConfigError> {
        if self.max_items == 0 || self.max_elapsed_ns == 0 {
            return Err(ConfigError::InvalidBudget(class));
        }
        Ok(())
    }
}

impl Default for DrainBudgetConfig {
    fn default() -> Self {
        Self::new(64, 10_000)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DispatchConfig {
    pub critical: DrainBudgetConfig,
    pub pending: DrainBudgetConfig,
    pub market: DrainBudgetConfig,
    pub timer: DrainBudgetConfig,
    pub max_fanout_per_step: usize,
    pub max_drain_once_ns: u64,
    pub timer_max_lateness_ns: u64,
    pub timer_capacity: usize,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            critical: DrainBudgetConfig::new(256, 20_000),
            pending: DrainBudgetConfig::new(64, 10_000),
            market: DrainBudgetConfig::new(256, 30_000),
            timer: DrainBudgetConfig::new(64, 10_000),
            max_fanout_per_step: 64,
            max_drain_once_ns: 100_000,
            timer_max_lateness_ns: 50_000,
            timer_capacity: 1_024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Dedicated,
    SpinSleep,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub mode: RuntimeMode,
    pub cpu_affinity: Option<usize>,
    pub spin_iterations: usize,
    pub sleep_us: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::SpinSleep,
            cpu_affinity: None,
            spin_iterations: 10_000,
            sleep_us: 10,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DiagnosticsConfig {
    pub pressure_scan_budget: usize,
    pub trace_ring_capacity: usize,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            pressure_scan_budget: 8,
            trace_ring_capacity: 4_096,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FaultSignalConfig {
    pub capacity: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotBarrierConfig {
    pub max_active: usize,
    pub per_barrier_staging_capacity: usize,
    pub global_staging_capacity: usize,
    pub timeout_ms: u64,
}

impl Default for SnapshotBarrierConfig {
    fn default() -> Self {
        Self {
            max_active: 16,
            per_barrier_staging_capacity: 8_192,
            global_staging_capacity: 65_536,
            timeout_ms: 5_000,
        }
    }
}

impl Default for FaultSignalConfig {
    fn default() -> Self {
        Self { capacity: 256 }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EventEngineConfig {
    pub arena: ArenaConfig,
    pub ingress: IngressConfig,
    pub subscribers: SubscriberConfig,
    pub pending_dispatch: PendingDispatchConfig,
    pub dispatch: DispatchConfig,
    pub runtime: RuntimeConfig,
    pub diagnostics: DiagnosticsConfig,
    pub fault_signal_ring: FaultSignalConfig,
    pub snapshot_barriers: SnapshotBarrierConfig,
}

impl EventEngineConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.arena.small_event.validate("small_event")?;
        self.arena.market_batch.validate("market_batch")?;
        self.arena.snapshot.validate("snapshot")?;
        if self.ingress.critical_capacity == 0
            || self.ingress.market_capacity == 0
            || self.ingress.max_sources == 0
        {
            return Err(ConfigError::ZeroCapacity("ingress"));
        }
        if self.subscribers.max_count == 0 || self.subscribers.default_capacity == 0 {
            return Err(ConfigError::ZeroCapacity("subscribers"));
        }
        if self.subscribers.idle_sleep_us == 0 {
            return Err(ConfigError::ZeroDuration("subscriber idle_sleep_us"));
        }
        if self.subscribers.runtime_mode == SubscriberRuntimeMode::Dedicated
            && self.subscribers.cpu_affinity.is_empty()
        {
            return Err(ConfigError::DedicatedSubscriberAffinity);
        }
        if self.subscribers.critical_reserve >= self.subscribers.default_capacity {
            return Err(ConfigError::CriticalReserve);
        }
        validate_ratio(
            self.subscribers.lagging_high_watermark_ratio,
            "lagging_high_watermark_ratio",
        )?;
        validate_ratio(
            self.subscribers.recovery_low_watermark_ratio,
            "recovery_low_watermark_ratio",
        )?;
        if self.subscribers.recovery_low_watermark_ratio
            >= self.subscribers.lagging_high_watermark_ratio
        {
            return Err(ConfigError::WatermarkOrder);
        }
        let pending = &self.pending_dispatch;
        if pending.per_subscriber_capacity == 0
            || pending.global_capacity == 0
            || pending.guaranteed_per_critical_subscriber > pending.per_subscriber_capacity
            || pending.guaranteed_per_critical_subscriber > pending.global_capacity
        {
            return Err(ConfigError::PendingCapacity);
        }
        validate_ratio(pending.high_watermark_ratio, "pending_high_watermark_ratio")?;
        if pending.max_age_ms == 0 {
            return Err(ConfigError::ZeroDuration("pending.max_age_ms"));
        }
        self.dispatch.critical.validate("critical")?;
        self.dispatch.pending.validate("pending")?;
        self.dispatch.market.validate("market")?;
        self.dispatch.timer.validate("timer")?;
        if self.dispatch.max_fanout_per_step == 0
            || self.dispatch.max_drain_once_ns == 0
            || self.dispatch.timer_capacity == 0
        {
            return Err(ConfigError::InvalidBudget("dispatch"));
        }
        if self.diagnostics.pressure_scan_budget == 0
            || self.diagnostics.trace_ring_capacity == 0
            || self.fault_signal_ring.capacity == 0
        {
            return Err(ConfigError::ZeroCapacity("diagnostics"));
        }
        if self.snapshot_barriers.max_active == 0
            || self.snapshot_barriers.per_barrier_staging_capacity == 0
            || self.snapshot_barriers.global_staging_capacity == 0
            || self.snapshot_barriers.per_barrier_staging_capacity
                > self.snapshot_barriers.global_staging_capacity
        {
            return Err(ConfigError::SnapshotBarrierCapacity);
        }
        if self.snapshot_barriers.timeout_ms == 0 {
            return Err(ConfigError::ZeroDuration("snapshot_barriers.timeout_ms"));
        }
        if self.runtime.mode == RuntimeMode::Dedicated && self.runtime.cpu_affinity.is_none() {
            return Err(ConfigError::DedicatedAffinity);
        }
        let available = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|core| core.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut assigned = std::collections::BTreeSet::new();
        for core in self
            .runtime
            .cpu_affinity
            .iter()
            .copied()
            .chain(self.subscribers.cpu_affinity.iter().copied())
        {
            if !available.contains(&core) {
                return Err(ConfigError::CpuAffinityUnavailable(core));
            }
            if !assigned.insert(core) {
                return Err(ConfigError::CpuAffinityConflict(core));
            }
        }
        Ok(())
    }
}

fn validate_ratio(value: f64, name: &'static str) -> Result<(), ConfigError> {
    if !value.is_finite() || !(0.0..1.0).contains(&value) {
        return Err(ConfigError::InvalidRatio(name));
    }
    Ok(())
}

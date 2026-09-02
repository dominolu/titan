use thiserror::Error;

use crate::PoolKind;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("{0} capacity must be positive")]
    ZeroCapacity(&'static str),
    #[error("{0} duration must be positive")]
    ZeroDuration(&'static str),
    #[error("invalid {pool} pool: {reason}")]
    InvalidPool {
        pool: &'static str,
        reason: &'static str,
    },
    #[error("critical_reserve must be below subscriber capacity")]
    CriticalReserve,
    #[error("invalid pending dispatch capacities")]
    PendingCapacity,
    #[error("invalid ratio {0}; expected 0 <= value < 1")]
    InvalidRatio(&'static str),
    #[error("recovery low watermark must be below lagging high watermark")]
    WatermarkOrder,
    #[error("invalid drain budget for {0}")]
    InvalidBudget(&'static str),
    #[error("dedicated mode requires cpu_affinity")]
    DedicatedAffinity,
    #[error("dedicated subscriber mode requires at least one subscriber cpu_affinity")]
    DedicatedSubscriberAffinity,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PublishError {
    #[error("event type or schema is not registered")]
    InvalidEvent,
    #[error("payload length {length} exceeds {pool:?} block size {capacity}")]
    PayloadTooLarge {
        pool: PoolKind,
        length: usize,
        capacity: usize,
    },
    #[error("{0:?} event pool is exhausted")]
    EventArenaExhausted(PoolKind),
    #[error("critical ingress is full")]
    CriticalIngressFull,
    #[error("market ingress is full")]
    MarketIngressFull,
    #[error("event engine is stopped")]
    Stopped,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("event engine already started")]
    AlreadyStarted,
    #[error("event engine is not running")]
    NotRunning,
    #[error("event engine control queue is full")]
    ControlQueueFull,
    #[error("event engine control response timed out")]
    ControlTimeout,
    #[error("route transaction {0} does not exist")]
    UnknownTransaction(u64),
    #[error("route transaction base version is stale")]
    StaleRouteVersion,
    #[error("maximum subscriber count reached")]
    SubscriberLimit,
    #[error("subscription capacity or pending guarantee is invalid")]
    InvalidSubscriptionCapacity,
    #[error("subscription {0} does not exist")]
    UnknownSubscription(u64),
    #[error("subscription {0} still has an in-flight handler")]
    RecoveryNotQuiescent(u64),
    #[error("event type or schema is not registered")]
    InvalidEvent,
    #[error("invalid asynchronous FastLane configuration")]
    InvalidFastLaneConfig,
    #[error("invalid PRIMARY asynchronous lane configuration")]
    InvalidPrimaryLaneConfig,
    #[error("safe-point action panicked")]
    SafePointPanicked,
    #[error("snapshot barrier request is invalid")]
    InvalidSnapshotBarrier,
    #[error("a snapshot barrier is already active")]
    SnapshotBarrierActive,
    #[error("snapshot barrier {0} does not exist or is in the wrong state")]
    UnknownSnapshotBarrier(u64),
    #[error("snapshot staging capacity is exhausted")]
    SnapshotStagingFull,
    #[error("snapshot completion did not provide every stream boundary")]
    SnapshotBoundaryMissing,
    #[error("snapshot replay has not reached the committed watermark")]
    SnapshotReplayNotCommitted,
    #[error("subscriber runtime failed: {0}")]
    SubscriberRuntime(String),
    #[error("PRIMARY lane {0} did not stop before its deadline")]
    PrimaryLaneStopTimeout(u64),
    #[error("event arena still has {0} outstanding blocks")]
    OutstandingBlocks(usize),
    #[error("timer queue is full")]
    TimerQueueFull,
    #[error("event loop terminated unexpectedly")]
    EventLoopFailed,
}

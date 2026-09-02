use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant, SystemTime},
};

use sha2::{Digest, Sha256};
use titan_account_plugin::Id128;
use titan_event_engine::SnapshotBarrierRequest;
use titan_event_engine::{PrimaryAsyncLaneHandle, StreamBoundary};

use crate::*;

#[derive(Clone, Debug)]
pub struct StrategyCheckpoint {
    pub strategy: StrategyHandle,
    pub artifact_digest: [u8; 32],
    pub entrypoint: Arc<str>,
    pub normalized_parameters_digest: [u8; 32],
    pub state_schema_version: u32,
    pub state_f64: Arc<[f64]>,
    pub state_i64: Arc<[i64]>,
    pub owned_orders: Arc<[StrategyOwnedOrder]>,
    pub pending_command_ids: Arc<[Id128]>,
    pub subscription_generation: u64,
    pub committed_lane_sequence: u64,
    pub stream_boundaries: Arc<[StreamBoundary]>,
    pub created_at: SystemTime,
    pub checksum: [u8; 32],
}

impl StrategyCheckpoint {
    pub fn verify(&self) -> bool {
        self.checksum == checkpoint_checksum(self)
    }
}

pub trait StoreService: Send + Sync {
    fn persist(&self, checkpoint: StrategyCheckpoint) -> LocalResult<()>;
    fn load_latest(&self, strategy_id: StrategyId) -> LocalResult<Option<StrategyCheckpoint>>;
}

pub trait StreamBoundaryProvider: Send + Sync {
    fn committed_boundaries(&self, strategy: StrategyHandle) -> LocalResult<Arc<[StreamBoundary]>>;
}

/// Provider-facing recovery orchestration. Implementations must install the EventEngine barrier
/// before requesting market/account snapshots and complete it only after the runtime-applied
/// replay watermark is committed.
pub trait StrategyRecoveryCoordinator: Send + Sync {
    fn synchronize(
        &self,
        strategy: StrategyHandle,
        definition: &StrategyDefinition,
        lane: &PrimaryAsyncLaneHandle,
        deadline: Instant,
    ) -> LocalResult<()>;
}

pub trait StrategySnapshotProvider: Send + Sync {
    fn source_ids(&self, definition: &StrategyDefinition) -> LocalResult<Arc<[u32]>>;
    fn publish_snapshots(
        &self,
        barrier: titan_event_engine::SnapshotBarrierId,
        strategy: StrategyHandle,
        definition: &StrategyDefinition,
        lane: &PrimaryAsyncLaneHandle,
        deadline: Instant,
    ) -> LocalResult<Arc<[StreamBoundary]>>;
}

pub struct EventEngineRecoveryCoordinator {
    provider: Arc<dyn StrategySnapshotProvider>,
}

impl EventEngineRecoveryCoordinator {
    pub fn new(provider: Arc<dyn StrategySnapshotProvider>) -> Self {
        Self { provider }
    }
}

impl StrategyRecoveryCoordinator for EventEngineRecoveryCoordinator {
    fn synchronize(
        &self,
        strategy: StrategyHandle,
        definition: &StrategyDefinition,
        lane: &PrimaryAsyncLaneHandle,
        deadline: Instant,
    ) -> LocalResult<()> {
        let source_ids = self.provider.source_ids(definition)?;
        let barrier = lane
            .begin_snapshot_barrier(SnapshotBarrierRequest {
                source_ids,
                deadline,
            })
            .map_err(|_| recovery_error("barrier_begin_failed"))?;
        let result = (|| {
            let boundaries = self
                .provider
                .publish_snapshots(barrier, strategy, definition, lane, deadline)?;
            let replay = lane
                .snapshot_provider_complete(barrier, &boundaries)
                .map_err(|_| recovery_error("provider_completion_failed"))?;
            while lane.progress().committed_sequence < replay {
                if Instant::now() >= deadline {
                    return Err(recovery_error("replay_timeout"));
                }
                std::thread::yield_now();
            }
            lane.complete_snapshot_barrier(barrier, lane.progress().committed_sequence)
                .map_err(|_| recovery_error("barrier_commit_failed"))
        })();
        if result.is_err() {
            let _ = lane.abort_snapshot_barrier(barrier);
        }
        result
    }
}

#[derive(Default)]
pub struct InMemoryCheckpointStore {
    values: Mutex<BTreeMap<StrategyId, StrategyCheckpoint>>,
}

impl StoreService for InMemoryCheckpointStore {
    fn persist(&self, checkpoint: StrategyCheckpoint) -> LocalResult<()> {
        if !checkpoint.verify() {
            return Err(checkpoint_error("checksum_invalid"));
        }
        self.values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(checkpoint.strategy.strategy_id, checkpoint);
        Ok(())
    }

    fn load_latest(&self, strategy_id: StrategyId) -> LocalResult<Option<StrategyCheckpoint>> {
        Ok(self
            .values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&strategy_id)
            .cloned())
    }
}

#[derive(Default)]
pub struct SnapshotCollector {
    values: Mutex<BTreeMap<u64, StrategyPrivateStateSnapshot>>,
    changed: Condvar,
}

impl StrategyStateSnapshotSink for SnapshotCollector {
    fn submit(&self, snapshot: StrategyPrivateStateSnapshot) -> LocalResult<()> {
        self.values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(snapshot.checkpoint_id, snapshot);
        self.changed.notify_all();
        Ok(())
    }
}

impl SnapshotCollector {
    fn wait(&self, id: u64, deadline: Instant) -> LocalResult<StrategyPrivateStateSnapshot> {
        let mut values = self.values.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if let Some(snapshot) = values.remove(&id) {
                return Ok(snapshot);
            }
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(checkpoint_error("snapshot_timeout"));
            }
            let (next, result) = self
                .changed
                .wait_timeout(values, timeout)
                .unwrap_or_else(|p| p.into_inner());
            values = next;
            if result.timed_out() {
                return Err(checkpoint_error("snapshot_timeout"));
            }
        }
    }
}

pub struct CheckpointCoordinator {
    store: Option<Arc<dyn StoreService>>,
    boundaries: Arc<dyn StreamBoundaryProvider>,
    sink: Arc<SnapshotCollector>,
    next_checkpoint: std::sync::atomic::AtomicU64,
}

impl CheckpointCoordinator {
    pub fn new(
        store: Option<Arc<dyn StoreService>>,
        boundaries: Arc<dyn StreamBoundaryProvider>,
        sink: Arc<SnapshotCollector>,
    ) -> Self {
        Self {
            store,
            boundaries,
            sink,
            next_checkpoint: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn snapshot_sink(&self) -> Arc<SnapshotCollector> {
        self.sink.clone()
    }

    pub fn checkpoint(
        &self,
        runtime: &Arc<dyn StrategyRuntime>,
        lane: &PrimaryAsyncLaneHandle,
        gateway: &Arc<dyn StrategyCommandGateway>,
        definition: &StrategyDefinition,
        manifest: &StrategyPackageManifest,
        deadline: Instant,
    ) -> LocalResult<StrategyCheckpoint> {
        let id = self
            .next_checkpoint
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let operation = runtime.freeze_state(StrategyStateSnapshotRequest { checkpoint_id: id })?;
        loop {
            let state = runtime.operation(operation);
            if state.state == StrategyOperationState::Failed {
                return Err(checkpoint_error("runtime_freeze_failed"));
            }
            if state.state == StrategyOperationState::Succeeded {
                break;
            }
            if Instant::now() >= deadline {
                return Err(checkpoint_error("runtime_freeze_timeout"));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let private = self.sink.wait(id, deadline)?;
        let command = gateway.metadata(private.strategy);
        let mut checkpoint = StrategyCheckpoint {
            strategy: private.strategy,
            artifact_digest: definition.package.expected_digest,
            entrypoint: definition.entrypoint.clone(),
            normalized_parameters_digest: normalized_parameters_digest(&definition.parameters)?,
            state_schema_version: manifest.state_schema_version,
            state_f64: private.state_f64,
            state_i64: private.state_i64,
            owned_orders: command.owned_orders,
            pending_command_ids: command.pending_command_ids,
            subscription_generation: private.strategy.generation,
            committed_lane_sequence: lane.progress().committed_sequence,
            stream_boundaries: self.boundaries.committed_boundaries(private.strategy)?,
            created_at: SystemTime::now(),
            checksum: [0; 32],
        };
        checkpoint.checksum = checkpoint_checksum(&checkpoint);
        self.store
            .as_ref()
            .ok_or_else(|| checkpoint_error("store_unavailable"))?
            .persist(checkpoint.clone())?;
        Ok(checkpoint)
    }

    pub fn restore(
        &self,
        definition: &StrategyDefinition,
        manifest: &StrategyPackageManifest,
    ) -> LocalResult<Option<StrategyCheckpoint>> {
        if definition.recovery == StrategyRecoveryPolicy::Fresh {
            return Ok(None);
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| checkpoint_error("store_unavailable"))?;
        let value = store.load_latest(definition.strategy_id)?;
        let Some(checkpoint) = value else {
            return if definition.recovery == StrategyRecoveryPolicy::RequireCheckpoint {
                Err(checkpoint_error("checkpoint_required"))
            } else {
                Ok(None)
            };
        };
        if !checkpoint.verify()
            || checkpoint.artifact_digest != definition.package.expected_digest
            || checkpoint.entrypoint != definition.entrypoint
            || checkpoint.normalized_parameters_digest
                != normalized_parameters_digest(&definition.parameters)?
            || checkpoint.state_schema_version != manifest.state_schema_version
        {
            return Err(checkpoint_error("checkpoint_incompatible"));
        }
        Ok(Some(checkpoint))
    }
}

fn checkpoint_checksum(checkpoint: &StrategyCheckpoint) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(checkpoint.strategy.strategy_id.0.to_le_bytes());
    digest.update(checkpoint.strategy.generation.to_le_bytes());
    digest.update(checkpoint.artifact_digest);
    digest.update(checkpoint.entrypoint.as_bytes());
    digest.update(checkpoint.normalized_parameters_digest);
    digest.update(checkpoint.state_schema_version.to_le_bytes());
    for value in checkpoint.state_f64.iter() {
        digest.update(value.to_bits().to_le_bytes());
    }
    for value in checkpoint.state_i64.iter() {
        digest.update(value.to_le_bytes());
    }
    for order in checkpoint.owned_orders.iter() {
        digest.update(order.client_order_id.0);
        digest.update(order.local_account_no.to_le_bytes());
        digest.update(order.local_asset_no.to_le_bytes());
        digest.update(order.account.account_id.0.to_le_bytes());
        digest.update(order.account.generation.to_le_bytes());
        digest.update(order.asset_id.to_le_bytes());
    }
    for command_id in checkpoint.pending_command_ids.iter() {
        digest.update(command_id.0);
    }
    digest.update(checkpoint.committed_lane_sequence.to_le_bytes());
    for boundary in checkpoint.stream_boundaries.iter() {
        digest.update(boundary.source_id.to_le_bytes());
        digest.update(boundary.stream_epoch.to_le_bytes());
        digest.update(boundary.source_sequence.to_le_bytes());
    }
    let created_at = checkpoint
        .created_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    digest.update(created_at.as_secs().to_le_bytes());
    digest.update(created_at.subsec_nanos().to_le_bytes());
    digest.finalize().into()
}

fn checkpoint_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::CheckpointFailed,
        "checkpoint",
        code,
        "strategy checkpoint operation failed",
    )
}

fn recovery_error(code: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::SubscriberResyncRequired,
        "snapshot_recovery",
        code,
        "strategy snapshot recovery failed",
    )
}

use std::collections::{BTreeMap, BTreeSet};

use super::{
    execution::{ExecutionCommand, InstrumentId, VenueId},
    scheduler::EventKey,
};
use crate::types::Status;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSourceManifest {
    pub source_id: u32,
    pub uri: String,
    pub content_hash: u64,
    pub source_priority: u16,
    pub data_kind: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataManifest {
    pub sources: Vec<DataSourceManifest>,
}

#[derive(Default)]
pub struct DataCatalog {
    manifests: BTreeMap<String, (u64, DataManifest)>,
}

impl DataCatalog {
    pub fn register(
        &mut self,
        name: impl Into<String>,
        manifest: DataManifest,
    ) -> Result<u64, PlatformError> {
        let name = name.into();
        if name.is_empty() || self.manifests.contains_key(&name) {
            return Err(PlatformError::DuplicateCatalogEntry);
        }
        let hash = manifest.validate_and_hash()?;
        self.manifests.insert(name, (hash, manifest));
        Ok(hash)
    }

    pub fn resolve(&self, name: &str) -> Option<(u64, &DataManifest)> {
        self.manifests
            .get(name)
            .map(|(hash, manifest)| (*hash, manifest))
    }
}

impl DataManifest {
    pub fn validate_and_hash(&self) -> Result<u64, PlatformError> {
        let mut ids = BTreeSet::new();
        let mut hash = 0xcbf29ce484222325_u64;
        let mut sources: Vec<_> = self.sources.iter().collect();
        sources.sort_by_key(|source| (source.source_priority, source.source_id));
        for source in sources {
            if !ids.insert(source.source_id) || source.uri.is_empty() || source.data_kind.is_empty()
            {
                return Err(PlatformError::InvalidManifest);
            }
            for byte in source
                .source_id
                .to_le_bytes()
                .into_iter()
                .chain(source.content_hash.to_le_bytes())
                .chain(source.source_priority.to_le_bytes())
                .chain(source.uri.bytes())
                .chain(source.data_kind.bytes())
            {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        }
        Ok(hash)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunConfig {
    pub run_id: u64,
    pub strategy_id: String,
    pub config_hash: u64,
    pub random_seed: u64,
}

impl RunConfig {
    /// Hash actually persisted to reproducibility metadata. Seed and strategy identity are always
    /// included, so callers cannot accidentally reuse a config hash after changing either.
    pub fn effective_config_hash(&self) -> u64 {
        let mut hash = self.config_hash ^ 0xcbf29ce484222325;
        for byte in self
            .random_seed
            .to_le_bytes()
            .into_iter()
            .chain(self.strategy_id.bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    pub fn normalized(&self) -> Self {
        Self {
            config_hash: self.effective_config_hash(),
            ..self.clone()
        }
    }
}

pub trait BatchTask {
    type Output;
    type Error;
    fn execute(&mut self, config: &RunConfig) -> Result<Self::Output, Self::Error>;
    fn reset(&mut self) -> Result<(), Self::Error>;
}

pub struct BatchNode<T> {
    task: T,
}

impl<T: BatchTask> BatchNode<T> {
    pub fn new(task: T) -> Self {
        Self { task }
    }

    pub fn run_all(&mut self, configs: &[RunConfig]) -> Result<Vec<T::Output>, T::Error> {
        let mut outputs = Vec::with_capacity(configs.len());
        for (index, config) in configs.iter().enumerate() {
            if index > 0 {
                self.task.reset()?;
            }
            let normalized = config.normalized();
            outputs.push(self.task.execute(&normalized)?);
        }
        Ok(outputs)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CustomDataEnvelope<T> {
    pub key: EventKey,
    pub schema_id: u32,
    pub payload: T,
}

pub trait SimulationHook {
    fn on_event(&mut self, key: EventKey, commands: &mut Vec<ExecutionCommand>);
    fn reset(&mut self);
}

pub trait ExecutionAlgorithm {
    fn on_event(&mut self, key: EventKey, out: &mut Vec<ExecutionCommand>);
    fn reset(&mut self);
}

/// Shared bounded command channel for strategies, execution algorithms and simulation hooks.
/// Every producer emits the same canonical ExecutionCommand and therefore enters the same risk,
/// transport and matching path.
pub struct PlatformCommandBus {
    capacity: usize,
    commands: Vec<ExecutionCommand>,
}

/// Mode-independent owner for platform command producers.
///
/// Algorithms and simulation hooks only emit canonical [`ExecutionCommand`] values. The
/// runtime which owns this component remains responsible for decoding those commands and
/// routing them through its normal risk, transport and matching path.
pub struct PlatformCommandProducers {
    bus: PlatformCommandBus,
    algorithms: Vec<Box<dyn ExecutionAlgorithm>>,
    hooks: Vec<Box<dyn SimulationHook>>,
    generated: Vec<ExecutionCommand>,
}

impl PlatformCommandProducers {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bus: PlatformCommandBus::with_capacity(capacity),
            algorithms: Vec::new(),
            hooks: Vec::new(),
            generated: Vec::with_capacity(capacity),
        }
    }

    pub fn add_algorithm<A: ExecutionAlgorithm + 'static>(&mut self, algorithm: A) {
        self.algorithms.push(Box::new(algorithm));
    }

    pub fn add_hook<H: SimulationHook + 'static>(&mut self, hook: H) {
        self.hooks.push(Box::new(hook));
    }

    pub fn collect(
        &mut self,
        key: EventKey,
        output: &mut Vec<ExecutionCommand>,
    ) -> Result<(), PlatformError> {
        self.generated.clear();
        for algorithm in &mut self.algorithms {
            algorithm.on_event(key, &mut self.generated);
        }
        for hook in &mut self.hooks {
            hook.on_event(key, &mut self.generated);
        }
        for command in self.generated.drain(..) {
            self.bus.push(command)?;
        }
        output.extend(self.bus.drain());
        Ok(())
    }

    pub fn reset(&mut self) {
        self.bus.reset();
        self.generated.clear();
        for algorithm in &mut self.algorithms {
            algorithm.reset();
        }
        for hook in &mut self.hooks {
            hook.reset();
        }
    }
}

impl PlatformCommandBus {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            commands: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, command: ExecutionCommand) -> Result<(), PlatformError> {
        if self.commands.len() == self.capacity {
            return Err(PlatformError::CommandCapacity);
        }
        self.commands.push(command);
        Ok(())
    }

    pub fn run_algorithm<A: ExecutionAlgorithm>(
        &mut self,
        algorithm: &mut A,
        key: EventKey,
    ) -> Result<(), PlatformError> {
        let before = self.commands.len();
        algorithm.on_event(key, &mut self.commands);
        if self.commands.len() > self.capacity {
            self.commands.truncate(before);
            return Err(PlatformError::CommandCapacity);
        }
        Ok(())
    }

    pub fn run_hook<H: SimulationHook>(
        &mut self,
        hook: &mut H,
        key: EventKey,
    ) -> Result<(), PlatformError> {
        let before = self.commands.len();
        hook.on_event(key, &mut self.commands);
        if self.commands.len() > self.capacity {
            self.commands.truncate(before);
            return Err(PlatformError::CommandCapacity);
        }
        Ok(())
    }

    pub fn drain(&mut self) -> impl Iterator<Item = ExecutionCommand> + '_ {
        self.commands.drain(..)
    }

    pub fn reset(&mut self) {
        self.commands.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContingencyKind {
    Oco,
    Oto,
    Bracket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContingencyGroup {
    pub group_id: u64,
    pub kind: ContingencyKind,
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub parent: Option<u64>,
    pub children: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContingencyAction {
    Activate(u64),
    Cancel(u64),
}

#[derive(Default)]
pub struct ContingencyManager {
    groups: BTreeMap<u64, ContingencyGroup>,
    order_groups: BTreeMap<u64, u64>,
    activated_parents: BTreeSet<u64>,
    triggered_groups: BTreeSet<u64>,
    closed_groups: BTreeSet<u64>,
}

impl ContingencyManager {
    pub fn insert(&mut self, group: ContingencyGroup) -> bool {
        if group.children.is_empty()
            || self.groups.contains_key(&group.group_id)
            || group
                .parent
                .is_some_and(|parent| group.children.contains(&parent))
            || group
                .parent
                .into_iter()
                .chain(group.children.iter().copied())
                .any(|order_id| self.order_groups.contains_key(&order_id))
        {
            return false;
        }
        if let Some(parent) = group.parent {
            self.order_groups.insert(parent, group.group_id);
        }
        for child in &group.children {
            self.order_groups.insert(*child, group.group_id);
        }
        self.groups.insert(group.group_id, group);
        true
    }

    pub fn should_hold(&self, order_id: u64) -> bool {
        let Some(group_id) = self.order_groups.get(&order_id) else {
            return false;
        };
        let group = &self.groups[group_id];
        group.children.contains(&order_id)
            && group.parent.is_some()
            && matches!(group.kind, ContingencyKind::Oto | ContingencyKind::Bracket)
            && !self.activated_parents.contains(group_id)
            && !self.closed_groups.contains(group_id)
    }

    pub fn should_reject(&self, order_id: u64) -> bool {
        self.order_groups
            .get(&order_id)
            .is_some_and(|group_id| self.closed_groups.contains(group_id))
    }

    pub fn on_report(&mut self, order_id: u64, status: Status, out: &mut Vec<ContingencyAction>) {
        let Some(group_id) = self.order_groups.get(&order_id).copied() else {
            return;
        };
        let Some(group) = self.groups.get(&group_id) else {
            return;
        };
        if group.parent == Some(order_id) {
            if status == Status::Filled && self.activated_parents.insert(group_id) {
                out.extend(
                    group
                        .children
                        .iter()
                        .copied()
                        .map(ContingencyAction::Activate),
                );
            } else if matches!(
                status,
                Status::Rejected | Status::Canceled | Status::Expired
            ) && self.closed_groups.insert(group_id)
            {
                out.extend(
                    group
                        .children
                        .iter()
                        .copied()
                        .map(ContingencyAction::Cancel),
                );
            }
        } else if group.children.contains(&order_id)
            && matches!(group.kind, ContingencyKind::Oco | ContingencyKind::Bracket)
            && matches!(status, Status::PartiallyFilled | Status::Filled)
            && self.triggered_groups.insert(group_id)
        {
            self.closed_groups.insert(group_id);
            out.extend(
                group
                    .children
                    .iter()
                    .copied()
                    .filter(|id| *id != order_id)
                    .map(ContingencyAction::Cancel),
            );
        }
    }

    pub fn reset(&mut self) {
        self.activated_parents.clear();
        self.triggered_groups.clear();
        self.closed_groups.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PlatformError {
    #[error("invalid data manifest")]
    InvalidManifest,
    #[error("catalog entry already exists or has an empty name")]
    DuplicateCatalogEntry,
    #[error("platform command buffer capacity exceeded")]
    CommandCapacity,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backtest::execution::{ExecutionOrderRequest, OrderOrigin},
        types::{OrdType, Side, TimeInForce},
    };

    struct OneShotAlgorithm;

    #[derive(Default)]
    struct SeedTask {
        previous_seed: u64,
    }

    impl BatchTask for SeedTask {
        type Output = u64;
        type Error = &'static str;

        fn execute(&mut self, config: &RunConfig) -> Result<Self::Output, Self::Error> {
            if self.previous_seed != 0 {
                return Err("batch task was not reset");
            }
            self.previous_seed = config.random_seed;
            Ok(config.random_seed)
        }

        fn reset(&mut self) -> Result<(), Self::Error> {
            self.previous_seed = 0;
            Ok(())
        }
    }

    #[derive(Default)]
    struct CancelHook;

    impl SimulationHook for CancelHook {
        fn on_event(&mut self, key: EventKey, out: &mut Vec<ExecutionCommand>) {
            out.push(ExecutionCommand::Cancel(
                super::super::execution::CancelRequest {
                    client_order_id: 9,
                    venue_id: VenueId(key.venue_no),
                    instrument_id: InstrumentId(key.asset_no),
                    local_submit_ts: key.timestamp,
                },
            ));
        }

        fn reset(&mut self) {}
    }

    impl ExecutionAlgorithm for OneShotAlgorithm {
        fn on_event(&mut self, _key: EventKey, out: &mut Vec<ExecutionCommand>) {
            out.push(ExecutionCommand::Submit(ExecutionOrderRequest {
                client_order_id: 9,
                venue_id: VenueId(1),
                instrument_id: InstrumentId(1),
                price: 10.0,
                qty: 1.0,
                side: Side::Buy,
                time_in_force: TimeInForce::GTC,
                order_type: OrdType::Limit,
                reduce_only: false,
                origin: OrderOrigin::ExecutionAlgorithm,
                local_submit_ts: 1,
            }));
        }

        fn reset(&mut self) {}
    }

    #[test]
    fn manifest_hash_changes_with_source_and_bracket_actions_are_deterministic() {
        let mut manifest = DataManifest {
            sources: vec![
                DataSourceManifest {
                    source_id: 1,
                    uri: "bars.npy".into(),
                    content_hash: 7,
                    source_priority: 0,
                    data_kind: "bar".into(),
                },
                DataSourceManifest {
                    source_id: 2,
                    uri: "ticks.npy".into(),
                    content_hash: 9,
                    source_priority: 1,
                    data_kind: "tick".into(),
                },
            ],
        };
        let first = manifest.validate_and_hash().unwrap();
        manifest.sources.reverse();
        assert_eq!(manifest.validate_and_hash().unwrap(), first);
        manifest.sources.reverse();
        manifest.sources[0].content_hash = 8;
        assert_ne!(manifest.validate_and_hash().unwrap(), first);

        let mut manager = ContingencyManager::default();
        manager.insert(ContingencyGroup {
            group_id: 1,
            kind: ContingencyKind::Bracket,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(1),
            parent: Some(10),
            children: vec![11, 12],
        });
        let mut actions = Vec::new();
        manager.on_report(10, Status::PartiallyFilled, &mut actions);
        assert!(actions.is_empty());
        assert!(manager.should_hold(11));
        manager.on_report(10, Status::Filled, &mut actions);
        assert_eq!(
            actions,
            [
                ContingencyAction::Activate(11),
                ContingencyAction::Activate(12)
            ]
        );
        actions.clear();
        manager.on_report(11, Status::Filled, &mut actions);
        assert_eq!(actions, [ContingencyAction::Cancel(12)]);

        let mut rejected = ContingencyManager::default();
        assert!(rejected.insert(ContingencyGroup {
            group_id: 2,
            kind: ContingencyKind::Bracket,
            venue_id: VenueId(1),
            instrument_id: InstrumentId(1),
            parent: Some(20),
            children: vec![21, 22],
        }));
        actions.clear();
        rejected.on_report(20, Status::Rejected, &mut actions);
        assert_eq!(
            actions,
            [ContingencyAction::Cancel(21), ContingencyAction::Cancel(22)]
        );
        assert!(rejected.should_reject(21));

        let mut catalog = DataCatalog::default();
        let catalog_hash = catalog.register("primary", manifest).unwrap();
        assert_eq!(catalog.resolve("primary").unwrap().0, catalog_hash);
        assert_eq!(
            catalog.register("primary", DataManifest::default()),
            Err(PlatformError::DuplicateCatalogEntry)
        );

        let mut bus = PlatformCommandBus::with_capacity(1);
        let mut algorithm = OneShotAlgorithm;
        bus.run_algorithm(
            &mut algorithm,
            EventKey {
                timestamp: 1,
                phase: super::super::scheduler::EventPhase::StrategyCallback,
                source_priority: 0,
                venue_no: 1,
                asset_no: 1,
                sequence: 0,
            },
        )
        .unwrap();
        let command = bus.drain().next().unwrap();
        let ExecutionCommand::Submit(request) = command else {
            panic!("algorithm must emit a submit command");
        };
        assert_eq!(request.origin, OrderOrigin::ExecutionAlgorithm);
    }

    #[test]
    fn batch_custom_data_and_hooks_stay_outside_execution_core() {
        let configs = [
            RunConfig {
                run_id: 1,
                strategy_id: "s".into(),
                config_hash: 10,
                random_seed: 7,
            },
            RunConfig {
                run_id: 2,
                strategy_id: "s".into(),
                config_hash: 11,
                random_seed: 8,
            },
        ];
        let mut node = BatchNode::new(SeedTask::default());
        assert_eq!(node.run_all(&configs).unwrap(), [7, 8]);
        assert_ne!(
            RunConfig {
                random_seed: 7,
                ..configs[0].clone()
            }
            .effective_config_hash(),
            RunConfig {
                random_seed: 8,
                ..configs[0].clone()
            }
            .effective_config_hash()
        );

        let key = EventKey {
            timestamp: 5,
            phase: super::super::scheduler::EventPhase::StrategyCallback,
            source_priority: 2,
            venue_no: 3,
            asset_no: 4,
            sequence: 6,
        };
        let custom = CustomDataEnvelope {
            key,
            schema_id: 99,
            payload: [1_u8, 2, 3],
        };
        assert_eq!((custom.schema_id, custom.payload), (99, [1, 2, 3]));

        let mut bus = PlatformCommandBus::with_capacity(1);
        bus.run_hook(&mut CancelHook, key).unwrap();
        let ExecutionCommand::Cancel(cancel) = bus.drain().next().unwrap() else {
            panic!("hook must emit through the canonical command bus");
        };
        assert_eq!(
            (
                cancel.venue_id,
                cancel.instrument_id,
                cancel.local_submit_ts
            ),
            (VenueId(3), InstrumentId(4), 5)
        );

        let mut producers = PlatformCommandProducers::with_capacity(2);
        producers.add_algorithm(OneShotAlgorithm);
        producers.add_hook(CancelHook);
        let mut commands = Vec::new();
        producers.collect(key, &mut commands).unwrap();
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], ExecutionCommand::Submit(_)));
        assert!(matches!(commands[1], ExecutionCommand::Cancel(_)));
        producers.reset();

        let mut bounded = PlatformCommandProducers::with_capacity(1);
        bounded.add_algorithm(OneShotAlgorithm);
        bounded.add_hook(CancelHook);
        commands.clear();
        assert_eq!(
            bounded.collect(key, &mut commands),
            Err(PlatformError::CommandCapacity)
        );
    }
}

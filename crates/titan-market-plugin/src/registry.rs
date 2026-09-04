use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU8, Ordering},
    },
};

use crate::{
    AssetId, ConnectorLifecycle, MarketConnector, MarketError, MarketErrorKind,
    MarketSourceDefinition, MarketSourceHandle, MarketSourceId, MarketSourceSnapshot,
};
use titan_plugin_engine::ResourceScope;

pub(crate) struct ConnectorEntry {
    pub handle: MarketSourceHandle,
    pub definition: MarketSourceDefinition,
    pub connector: Arc<dyn MarketConnector>,
    pub resources: std::sync::Mutex<Option<ResourceScope>>,
    lifecycle: AtomicU8,
}

impl ConnectorEntry {
    pub(crate) fn new(
        handle: MarketSourceHandle,
        definition: MarketSourceDefinition,
        connector: Arc<dyn MarketConnector>,
        resources: ResourceScope,
    ) -> Self {
        Self {
            handle,
            definition,
            connector,
            resources: std::sync::Mutex::new(Some(resources)),
            lifecycle: AtomicU8::new(ConnectorLifecycle::Created as u8),
        }
    }

    pub(crate) fn lifecycle(&self) -> ConnectorLifecycle {
        match self.lifecycle.load(Ordering::Acquire) {
            0 => ConnectorLifecycle::Created,
            1 => ConnectorLifecycle::Starting,
            2 => ConnectorLifecycle::Running,
            3 => ConnectorLifecycle::Stopping,
            4 => ConnectorLifecycle::Stopped,
            _ => ConnectorLifecycle::Failed,
        }
    }

    pub(crate) fn set_lifecycle(&self, lifecycle: ConnectorLifecycle) {
        self.lifecycle.store(lifecycle as u8, Ordering::Release);
    }

    pub(crate) fn close_resources(&self) -> Result<(), MarketError> {
        let mut guard = self.resources.lock().unwrap_or_else(|p| p.into_inner());
        let Some(mut resources) = guard.take() else {
            return Ok(());
        };
        resources.close().map_err(|errors| {
            MarketError::new(
                MarketErrorKind::ResourceReleaseFailed,
                errors
                    .into_iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })
    }

    pub(crate) fn snapshot(&self) -> MarketSourceSnapshot {
        MarketSourceSnapshot {
            handle: self.handle,
            source_key: self.definition.source_key.clone(),
            connector_type: self.definition.connector_type.clone(),
            definition_version: self.definition.definition_version,
            enabled: self.definition.enabled,
            lifecycle: self.lifecycle(),
        }
    }
}

#[derive(Default)]
struct ConnectorRegistryState {
    by_id: HashMap<MarketSourceId, Arc<ConnectorEntry>>,
    by_key: HashMap<Arc<str>, MarketSourceHandle>,
    asset_owners: HashMap<AssetId, MarketSourceId>,
    last_generation: HashMap<Arc<str>, u64>,
    last_generation_by_id: HashMap<MarketSourceId, u64>,
    last_source_id: HashMap<Arc<str>, MarketSourceId>,
}

#[derive(Default)]
pub struct ConnectorRegistry {
    state: RwLock<ConnectorRegistryState>,
}

impl ConnectorRegistry {
    pub(crate) fn allocate_identity(
        &self,
        source_key: &str,
        max_sources: usize,
    ) -> Result<(MarketSourceId, u64), MarketError> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        let max_id = u32::try_from(max_sources).map_err(|_| {
            MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source capacity exceeds the SourceStreamId allocator",
            )
        })?;
        let preferred = state
            .last_source_id
            .get(source_key)
            .copied()
            .filter(|id| !state.by_id.contains_key(id));
        let source_id = preferred
            .or_else(|| {
                (1..=max_id)
                    .map(MarketSourceId)
                    .find(|id| !state.by_id.contains_key(id))
            })
            .ok_or_else(|| {
                MarketError::new(
                    MarketErrorKind::CapacityExceeded,
                    "no source stream slot available",
                )
            })?;
        let generation = state
            .last_generation
            .get(source_key)
            .copied()
            .unwrap_or(0)
            .max(
                state
                    .last_generation_by_id
                    .get(&source_id)
                    .copied()
                    .unwrap_or(0),
            )
            .saturating_add(1);
        Ok((source_id, generation))
    }

    pub(crate) fn validate_insert(
        &self,
        definition: &MarketSourceDefinition,
        max_sources: usize,
        max_instruments: usize,
        replacing: Option<MarketSourceId>,
    ) -> Result<(), MarketError> {
        if definition.source_key.trim().is_empty()
            || definition.connector_type.trim().is_empty()
            || definition.instruments.is_empty()
        {
            return Err(MarketError::new(
                MarketErrorKind::InvalidDefinition,
                "source_key, connector_type and instruments are required",
            ));
        }
        if definition.instruments.len() > max_instruments {
            return Err(MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source instrument capacity exceeded",
            ));
        }
        let mut symbols = HashSet::new();
        let mut assets = HashSet::new();
        for binding in definition.instruments.iter() {
            if binding.native_symbol.trim().is_empty()
                || !symbols.insert(binding.native_symbol.as_ref())
                || !assets.insert(binding.asset_id)
            {
                return Err(MarketError::new(
                    MarketErrorKind::InvalidDefinition,
                    "instrument symbols/assets must be unique and native symbols must not be empty",
                ));
            }
        }
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        let retained_instruments: usize = state
            .by_id
            .values()
            .filter(|entry| Some(entry.handle.source_id) != replacing)
            .map(|entry| entry.definition.instruments.len())
            .sum();
        if retained_instruments.saturating_add(definition.instruments.len()) > max_instruments {
            return Err(MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "plugin instrument capacity exceeded",
            ));
        }
        if state
            .by_key
            .get(definition.source_key.as_ref())
            .is_some_and(|handle| Some(handle.source_id) != replacing)
        {
            return Err(MarketError::new(
                MarketErrorKind::AlreadyExists,
                "source_key already exists",
            ));
        }
        if replacing.is_none() && state.by_id.len() >= max_sources {
            return Err(MarketError::new(
                MarketErrorKind::CapacityExceeded,
                "source capacity exceeded",
            ));
        }
        for asset in assets {
            if state
                .asset_owners
                .get(&asset)
                .is_some_and(|owner| Some(*owner) != replacing)
            {
                return Err(MarketError::new(
                    MarketErrorKind::AlreadyExists,
                    format!("asset_id {} already belongs to another source", asset.0),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn insert(&self, entry: Arc<ConnectorEntry>) -> Result<(), MarketError> {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        if state.by_id.contains_key(&entry.handle.source_id)
            || state.by_key.contains_key(&entry.definition.source_key)
        {
            return Err(MarketError::new(
                MarketErrorKind::AlreadyExists,
                "source already exists",
            ));
        }
        for binding in entry.definition.instruments.iter() {
            if state.asset_owners.contains_key(&binding.asset_id) {
                return Err(MarketError::new(
                    MarketErrorKind::AlreadyExists,
                    "asset already exists",
                ));
            }
        }
        state
            .last_generation
            .insert(entry.definition.source_key.clone(), entry.handle.generation);
        state
            .last_generation_by_id
            .insert(entry.handle.source_id, entry.handle.generation);
        state
            .last_source_id
            .insert(entry.definition.source_key.clone(), entry.handle.source_id);
        state
            .by_key
            .insert(entry.definition.source_key.clone(), entry.handle);
        for binding in entry.definition.instruments.iter() {
            state
                .asset_owners
                .insert(binding.asset_id, entry.handle.source_id);
        }
        state.by_id.insert(entry.handle.source_id, entry);
        Ok(())
    }

    pub(crate) fn get(
        &self,
        handle: MarketSourceHandle,
    ) -> Result<Arc<ConnectorEntry>, MarketError> {
        let state = self.state.read().unwrap_or_else(|p| p.into_inner());
        let entry = state
            .by_id
            .get(&handle.source_id)
            .ok_or_else(|| MarketError::new(MarketErrorKind::SourceNotFound, "source not found"))?;
        if entry.handle.generation != handle.generation {
            return Err(MarketError::new(
                MarketErrorKind::StaleHandle,
                "source handle generation is stale",
            ));
        }
        Ok(entry.clone())
    }

    pub(crate) fn resolve(&self, key: &str) -> Result<MarketSourceHandle, MarketError> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_key
            .get(key)
            .copied()
            .ok_or_else(|| MarketError::new(MarketErrorKind::SourceNotFound, "source not found"))
    }

    pub(crate) fn remove(
        &self,
        handle: MarketSourceHandle,
    ) -> Result<Arc<ConnectorEntry>, MarketError> {
        self.get(handle)?;
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        let entry = state
            .by_id
            .remove(&handle.source_id)
            .ok_or_else(|| MarketError::new(MarketErrorKind::SourceNotFound, "source not found"))?;
        state.by_key.remove(&entry.definition.source_key);
        for binding in entry.definition.instruments.iter() {
            state.asset_owners.remove(&binding.asset_id);
        }
        Ok(entry)
    }

    pub(crate) fn swap(
        &self,
        old: MarketSourceHandle,
        new_entry: Arc<ConnectorEntry>,
    ) -> Result<Arc<ConnectorEntry>, MarketError> {
        self.get(old)?;
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        let old_entry =
            state.by_id.get(&old.source_id).cloned().ok_or_else(|| {
                MarketError::new(MarketErrorKind::SourceNotFound, "source not found")
            })?;
        state.by_key.remove(&old_entry.definition.source_key);
        for binding in old_entry.definition.instruments.iter() {
            state.asset_owners.remove(&binding.asset_id);
        }
        state.last_generation.insert(
            new_entry.definition.source_key.clone(),
            new_entry.handle.generation,
        );
        state
            .last_generation_by_id
            .insert(new_entry.handle.source_id, new_entry.handle.generation);
        state.last_source_id.insert(
            new_entry.definition.source_key.clone(),
            new_entry.handle.source_id,
        );
        state
            .by_key
            .insert(new_entry.definition.source_key.clone(), new_entry.handle);
        for binding in new_entry.definition.instruments.iter() {
            state
                .asset_owners
                .insert(binding.asset_id, new_entry.handle.source_id);
        }
        state.by_id.insert(new_entry.handle.source_id, new_entry);
        Ok(old_entry)
    }

    pub(crate) fn list_entries(&self) -> Vec<Arc<ConnectorEntry>> {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .values()
            .cloned()
            .collect()
    }

    pub fn list(&self) -> Arc<[MarketSourceSnapshot]> {
        let mut snapshots: Vec<_> = self
            .list_entries()
            .into_iter()
            .map(|entry| entry.snapshot())
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.handle.source_id);
        snapshots.into()
    }

    pub fn len(&self) -> usize {
        self.state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .by_id
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

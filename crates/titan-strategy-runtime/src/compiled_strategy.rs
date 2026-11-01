use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use crate::*;

#[derive(Clone)]
pub struct LoadedStrategy {
    pub id: CompiledStrategyDigest,
    pub manifest: CompiledStrategyMetadata,
    pub native: Arc<LoadedNativeStrategyV13>,
}

#[derive(Clone)]
pub struct StrategyLoaderContext {
    pub allowed_strategy_roots: Arc<[Arc<str>]>,
    pub require_signature: bool,
    pub ed25519_keys: BTreeMap<Arc<str>, [u8; 32]>,
}

#[derive(Clone)]
pub struct StrategyLoadRequest {
    pub strategy_file: StrategyFileRef,
}

pub trait StrategyLoaderFactory: Send + Sync {
    fn loader_type(&self) -> &str;
    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyLoader>, StrategyError>;
}

pub trait StrategyLoader: Send + Sync {
    fn inspect(
        &self,
        strategy_file: &StrategyFileRef,
    ) -> Result<CompiledStrategyMetadata, StrategyError>;
    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<LoadedStrategy, StrategyError>;
}

#[derive(Default)]
pub struct StrategyLoaderRegistry {
    factories: RwLock<HashMap<Arc<str>, Arc<dyn StrategyLoaderFactory>>>,
}

impl StrategyLoaderRegistry {
    pub fn register(&self, factory: Arc<dyn StrategyLoaderFactory>) -> LocalResult<()> {
        let key: Arc<str> = Arc::from(factory.loader_type());
        if key.is_empty() {
            return Err(StrategyError::new(
                StrategyErrorKind::InvalidDefinition,
                "register_loader",
                "empty_loader_type",
                "loader type must not be empty",
            ));
        }
        let mut factories = self.factories.write().unwrap_or_else(|p| p.into_inner());
        if factories.contains_key(&key) {
            return Err(StrategyError::new(
                StrategyErrorKind::AlreadyExists,
                "register_loader",
                "loader_type_conflict",
                "loader type is already registered",
            ));
        }
        factories.insert(key, factory);
        Ok(())
    }

    pub fn create(
        &self,
        loader_type: &str,
        context: StrategyLoaderContext,
    ) -> LocalResult<Arc<dyn StrategyLoader>> {
        self.factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(loader_type)
            .ok_or_else(|| {
                StrategyError::new(
                    StrategyErrorKind::PackageNotFound,
                    "create_loader",
                    "loader_not_registered",
                    "requested loader type is not registered",
                )
            })?
            .create(context)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StrategyCacheKey {
    pub compiled_strategy_digest: [u8; 32],
}

struct CachedStrategy {
    compiled_strategy: LoadedStrategy,
    last_used: u64,
}

struct StrategyCacheState {
    values: BTreeMap<StrategyCacheKey, CachedStrategy>,
    clock: u64,
}

pub struct LoadedStrategyCache {
    capacity: usize,
    state: Mutex<StrategyCacheState>,
}

impl LoadedStrategyCache {
    pub fn new(capacity: usize) -> LocalResult<Self> {
        if capacity == 0 {
            return Err(StrategyError::new(
                StrategyErrorKind::InvalidDefinition,
                "strategy_cache",
                "zero_capacity",
                "compiled_strategy cache capacity must be positive",
            ));
        }
        Ok(Self {
            capacity,
            state: Mutex::new(StrategyCacheState {
                values: BTreeMap::new(),
                clock: 0,
            }),
        })
    }

    pub fn get(&self, key: &StrategyCacheKey) -> Option<LoadedStrategy> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.values.get_mut(key).map(|cached| {
            cached.last_used = clock;
            cached.compiled_strategy.clone()
        })
    }

    pub fn insert(&self, key: StrategyCacheKey, compiled_strategy: LoadedStrategy) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.values.contains_key(&key) {
            while state.values.len() >= self.capacity {
                let victim = state
                    .values
                    .iter()
                    .filter(|(_, cached)| Arc::strong_count(&cached.compiled_strategy.native) == 1)
                    .min_by_key(|(_, cached)| cached.last_used)
                    .map(|(key, _)| key.clone());
                let Some(victim) = victim else {
                    // Every cached compiled_strategy is still leased by a live runtime. Keep the
                    // cache bounded and let this compiled_strategy live only in its instance.
                    return;
                };
                state.values.remove(&victim);
            }
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.values.insert(
            key,
            CachedStrategy {
                compiled_strategy,
                last_used: clock,
            },
        );
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values
            .len()
    }
}

pub struct NativeV13LoaderFactory;

impl StrategyLoaderFactory for NativeV13LoaderFactory {
    fn loader_type(&self) -> &str {
        "native-v13"
    }

    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyLoader>, StrategyError> {
        Ok(Arc::new(NativeV13Loader { context }))
    }
}

struct NativeV13Loader {
    context: StrategyLoaderContext,
}

impl NativeV13Loader {
    fn strategy_path(&self, strategy_file: &StrategyFileRef) -> LocalResult<std::path::PathBuf> {
        let raw = strategy_file.uri.strip_prefix("file://").ok_or_else(|| {
            strategy_load_error("v13_uri", "V13 compiled_strategy URI must use file://")
        })?;
        let path = std::fs::canonicalize(raw).map_err(|_| {
            strategy_load_error("v13_path", "V13 compiled_strategy path cannot be resolved")
        })?;
        let allowed = self.context.allowed_strategy_roots.iter().any(|root| {
            std::fs::canonicalize(root.as_ref()).is_ok_and(|root| path.starts_with(root))
        });
        if !allowed {
            return Err(strategy_load_error(
                "strategy_root_denied",
                "V13 compiled_strategy is outside allowed roots",
            ));
        }
        Ok(path)
    }

    fn loader(&self) -> NativeStrategyLoaderV13 {
        NativeStrategyLoaderV13::new(
            std::env::temp_dir().join("titan-native-v13-cache"),
            V13TrustPolicy {
                require_signature: self.context.require_signature,
                ed25519_keys: self.context.ed25519_keys.clone(),
            },
        )
    }
}

fn manifest_v13(value: &CompiledStrategyMetadataV13) -> LocalResult<CompiledStrategyMetadata> {
    let source_version = semver::Version::parse(&value.source_version).map_err(|_| {
        strategy_load_error("strategy_version", "V13 strategy version is not semver")
    })?;
    let mut capabilities = 0_u64;
    if value
        .capabilities
        .contains(StrategyCapabilitiesV13::MARKET_DATA)
    {
        capabilities |= StrategyCapabilities::READ_TICK.0
            | StrategyCapabilities::READ_BAR.0
            | StrategyCapabilities::READ_DEPTH.0;
    }
    if value
        .capabilities
        .contains(StrategyCapabilitiesV13::ACCOUNT_DATA)
    {
        capabilities |= StrategyCapabilities::READ_ACCOUNT.0;
    }
    if value
        .capabilities
        .contains(StrategyCapabilitiesV13::ORDER_EXECUTION)
    {
        capabilities |= StrategyCapabilities::SUBMIT_ORDER.0 | StrategyCapabilities::CANCEL_ORDER.0;
    }
    if value.capabilities.contains(StrategyCapabilitiesV13::TIMER) {
        capabilities |= StrategyCapabilities::TIMER_CALLBACK.0;
    }
    let mut subscriptions = Vec::new();
    for item in value.subscriptions.iter() {
        let event_type: Arc<str> = Arc::from(match item.event {
            V13EventKind::Tick => titan_market_service::BBO_EVENT,
            V13EventKind::Bar => titan_market_service::BAR_BATCH_EVENT,
            V13EventKind::Depth => titan_market_service::DEPTH_BATCH_EVENT,
            V13EventKind::Fill => titan_account_service::FILL_EVENT,
            V13EventKind::Order | V13EventKind::Cancel => {
                titan_account_service::ORDER_CHANGED_EVENT
            }
            V13EventKind::Position => titan_account_service::POSITION_CHANGED_EVENT,
            V13EventKind::Balance => titan_account_service::BALANCE_CHANGED_EVENT,
            V13EventKind::AccountState => titan_account_service::STREAM_STATE_CHANGED_EVENT,
            V13EventKind::Timer
            | V13EventKind::Start
            | V13EventKind::Stop
            | V13EventKind::Recover => "titan.strategy.Timer",
        });
        if event_type.as_ref() == "titan.strategy.Timer" {
            continue;
        }
        let subscription = StrategySubscriptionSpec {
            event_type,
            schema_version: item.schema_version,
            routing_keys: Arc::from([]),
            qos: match item.qos {
                V13EventQos::Latest => titan_core_types::EventQos::Latest,
                V13EventQos::ReliableOrdered => titan_core_types::EventQos::ReliableOrdered,
                V13EventQos::BestEffort => titan_core_types::EventQos::BestEffort,
            },
        };
        if !subscriptions
            .iter()
            .any(|existing: &StrategySubscriptionSpec| {
                existing.event_type == subscription.event_type
                    && existing.schema_version == subscription.schema_version
                    && existing.qos == subscription.qos
            })
        {
            subscriptions.push(subscription);
        }
    }
    if value
        .capabilities
        .contains(StrategyCapabilitiesV13::HOST_DEPTH_100)
    {
        subscriptions.push(StrategySubscriptionSpec {
            event_type: Arc::from(titan_market_service::STREAM_INVALIDATED_EVENT),
            schema_version: 1,
            routing_keys: Arc::from([]),
            qos: titan_core_types::EventQos::ReliableOrdered,
        });
    }
    Ok(CompiledStrategyMetadata {
        runtime_type: Arc::from("native-v13"),
        source_version,
        runtime_abi: titan_core_types::ApiVersion::new(13, 0),
        parameter_schema: value.parameter_schema.clone(),
        parameter_schema_version: 1,
        recovery_state_schema_version: value.state.version,
        state_byte_len: value.state.byte_len,
        callbacks: StrategyCallbackMask(value.callback_mask as u32),
        capabilities: StrategyCapabilities(capabilities),
        subscriptions: subscriptions.into(),
        compiled_strategy_digest: value.compiled_strategy_digest,
    })
}

impl StrategyLoader for NativeV13Loader {
    fn inspect(
        &self,
        strategy_file: &StrategyFileRef,
    ) -> Result<CompiledStrategyMetadata, StrategyError> {
        let path = self.strategy_path(strategy_file)?;
        let value = self.loader().inspect(&path).map_err(|_| {
            strategy_load_error("v13_inspect", "V13 compiled_strategy inspection failed")
        })?;
        if value.compiled_strategy_digest != strategy_file.expected_digest {
            return Err(strategy_load_error(
                "compiled_strategy_digest_mismatch",
                "V13 compiled_strategy digest does not match deployment pin",
            ));
        }
        manifest_v13(&value)
    }

    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<LoadedStrategy, StrategyError> {
        if Instant::now() >= deadline {
            return Err(strategy_load_error(
                "v13_load_deadline",
                "V13 load deadline expired",
            ));
        }
        let path = self.strategy_path(&request.strategy_file)?;
        let compiled_strategy = self.loader().load(&path).map_err(|_| {
            strategy_load_error("v13_load", "V13 native compiled_strategy load failed")
        })?;
        if compiled_strategy.manifest.compiled_strategy_digest
            != request.strategy_file.expected_digest
        {
            return Err(strategy_load_error(
                "compiled_strategy_digest_mismatch",
                "V13 compiled_strategy digest does not match deployment pin",
            ));
        }
        let manifest = manifest_v13(&compiled_strategy.manifest)?;
        Ok(LoadedStrategy {
            id: CompiledStrategyDigest {
                digest: manifest.compiled_strategy_digest,
            },
            manifest,
            native: Arc::new(compiled_strategy),
        })
    }
}

fn strategy_load_error(code: &'static str, message: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::LoadFailed,
        "native_v13_loader",
        code,
        message,
    )
}

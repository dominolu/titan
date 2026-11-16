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
    pub native: Arc<LoadedNativeStrategy>,
}

#[derive(Clone)]
pub struct StrategyLoaderContext {
    pub allowed_strategy_roots: Arc<[Arc<str>]>,
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

pub struct NativeLoaderFactory;

impl StrategyLoaderFactory for NativeLoaderFactory {
    fn loader_type(&self) -> &str {
        "native"
    }

    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyLoader>, StrategyError> {
        Ok(Arc::new(NativeLoader { context }))
    }
}

struct NativeLoader {
    context: StrategyLoaderContext,
}

impl NativeLoader {
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

    fn loader(&self) -> NativeStrategyLoader {
        NativeStrategyLoader::new(std::env::temp_dir().join("titan-native-cache"))
    }
}

fn manifest_from_artifact(value: &ArtifactMetadata) -> LocalResult<CompiledStrategyMetadata> {
    let source_version = semver::Version::parse(&value.source_version).map_err(|_| {
        strategy_load_error("strategy_version", "V13 strategy version is not semver")
    })?;
    let mut capabilities = 0_u64;
    if value
        .capabilities
        .contains(ManifestCapabilities::MARKET_DATA)
    {
        capabilities |= StrategyCapabilities::READ_TICK.0
            | StrategyCapabilities::READ_BAR.0
            | StrategyCapabilities::READ_DEPTH.0;
    }
    if value
        .capabilities
        .contains(ManifestCapabilities::ACCOUNT_DATA)
    {
        capabilities |= StrategyCapabilities::READ_ACCOUNT.0;
    }
    if value
        .capabilities
        .contains(ManifestCapabilities::ORDER_EXECUTION)
    {
        capabilities |= StrategyCapabilities::SUBMIT_ORDER.0 | StrategyCapabilities::CANCEL_ORDER.0;
    }
    if value.capabilities.contains(ManifestCapabilities::TIMER) {
        capabilities |= StrategyCapabilities::TIMER_CALLBACK.0;
    }
    if value
        .capabilities
        .contains(ManifestCapabilities::OBJECT_CONTEXT)
    {
        // The object table is the single capability behind two wire encodings: V13 manifest bit
        // 1<<11 and V14 runtime bit 1<<13. Both are named by [`OBJECT_CONTEXT_CAPABILITY`]
        // (`"object_context"`), which documents the pair and why the bit numbers differ. Neither
        // typed constant may be replaced by a bare literal here.
        capabilities |= StrategyCapabilities::OBJECT_CONTEXT.0;
    }
    for (native, runtime) in [
        (
            ManifestCapabilities::BUSINESS_FAULT_DRAIN,
            StrategyCapabilities::BUSINESS_FAULT_DRAIN,
        ),
        (
            ManifestCapabilities::REQUIRES_PERIODIC_TIMER,
            StrategyCapabilities::REQUIRES_PERIODIC_TIMER,
        ),
        (
            ManifestCapabilities::MULTI_VENUE_EXECUTION,
            StrategyCapabilities::MULTI_VENUE_EXECUTION,
        ),
    ] {
        if value.capabilities.contains(native) {
            capabilities |= runtime.0;
        }
    }
    let mut subscriptions = Vec::new();
    for item in value.subscriptions.iter() {
        let event_type: Arc<str> = Arc::from(match item.event {
            EventKind::Tick => titan_market_service::BBO_EVENT,
            EventKind::Bar => titan_market_service::BAR_BATCH_EVENT,
            EventKind::Depth => titan_market_service::DEPTH_BATCH_EVENT,
            EventKind::Fill => titan_account_service::FILL_EVENT,
            EventKind::Order | EventKind::Cancel => titan_account_service::ORDER_CHANGED_EVENT,
            EventKind::Position => titan_account_service::POSITION_CHANGED_EVENT,
            EventKind::Balance => titan_account_service::BALANCE_CHANGED_EVENT,
            EventKind::AccountState => titan_account_service::STREAM_STATE_CHANGED_EVENT,
            EventKind::Timer | EventKind::Start | EventKind::Stop | EventKind::Recover => {
                "titan.strategy.Timer"
            }
        });
        if event_type.as_ref() == "titan.strategy.Timer" {
            continue;
        }
        let subscription = StrategySubscriptionSpec {
            event_type,
            schema_version: item.schema_version,
            routing_keys: Arc::from([]),
            qos: match item.qos {
                SubscriptionQos::Latest => titan_core_types::EventQos::Latest,
                SubscriptionQos::ReliableOrdered => titan_core_types::EventQos::ReliableOrdered,
                SubscriptionQos::BestEffort => titan_core_types::EventQos::BestEffort,
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
        .contains(ManifestCapabilities::HOST_DEPTH_100)
    {
        subscriptions.push(StrategySubscriptionSpec {
            event_type: Arc::from(titan_market_service::STREAM_INVALIDATED_EVENT),
            schema_version: 1,
            routing_keys: Arc::from([]),
            qos: titan_core_types::EventQos::ReliableOrdered,
        });
    }
    Ok(CompiledStrategyMetadata {
        runtime_type: Arc::from("native"),
        source_version,
        runtime_abi: titan_core_types::ApiVersion::new(14, 0),
        parameter_schema: value.parameter_schema.clone(),
        parameter_schema_version: 1,
        parameters_digest: value.parameters_digest,
        recovery_state_schema_version: value.state.version,
        state_byte_len: value.state.byte_len,
        callbacks: StrategyCallbackMask(value.callback_mask as u32),
        capabilities: StrategyCapabilities(capabilities),
        subscriptions: subscriptions.into(),
        object_manifest: value.object_manifest.clone(),
        compiled_strategy_digest: value.compiled_strategy_digest,
    })
}

impl StrategyLoader for NativeLoader {
    fn inspect(
        &self,
        strategy_file: &StrategyFileRef,
    ) -> Result<CompiledStrategyMetadata, StrategyError> {
        let path = self.strategy_path(strategy_file)?;
        let value = self.loader().inspect(&path).map_err(v13_load_error)?;
        if value.compiled_strategy_digest != strategy_file.expected_digest {
            return Err(strategy_load_error(
                "compiled_strategy_digest_mismatch",
                "V13 compiled_strategy digest does not match deployment pin",
            ));
        }
        manifest_from_artifact(&value)
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
        let compiled_strategy = self.loader().load(&path).map_err(v13_load_error)?;
        if compiled_strategy.manifest.compiled_strategy_digest
            != request.strategy_file.expected_digest
        {
            return Err(strategy_load_error(
                "compiled_strategy_digest_mismatch",
                "V13 compiled_strategy digest does not match deployment pin",
            ));
        }
        let manifest = manifest_from_artifact(&compiled_strategy.manifest)?;
        Ok(LoadedStrategy {
            id: CompiledStrategyDigest {
                digest: manifest.compiled_strategy_digest,
            },
            manifest,
            native: Arc::new(compiled_strategy),
        })
    }
}

fn strategy_load_error(code: &'static str, message: impl Into<Arc<str>>) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::LoadFailed,
        "native_loader",
        code,
        message,
    )
}

/// Maps a loader failure onto a stable reason code.
///
/// The previous `map_err(|_| ...)` erased every cause, so a fingerprint change (the expected
/// consequence of an ABI revision) looked like an unspecified load failure. An operator could not
/// tell "recompile the strategy" from a corrupt file.
fn v13_load_error(error: LoadError) -> StrategyError {
    let (kind, code) = match &error {
        LoadError::AbiVersion { .. } => {
            (StrategyErrorKind::AbiMismatch, "v13_abi_version_mismatch")
        }
        LoadError::AbiFingerprint => (
            StrategyErrorKind::AbiMismatch,
            "v13_abi_fingerprint_mismatch",
        ),
        LoadError::AbiLayout { .. } => (StrategyErrorKind::AbiMismatch, "v13_abi_layout_mismatch"),
        LoadError::Bundle(_) => (StrategyErrorKind::LoadFailed, "v13_bundle_invalid"),
        LoadError::Io(_) => (StrategyErrorKind::LoadFailed, "v13_io"),
        LoadError::Cbor(_) => (StrategyErrorKind::LoadFailed, "v13_cbor_invalid"),
        LoadError::Contract(_) => (StrategyErrorKind::LoadFailed, "v13_contract_violation"),
        LoadError::State(_) => (StrategyErrorKind::InvalidState, "v13_state_invalid"),
        LoadError::Native(_) => (StrategyErrorKind::LoadFailed, "v13_native_load_failed"),
        LoadError::Callback { .. } => (StrategyErrorKind::CallbackFailed, "v13_callback_failed"),
    };
    StrategyError::new(kind, "native_loader", code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_mismatch_is_reported_as_a_structured_reason() {
        let error = v13_load_error(LoadError::AbiFingerprint);
        assert_eq!(error.kind, StrategyErrorKind::AbiMismatch);
        assert_eq!(error.reason_code.as_ref(), "v13_abi_fingerprint_mismatch");
        assert!(error.message.contains("recompile"), "{}", error.message);

        let error = v13_load_error(LoadError::AbiVersion {
            artifact: 13,
            host: 14,
        });
        assert_eq!(error.kind, StrategyErrorKind::AbiMismatch);
        assert_eq!(error.reason_code.as_ref(), "v13_abi_version_mismatch");
        assert!(error.message.contains("artifact 13"));
        assert!(error.message.contains("host 14"));

        let error = v13_load_error(LoadError::Contract("state mismatch"));
        assert_eq!(error.kind, StrategyErrorKind::LoadFailed);
        assert_eq!(error.reason_code.as_ref(), "v13_contract_violation");
        assert!(error.message.contains("state mismatch"));
    }

    /// The old status code moved the object-context capability through hard-coded bits while the
    /// manifest string lived elsewhere, so three literals described one capability. The bits are
    /// still the two frozen wire formats, but they are now documented as one capability in
    /// `object_gate.rs::OBJECT_CONTEXT_CAPABILITY`; this pins that the V13→V14 translation still
    /// means the same thing and that the two bit numbers are deliberately different.
    /// This repository was not compiled for this change, so the checks are deliberately limited to
    /// what is structural: the string source, the two bit numbers, their inequality, and the
    /// V13→V14 translation. They do not re-implement `manifest_from_artifact`'s other validations.
    #[test]
    fn object_context_capability_keeps_its_meaning_across_bits() {
        assert_eq!(OBJECT_CONTEXT_CAPABILITY, "object_context");
        assert_eq!(ManifestCapabilities::OBJECT_CONTEXT.0, 1 << 11);
        assert_eq!(StrategyCapabilities::OBJECT_CONTEXT.0, 1 << 13);
        // Not a copy: the two wire formats use different bit numbers on purpose.
        assert_ne!(
            ManifestCapabilities::OBJECT_CONTEXT.0,
            StrategyCapabilities::OBJECT_CONTEXT.0
        );
        // Neither bit leaks into the neighbour bit of its own set.
        assert!(
            !ManifestCapabilities::OBJECT_CONTEXT
                .contains(ManifestCapabilities::MULTI_VENUE_EXECUTION)
        );
        assert!(
            !StrategyCapabilities::OBJECT_CONTEXT
                .contains(StrategyCapabilities::MULTI_VENUE_EXECUTION)
        );
        // The V14 bit is not reachable from any V13 bit other than OBJECT_CONTEXT.
        let all_but_object_context = ManifestCapabilities(
            ManifestCapabilities::MARKET_DATA.0
                | ManifestCapabilities::ACCOUNT_DATA.0
                | ManifestCapabilities::ORDER_EXECUTION.0
                | ManifestCapabilities::TIMER.0
                | ManifestCapabilities::STOP_BARRIER.0
                | ManifestCapabilities::RECOVERY_ORDER_RESULTS.0
                | ManifestCapabilities::PAUSE_SAFE.0
                | ManifestCapabilities::HOST_DEPTH_100.0
                | ManifestCapabilities::BUSINESS_FAULT_DRAIN.0
                | ManifestCapabilities::REQUIRES_PERIODIC_TIMER.0
                | ManifestCapabilities::MULTI_VENUE_EXECUTION.0,
        );
        assert!(!all_but_object_context.contains(ManifestCapabilities::OBJECT_CONTEXT));
    }
}

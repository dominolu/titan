use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use crate::*;

#[derive(Clone)]
pub struct StrategyArtifact {
    pub id: StrategyArtifactId,
    pub manifest: StrategyPackageManifest,
    pub native: Arc<StrategyArtifactV13>,
}

#[derive(Clone)]
pub struct StrategyLoaderContext {
    pub allowed_artifact_roots: Arc<[Arc<str>]>,
    pub require_signature: bool,
}

#[derive(Clone)]
pub struct StrategyLoadRequest {
    pub package: StrategyPackageRef,
}

pub trait StrategyPackageLoaderFactory: Send + Sync {
    fn loader_type(&self) -> &str;
    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError>;
}

pub trait StrategyPackageLoader: Send + Sync {
    fn inspect(
        &self,
        package: &StrategyPackageRef,
    ) -> Result<StrategyPackageManifest, StrategyError>;
    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<StrategyArtifact, StrategyError>;
}

#[derive(Default)]
pub struct StrategyPackageLoaderRegistry {
    factories: RwLock<HashMap<Arc<str>, Arc<dyn StrategyPackageLoaderFactory>>>,
}

impl StrategyPackageLoaderRegistry {
    pub fn register(&self, factory: Arc<dyn StrategyPackageLoaderFactory>) -> LocalResult<()> {
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
    ) -> LocalResult<Arc<dyn StrategyPackageLoader>> {
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
pub struct ArtifactCacheKey {
    pub artifact_digest: [u8; 32],
}

struct CachedArtifact {
    artifact: StrategyArtifact,
    last_used: u64,
}

struct ArtifactCacheState {
    values: BTreeMap<ArtifactCacheKey, CachedArtifact>,
    clock: u64,
}

pub struct StrategyArtifactCache {
    capacity: usize,
    state: Mutex<ArtifactCacheState>,
}

impl StrategyArtifactCache {
    pub fn new(capacity: usize) -> LocalResult<Self> {
        if capacity == 0 {
            return Err(StrategyError::new(
                StrategyErrorKind::InvalidDefinition,
                "artifact_cache",
                "zero_capacity",
                "artifact cache capacity must be positive",
            ));
        }
        Ok(Self {
            capacity,
            state: Mutex::new(ArtifactCacheState {
                values: BTreeMap::new(),
                clock: 0,
            }),
        })
    }

    pub fn get(&self, key: &ArtifactCacheKey) -> Option<StrategyArtifact> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.values.get_mut(key).map(|cached| {
            cached.last_used = clock;
            cached.artifact.clone()
        })
    }

    pub fn insert(&self, key: ArtifactCacheKey, artifact: StrategyArtifact) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.values.contains_key(&key) {
            while state.values.len() >= self.capacity {
                let victim = state
                    .values
                    .iter()
                    .filter(|(_, cached)| Arc::strong_count(&cached.artifact.native) == 1)
                    .min_by_key(|(_, cached)| cached.last_used)
                    .map(|(key, _)| key.clone());
                let Some(victim) = victim else {
                    // Every cached artifact is still leased by a live runtime. Keep the
                    // cache bounded and let this artifact live only in its instance.
                    return;
                };
                state.values.remove(&victim);
            }
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.values.insert(
            key,
            CachedArtifact {
                artifact,
                last_used: clock,
            },
        );
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

impl StrategyPackageLoaderFactory for NativeV13LoaderFactory {
    fn loader_type(&self) -> &str {
        "native-v13"
    }

    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError> {
        Ok(Arc::new(NativeV13Loader { context }))
    }
}

struct NativeV13Loader {
    context: StrategyLoaderContext,
}

impl NativeV13Loader {
    fn artifact_path(&self, package: &StrategyPackageRef) -> LocalResult<std::path::PathBuf> {
        let raw = package
            .uri
            .strip_prefix("file://")
            .ok_or_else(|| artifact_error("v13_uri", "V13 artifact URI must use file://"))?;
        let path = std::fs::canonicalize(raw)
            .map_err(|_| artifact_error("v13_path", "V13 artifact path cannot be resolved"))?;
        let allowed = self.context.allowed_artifact_roots.iter().any(|root| {
            std::fs::canonicalize(root.as_ref())
                .is_ok_and(|root| path.starts_with(root))
        });
        if !allowed {
            return Err(artifact_error(
                "artifact_root_denied",
                "V13 artifact is outside allowed roots",
            ));
        }
        Ok(path)
    }

    fn loader(&self) -> NativeArtifactLoaderV13 {
        NativeArtifactLoaderV13::new(
            std::env::temp_dir().join("titan-native-v13-cache"),
            V13TrustPolicy {
                require_signature: self.context.require_signature,
                ..V13TrustPolicy::default()
            },
        )
    }
}

fn manifest_v13(value: &ArtifactManifestV13) -> LocalResult<StrategyPackageManifest> {
    let package_version = semver::Version::parse(&value.strategy_version)
        .map_err(|_| artifact_error("strategy_version", "V13 strategy version is not semver"))?;
    let mut capabilities = 0_u64;
    if value.capabilities.contains(StrategyCapabilitiesV13::MARKET_DATA) {
        capabilities |= StrategyCapabilities::READ_TICK.0
            | StrategyCapabilities::READ_BAR.0
            | StrategyCapabilities::READ_DEPTH.0;
    }
    if value.capabilities.contains(StrategyCapabilitiesV13::ACCOUNT_DATA) {
        capabilities |= StrategyCapabilities::READ_ACCOUNT.0;
    }
    if value.capabilities.contains(StrategyCapabilitiesV13::ORDER_EXECUTION) {
        capabilities |= StrategyCapabilities::SUBMIT_ORDER.0 | StrategyCapabilities::CANCEL_ORDER.0;
    }
    if value.capabilities.contains(StrategyCapabilitiesV13::TIMER) {
        capabilities |= StrategyCapabilities::SCHEDULE_TIMER.0;
    }
    let mut subscriptions = Vec::new();
    for item in value.subscriptions.iter() {
        let event_type: Arc<str> = Arc::from(match item.event {
            V13EventKind::Tick => titan_market_service::BBO_EVENT,
            V13EventKind::Bar => titan_market_service::BAR_BATCH_EVENT,
            V13EventKind::Depth => titan_market_service::DEPTH_BATCH_EVENT,
            V13EventKind::Fill => titan_account_service::FILL_EVENT,
            V13EventKind::Order | V13EventKind::Cancel => titan_account_service::ORDER_CHANGED_EVENT,
            V13EventKind::Position => titan_account_service::POSITION_CHANGED_EVENT,
            V13EventKind::Balance => titan_account_service::BALANCE_CHANGED_EVENT,
            V13EventKind::AccountState => titan_account_service::STREAM_STATE_CHANGED_EVENT,
            V13EventKind::Timer | V13EventKind::Start | V13EventKind::Stop => "titan.strategy.Timer",
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
        if !subscriptions.iter().any(|existing: &StrategySubscriptionSpec| {
            existing.event_type == subscription.event_type
                && existing.schema_version == subscription.schema_version
                && existing.qos == subscription.qos
        }) {
            subscriptions.push(subscription);
        }
    }
    Ok(StrategyPackageManifest {
        strategy_type: Arc::from("native-v13"),
        package_version,
        runtime_abi: titan_core_types::ApiVersion::new(13, 0),
        parameter_schema: value.parameter_schema.clone(),
        parameter_schema_version: 1,
        state_schema_version: value.state.version,
        state_byte_len: value.state.byte_len,
        callbacks: StrategyCallbackMask(value.callback_mask as u32),
        capabilities: StrategyCapabilities(capabilities),
        subscriptions: subscriptions.into(),
        artifact_digest: value.artifact_digest,
    })
}

impl StrategyPackageLoader for NativeV13Loader {
    fn inspect(
        &self,
        package: &StrategyPackageRef,
    ) -> Result<StrategyPackageManifest, StrategyError> {
        let path = self.artifact_path(package)?;
        let value = self
            .loader()
            .inspect(&path)
            .map_err(|_| artifact_error("v13_inspect", "V13 artifact inspection failed"))?;
        if value.artifact_digest != package.expected_digest {
            return Err(artifact_error(
                "artifact_digest_mismatch",
                "V13 artifact digest does not match deployment pin",
            ));
        }
        manifest_v13(&value)
    }

    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<StrategyArtifact, StrategyError> {
        if Instant::now() >= deadline {
            return Err(artifact_error("v13_load_deadline", "V13 load deadline expired"));
        }
        let path = self.artifact_path(&request.package)?;
        let artifact = self
            .loader()
            .load(&path)
            .map_err(|_| artifact_error("v13_load", "V13 native artifact load failed"))?;
        if artifact.manifest.artifact_digest != request.package.expected_digest {
            return Err(artifact_error(
                "artifact_digest_mismatch",
                "V13 artifact digest does not match deployment pin",
            ));
        }
        let manifest = manifest_v13(&artifact.manifest)?;
        Ok(StrategyArtifact {
            id: StrategyArtifactId { digest: manifest.artifact_digest },
            manifest,
            native: Arc::new(artifact),
        })
    }
}

fn artifact_error(code: &'static str, message: &'static str) -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::LoadFailed,
        "native_v13_loader",
        code,
        message,
    )
}

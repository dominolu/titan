use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use sha2::{Digest, Sha256};
#[cfg(feature = "numba-loader")]
use titan_python_host::{LoadedNumbaStrategy, StrategyCompiler};
use titan_runtime::CallbackRegistry;

use crate::*;

pub trait StrategyCodeKeepalive: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> StrategyCodeKeepalive for T {}

#[derive(Clone)]
pub struct StrategyCodeLease(Arc<dyn StrategyCodeKeepalive>);

impl StrategyCodeLease {
    pub fn new(value: impl StrategyCodeKeepalive) -> Self {
        Self(Arc::new(value))
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

impl Default for StrategyCodeLease {
    fn default() -> Self {
        Self::new(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct StrategyStateMemory {
    pub f64_values: Vec<f64>,
    pub i64_values: Vec<i64>,
}

pub struct StrategyArtifact {
    pub id: StrategyArtifactId,
    pub manifest: StrategyPackageManifest,
    pub callbacks: CallbackRegistry,
    pub state: StrategyStateMemory,
    pub code_lease: StrategyCodeLease,
}

impl StrategyArtifact {
    pub fn clone_for_instance(&self, f64_capacity: usize, i64_capacity: usize) -> Self {
        let mut state = StrategyStateMemory {
            f64_values: vec![0.0; f64_capacity],
            i64_values: vec![0; i64_capacity],
        };
        let f64_len = state.f64_values.len().min(self.state.f64_values.len());
        let i64_len = state.i64_values.len().min(self.state.i64_values.len());
        state.f64_values[..f64_len].copy_from_slice(&self.state.f64_values[..f64_len]);
        state.i64_values[..i64_len].copy_from_slice(&self.state.i64_values[..i64_len]);
        Self {
            id: self.id,
            manifest: self.manifest.clone(),
            callbacks: self.callbacks.clone(),
            state,
            code_lease: self.code_lease.clone(),
        }
    }
}

#[derive(Clone)]
pub struct StrategyLoaderContext {
    pub allowed_artifact_roots: Arc<[Arc<str>]>,
    pub require_signature: bool,
}

#[derive(Clone)]
pub struct StrategyLoadRequest {
    pub package: StrategyPackageRef,
    pub entrypoint: Arc<str>,
    pub parameters: Arc<[u8]>,
    pub runtime_abi_fingerprint: Arc<str>,
    pub target_cpu: Arc<str>,
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
    pub entrypoint: Arc<str>,
    pub normalized_parameters_digest: [u8; 32],
    pub runtime_abi_fingerprint: Arc<str>,
    pub target_cpu: Arc<str>,
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

    pub fn get(
        &self,
        key: &ArtifactCacheKey,
        f64_capacity: usize,
        i64_capacity: usize,
    ) -> Option<StrategyArtifact> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        state.values.get_mut(key).map(|cached| {
            cached.last_used = clock;
            cached
                .artifact
                .clone_for_instance(f64_capacity, i64_capacity)
        })
    }

    pub fn insert(&self, key: ArtifactCacheKey, artifact: StrategyArtifact) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.values.contains_key(&key) {
            while state.values.len() >= self.capacity {
                let victim = state
                    .values
                    .iter()
                    .filter(|(_, cached)| cached.artifact.code_lease.strong_count() == 1)
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

pub fn normalized_parameters_digest(parameters: &[u8]) -> LocalResult<[u8; 32]> {
    let value: serde_json::Value = serde_json::from_slice(parameters).map_err(|_| {
        StrategyError::new(
            StrategyErrorKind::ParameterInvalid,
            "parameters",
            "invalid_json",
            "strategy parameters are not valid JSON",
        )
    })?;
    let normalized = serde_json::to_vec(&value).map_err(|_| {
        StrategyError::new(
            StrategyErrorKind::ParameterInvalid,
            "parameters",
            "normalization_failed",
            "strategy parameters could not be normalized",
        )
    })?;
    Ok(Sha256::digest(normalized).into())
}

#[cfg(feature = "numba-loader")]
pub struct InProcessNumbaLoaderFactory {
    compiler: Arc<dyn StrategyCompiler>,
}

#[cfg(feature = "numba-loader")]
impl InProcessNumbaLoaderFactory {
    pub fn new(compiler: Arc<dyn StrategyCompiler>) -> Self {
        Self { compiler }
    }
}

#[cfg(feature = "numba-loader")]
impl StrategyPackageLoaderFactory for InProcessNumbaLoaderFactory {
    fn loader_type(&self) -> &str {
        "numba-python"
    }

    fn create(
        &self,
        context: StrategyLoaderContext,
    ) -> Result<Arc<dyn StrategyPackageLoader>, StrategyError> {
        Ok(Arc::new(InProcessNumbaLoader {
            context,
            compiler: self.compiler.clone(),
        }))
    }
}

#[cfg(feature = "numba-loader")]
struct InProcessNumbaLoader {
    context: StrategyLoaderContext,
    compiler: Arc<dyn StrategyCompiler>,
}

#[cfg(feature = "numba-loader")]
#[derive(serde::Deserialize)]
struct PackageManifestFile {
    strategy_type: String,
    package_version: semver::Version,
    runtime_abi: titan_plugin_engine::ApiVersion,
    parameter_schema: serde_json::Value,
    parameter_schema_version: u32,
    state_schema_version: u32,
    callbacks: u32,
    capabilities: u64,
    artifact_digest: String,
    content_files: Vec<String>,
}

#[cfg(feature = "numba-loader")]
impl StrategyPackageLoader for InProcessNumbaLoader {
    fn inspect(
        &self,
        package: &StrategyPackageRef,
    ) -> Result<StrategyPackageManifest, StrategyError> {
        let (root, file) = self.read_manifest(package)?;
        let digest = digest_content_files(&root, &file.content_files)?;
        let declared = parse_sha256(&file.artifact_digest)?;
        if digest != declared || digest != package.expected_digest {
            return Err(StrategyError::new(
                StrategyErrorKind::DigestMismatch,
                "numba_inspect",
                "content_digest_mismatch",
                "package content does not match its pinned digest",
            ));
        }
        Ok(StrategyPackageManifest {
            strategy_type: Arc::from(file.strategy_type),
            package_version: file.package_version,
            runtime_abi: file.runtime_abi,
            parameter_schema: Arc::new(file.parameter_schema),
            parameter_schema_version: file.parameter_schema_version,
            state_schema_version: file.state_schema_version,
            callbacks: StrategyCallbackMask(file.callbacks),
            capabilities: StrategyCapabilities(file.capabilities),
            artifact_digest: digest,
        })
    }

    fn load(
        &self,
        request: StrategyLoadRequest,
        deadline: Instant,
    ) -> Result<StrategyArtifact, StrategyError> {
        if Instant::now() >= deadline {
            return Err(StrategyError::new(
                StrategyErrorKind::CompileFailed,
                "numba_load",
                "deadline",
                "strategy compile deadline expired",
            ));
        }
        let manifest = self.inspect(&request.package)?;
        let parameters: serde_json::Value =
            serde_json::from_slice(&request.parameters).map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::ParameterInvalid,
                    "numba_load",
                    "parameter_json",
                    "strategy parameters are not valid JSON",
                )
            })?;
        let loaded = self
            .compiler
            .compile(
                &titan_python_host::StrategySpec {
                    entrypoint: request.entrypoint.to_string(),
                    parameters,
                },
                &titan_runtime::runtime_abi_descriptor(),
            )
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::CompileFailed,
                    "numba_load",
                    "compile_failed",
                    "Numba strategy compilation failed",
                )
            })?;
        let callbacks = unsafe { CallbackRegistry::from_addresses(&loaded.callback_addresses) }
            .map_err(|_| {
                StrategyError::new(
                    StrategyErrorKind::AbiMismatch,
                    "numba_load",
                    "callback_addresses",
                    "compiled callback registry is invalid",
                )
            })?;
        let state = unsafe {
            StrategyStateMemory {
                f64_values: std::slice::from_raw_parts(loaded.state_f64_ptr, loaded.state_f64_len)
                    .to_vec(),
                i64_values: std::slice::from_raw_parts(loaded.state_i64_ptr, loaded.state_i64_len)
                    .to_vec(),
            }
        };
        Ok(StrategyArtifact {
            id: StrategyArtifactId {
                digest: manifest.artifact_digest,
            },
            manifest,
            callbacks,
            state,
            code_lease: StrategyCodeLease::new(NumbaKeepalive { _loaded: loaded }),
        })
    }
}

// The Python handle is created under the GIL and is never accessed on the callback worker. It is
// retained solely to keep generated executable code and NumPy state owners alive.
#[cfg(feature = "numba-loader")]
struct NumbaKeepalive {
    _loaded: LoadedNumbaStrategy,
}
#[cfg(feature = "numba-loader")]
unsafe impl Send for NumbaKeepalive {}
#[cfg(feature = "numba-loader")]
unsafe impl Sync for NumbaKeepalive {}

#[cfg(feature = "numba-loader")]
impl InProcessNumbaLoader {
    fn read_manifest(
        &self,
        package: &StrategyPackageRef,
    ) -> LocalResult<(std::path::PathBuf, PackageManifestFile)> {
        if self.context.require_signature && package.signature_ref.is_none() {
            return Err(StrategyError::new(
                StrategyErrorKind::SignatureInvalid,
                "numba_inspect",
                "signature_required",
                "strategy package signature is required",
            ));
        }
        let uri = package.uri.strip_prefix("file://").ok_or_else(|| {
            StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "unsupported_uri",
                "only file package URIs are enabled",
            )
        })?;
        let root = std::fs::canonicalize(uri).map_err(|_| {
            StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "package_not_found",
                "strategy package could not be opened",
            )
        })?;
        if self.context.allowed_artifact_roots.is_empty() {
            return Err(StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "artifact_roots_empty",
                "no artifact roots are authorized",
            ));
        }
        let authorized = self.context.allowed_artifact_roots.iter().any(|allowed| {
            std::fs::canonicalize(allowed.as_ref()).is_ok_and(|allowed| root.starts_with(allowed))
        });
        if !authorized {
            return Err(StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "artifact_root_denied",
                "strategy package is outside authorized roots",
            ));
        }
        let bytes = std::fs::read(root.join("strategy-manifest.json")).map_err(|_| {
            StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "manifest_not_found",
                "strategy package manifest could not be read",
            )
        })?;
        let manifest = serde_json::from_slice(&bytes).map_err(|_| {
            StrategyError::new(
                StrategyErrorKind::LoadFailed,
                "numba_inspect",
                "manifest_invalid",
                "strategy package manifest is invalid",
            )
        })?;
        Ok((root, manifest))
    }
}

#[cfg(feature = "numba-loader")]
fn digest_content_files(root: &std::path::Path, files: &[String]) -> LocalResult<[u8; 32]> {
    if files.is_empty() {
        return Err(StrategyError::new(
            StrategyErrorKind::LoadFailed,
            "numba_inspect",
            "content_files_empty",
            "manifest content file list is empty",
        ));
    }
    let mut files = files.to_vec();
    files.sort();
    files.dedup();
    let mut digest = Sha256::new();
    for relative in files {
        let path = root.join(&relative);
        let canonical = std::fs::canonicalize(&path).map_err(|_| {
            StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "content_file_missing",
                "strategy package content file is missing",
            )
        })?;
        if !canonical.starts_with(root) {
            return Err(StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "content_path_escape",
                "strategy package content path escapes its root",
            ));
        }
        let bytes = std::fs::read(canonical).map_err(|_| {
            StrategyError::new(
                StrategyErrorKind::PackageNotFound,
                "numba_inspect",
                "content_file_unreadable",
                "strategy package content file could not be read",
            )
        })?;
        digest.update(relative.as_bytes());
        digest.update([0]);
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    Ok(digest.finalize().into())
}

#[cfg(feature = "numba-loader")]
fn parse_sha256(value: &str) -> LocalResult<[u8; 32]> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() != 64 {
        return Err(digest_format_error());
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| digest_format_error())?;
    }
    Ok(output)
}

#[cfg(feature = "numba-loader")]
fn digest_format_error() -> StrategyError {
    StrategyError::new(
        StrategyErrorKind::DigestMismatch,
        "numba_inspect",
        "digest_format",
        "manifest digest is not a SHA-256 value",
    )
}

use std::{collections::BTreeSet, fmt, sync::Arc, time::SystemTime};

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ApiVersion {
    pub major: u16,
    pub minor: u16,
}

impl ApiVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub const fn supports(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PluginIdentity {
    pub plugin_type: Arc<str>,
    pub instance_id: Arc<str>,
}

impl PluginIdentity {
    pub fn new(plugin_type: impl Into<Arc<str>>, instance_id: impl Into<Arc<str>>) -> Self {
        Self {
            plugin_type: plugin_type.into(),
            instance_id: instance_id.into(),
        }
    }
}

impl fmt::Display for PluginIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.plugin_type, self.instance_id)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ServiceScope {
    Global,
    PluginInstance(Arc<str>),
    Custom { namespace: Arc<str>, key: Arc<str> },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ScopeKind {
    Global,
    PluginInstance,
    Custom,
}

impl ServiceScope {
    pub const fn kind(&self) -> ScopeKind {
        match self {
            Self::Global => ScopeKind::Global,
            Self::PluginInstance(_) => ScopeKind::PluginInstance,
            Self::Custom { .. } => ScopeKind::Custom,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ServiceId {
    pub namespace: Arc<str>,
    pub name: Arc<str>,
}

impl ServiceId {
    pub fn new(namespace: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ServiceKey {
    pub id: ServiceId,
    pub version: Version,
    pub scope: ServiceScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CallMode {
    Inline,
    Command,
    Async,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ExecutionModel {
    Dedicated,
    Background,
    Passive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReloadPolicy {
    Never,
    WhenQuiescent,
    RestartRequired,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum EventQos {
    Latest,
    ReliableOrdered,
    BestEffort,
}

#[derive(Clone, Debug)]
pub struct ProvidedService {
    pub id: ServiceId,
    pub version: Version,
    pub scope_kind: ScopeKind,
    pub call_mode: CallMode,
}

#[derive(Clone, Debug)]
pub struct RequiredService {
    pub id: ServiceId,
    pub version: VersionReq,
    pub scope_kind: ScopeKind,
    pub required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedEvent {
    pub event_type: Arc<str>,
    pub schema_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscribedEvent {
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub allowed_qos: BTreeSet<EventQos>,
}

#[derive(Clone, Debug)]
pub struct PluginManifest {
    pub plugin_type: Arc<str>,
    pub name: Arc<str>,
    pub version: Version,
    pub engine_api_version: ApiVersion,
    pub abi_version: ApiVersion,
    pub config_schema: Arc<serde_json::Value>,
    pub provides: Vec<ProvidedService>,
    pub requires: Vec<RequiredService>,
    pub publishes: Vec<PublishedEvent>,
    pub subscribes: Vec<SubscribedEvent>,
    pub supported_execution_models: BTreeSet<ExecutionModel>,
    pub reload_policy: ReloadPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallbackBudget {
    pub soft_budget_us: u64,
    pub stall_threshold_us: u64,
    pub max_consecutive_violations: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSpec {
    pub model: ExecutionModel,
    pub cpu_affinity: Option<usize>,
    pub callback_budget: Option<CallbackBudget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionLimits {
    pub max_capacity: usize,
    pub allowed_qos: BTreeSet<EventQos>,
}

#[derive(Clone, Debug)]
pub struct ConfigSnapshot {
    pub version: u64,
    pub hash: Arc<str>,
    pub loaded_at: SystemTime,
    pub source: Arc<str>,
    pub value: Arc<serde_json::Value>,
}

impl ConfigSnapshot {
    pub fn new(version: u64, value: serde_json::Value) -> Self {
        Self {
            version,
            hash: Arc::from(format!("v{version}")),
            loaded_at: SystemTime::now(),
            source: Arc::from("memory"),
            value: Arc::new(value),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PluginSpec {
    pub instance_id: Arc<str>,
    pub plugin_type: Arc<str>,
    pub config: Arc<ConfigSnapshot>,
    pub enabled: bool,
    pub execution: ExecutionSpec,
    pub subscription_limits: SubscriptionLimits,
    /// Concrete scopes for services exported by this instance, keyed by service id.
    pub service_scopes: Vec<(ServiceId, ServiceScope)>,
    /// Concrete selectors for required services, keyed by service id.
    pub required_service_scopes: Vec<(ServiceId, ServiceScope)>,
}

impl PluginSpec {
    pub fn identity(&self) -> PluginIdentity {
        PluginIdentity::new(self.plugin_type.clone(), self.instance_id.clone())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: u64,
    pub causation_id: u64,
}

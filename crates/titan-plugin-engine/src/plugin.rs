use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    BoundServices, ConfigSnapshot, EventPublisher, PluginError, PluginIdentity,
    ResourceScopeHandle, ScopedEventRouter, ServiceEndpoint, ServiceKey, SubscriptionBinding,
};

#[derive(Clone)]
pub struct PluginInit {
    pub identity: PluginIdentity,
    pub config: Arc<ConfigSnapshot>,
}

pub struct ValidationContext {
    pub identity: PluginIdentity,
    pub config: Arc<ConfigSnapshot>,
    pub services: BoundServices,
}

pub struct PluginContext {
    pub identity: PluginIdentity,
    pub config: Arc<ConfigSnapshot>,
    pub services: BoundServices,
    pub events: EventPublisher,
    pub event_routes: Option<ScopedEventRouter>,
    pub resources: ResourceScopeHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    Shutdown,
    Restart,
    Failure,
}

pub trait Plugin: Send + 'static {
    fn validate(&self, context: &ValidationContext) -> Result<(), PluginError>;
    fn start(&mut self, context: &mut PluginContext) -> Result<(), PluginError>;
    fn quiesce(&mut self, reason: StopReason) -> Result<(), PluginError>;
    fn stop(&mut self) -> Result<(), PluginError>;
}

#[derive(Clone)]
pub struct ServiceExport {
    pub service_key: ServiceKey,
    pub endpoint: Arc<dyn ServiceEndpoint>,
}

pub struct PluginBundle {
    pub lifecycle: Box<dyn Plugin>,
    pub service_exports: Vec<ServiceExport>,
    pub subscription_bindings: Vec<SubscriptionBinding>,
}

pub trait PluginFactory: Send + Sync + 'static {
    fn manifest(&self) -> &'static crate::PluginManifest;
    fn create(&self, init: PluginInit) -> Result<PluginBundle, PluginError>;
}

pub(crate) fn publication_grants(
    manifest: &crate::PluginManifest,
) -> BTreeMap<Arc<str>, BTreeSet<u32>> {
    let mut grants = BTreeMap::new();
    for event in &manifest.publishes {
        grants
            .entry(event.event_type.clone())
            .or_insert_with(BTreeSet::new)
            .insert(event.schema_version);
    }
    grants
}

pub(crate) fn subscription_grants(
    manifest: &crate::PluginManifest,
) -> BTreeMap<(Arc<str>, u32), BTreeSet<crate::EventQos>> {
    manifest
        .subscribes
        .iter()
        .map(|event| {
            (
                (event.event_type.clone(), event.schema_version),
                event.allowed_qos.clone(),
            )
        })
        .collect()
}

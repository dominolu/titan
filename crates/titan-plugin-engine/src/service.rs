use std::{
    any::Any,
    collections::BTreeMap,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwapOption;

use crate::{
    ActivationGate, ErrorKind, LifecycleState, PluginError, PluginIdentity, ServiceKey,
    TraceContext,
};

pub type BoxValue = Box<dyn Any + Send>;

pub trait ServiceEndpoint: Send + Sync + 'static {
    fn call(&self, request: BoxValue, trace: TraceContext) -> Result<BoxValue, PluginError>;
    fn as_any(&self) -> &dyn Any;
}

pub trait Service: Send + Sync + 'static {
    type Request: Send + 'static;
    type Response: Send + 'static;
}

pub trait TypedServiceEndpoint<S: Service>: Send + Sync + 'static {
    fn call(&self, request: S::Request, trace: TraceContext) -> Result<S::Response, PluginError>;
}

struct TypedEndpointAdapter<S: Service, E: TypedServiceEndpoint<S>> {
    inner: E,
    _service: PhantomData<fn() -> S>,
}

impl<S: Service, E: TypedServiceEndpoint<S>> ServiceEndpoint for TypedEndpointAdapter<S, E> {
    fn call(&self, request: BoxValue, trace: TraceContext) -> Result<BoxValue, PluginError> {
        let request = request.downcast::<S::Request>().map_err(|_| {
            crate::engine_error(
                ErrorKind::PluginFailed,
                "typed_service_call",
                "request type mismatch",
            )
        })?;
        Ok(Box::new(self.inner.call(*request, trace)?))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn typed_endpoint<S: Service, E: TypedServiceEndpoint<S>>(
    endpoint: E,
) -> Arc<dyn ServiceEndpoint> {
    boxed_typed_endpoint::<S>(Arc::new(endpoint))
}

pub struct EndpointVersion {
    pub generation: u64,
    pub endpoint: Arc<dyn ServiceEndpoint>,
    pub activation_gate: Arc<ActivationGate>,
}

/// Stable slot. Loading an Arc creates the endpoint/code lease required for safe replacement.
pub struct EndpointSlot {
    value: ArcSwapOption<EndpointVersion>,
    next_generation: AtomicU64,
}

impl Default for EndpointSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointSlot {
    pub fn new() -> Self {
        Self {
            value: ArcSwapOption::empty(),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn publish(&self, endpoint: Arc<dyn ServiceEndpoint>, gate: Arc<ActivationGate>) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.value.store(Some(Arc::new(EndpointVersion {
            generation,
            endpoint,
            activation_gate: gate,
        })));
        generation
    }

    pub fn set_unavailable(&self) {
        self.value.store(None);
    }

    pub fn load(&self) -> Option<Arc<EndpointVersion>> {
        self.value.load_full()
    }

    pub fn generation(&self) -> Option<u64> {
        self.value.load().as_ref().map(|value| value.generation)
    }
}

#[derive(Clone)]
pub struct UntypedServiceHandle {
    key: ServiceKey,
    provider: PluginIdentity,
    slot: Arc<EndpointSlot>,
}

impl UntypedServiceHandle {
    pub fn key(&self) -> &ServiceKey {
        &self.key
    }
    pub fn provider(&self) -> &PluginIdentity {
        &self.provider
    }
    pub fn generation(&self) -> Option<u64> {
        self.slot.generation()
    }

    pub fn call(&self, request: BoxValue, trace: TraceContext) -> Result<BoxValue, PluginError> {
        let version = self
            .slot
            .load()
            .ok_or_else(|| self.error(ErrorKind::ServiceUnavailable, "provider is unavailable"))?;
        if !version.activation_gate.is_active() {
            return Err(self.error(
                ErrorKind::RuntimeNotActive,
                "provider runtime is not active",
            ));
        }
        catch_unwind(AssertUnwindSafe(|| version.endpoint.call(request, trace)))
            .map_err(|_| self.error(ErrorKind::PluginFailed, "service endpoint panicked"))?
    }

    fn error(&self, kind: ErrorKind, message: &str) -> PluginError {
        PluginError::new(
            kind,
            self.provider.clone(),
            LifecycleState::Running,
            "service_call",
            message,
        )
        .recoverable(true)
    }
}

#[derive(Clone)]
pub struct ServiceHandle<S: Service> {
    inner: UntypedServiceHandle,
    _service: PhantomData<fn() -> S>,
}

impl<S: Service> ServiceHandle<S> {
    pub fn call(
        &self,
        request: S::Request,
        trace: TraceContext,
    ) -> Result<S::Response, PluginError> {
        let version = self.inner.slot.load().ok_or_else(|| {
            self.inner
                .error(ErrorKind::ServiceUnavailable, "provider is unavailable")
        })?;
        if !version.activation_gate.is_active() {
            return Err(self.inner.error(
                ErrorKind::RuntimeNotActive,
                "provider runtime is not active",
            ));
        }
        // A typed adapter avoids request/response allocation on the hot path.
        // The erased fallback remains available for dynamic ABI and cold-path services.
        if let Some(adapter) = version
            .endpoint
            .as_any()
            .downcast_ref::<TypedEndpointAdapter<S, TypedEndpointBox<S>>>()
        {
            return catch_unwind(AssertUnwindSafe(|| adapter.inner.call(request, trace))).map_err(
                |_| {
                    self.inner
                        .error(ErrorKind::PluginFailed, "service endpoint panicked")
                },
            )?;
        }
        catch_unwind(AssertUnwindSafe(|| {
            version.endpoint.call(Box::new(request), trace)
        }))
        .map_err(|_| {
            self.inner
                .error(ErrorKind::PluginFailed, "service endpoint panicked")
        })??
        .downcast::<S::Response>()
        .map(|value| *value)
        .map_err(|_| {
            PluginError::new(
                ErrorKind::PluginFailed,
                self.inner.provider.clone(),
                LifecycleState::Running,
                "service_call",
                "endpoint returned an incompatible response type",
            )
        })
    }

    pub fn generation(&self) -> Option<u64> {
        self.inner.slot.generation()
    }
}

struct TypedEndpointBox<S: Service>(Arc<dyn TypedServiceEndpoint<S>>);
impl<S: Service> TypedServiceEndpoint<S> for TypedEndpointBox<S> {
    fn call(&self, request: S::Request, trace: TraceContext) -> Result<S::Response, PluginError> {
        self.0.call(request, trace)
    }
}

pub fn boxed_typed_endpoint<S: Service>(
    endpoint: Arc<dyn TypedServiceEndpoint<S>>,
) -> Arc<dyn ServiceEndpoint> {
    Arc::new(TypedEndpointAdapter::<S, TypedEndpointBox<S>> {
        inner: TypedEndpointBox(endpoint),
        _service: PhantomData,
    })
}

#[derive(Clone, Default)]
pub struct BoundServices {
    handles: Arc<BTreeMap<ServiceKey, UntypedServiceHandle>>,
}

impl BoundServices {
    pub(crate) fn new(handles: BTreeMap<ServiceKey, UntypedServiceHandle>) -> Self {
        Self {
            handles: Arc::new(handles),
        }
    }

    pub fn require<S: Service>(&self, key: &ServiceKey) -> Result<ServiceHandle<S>, PluginError> {
        self.handles
            .get(key)
            .cloned()
            .map(|inner| ServiceHandle {
                inner,
                _service: PhantomData,
            })
            .ok_or_else(|| {
                PluginError::new(
                    ErrorKind::DependencyMissing,
                    PluginIdentity::new("unknown", "consumer"),
                    LifecycleState::Resolved,
                    "bind_service",
                    format!("service {key:?} was not bound"),
                )
            })
    }

    pub fn optional<S: Service>(&self, key: &ServiceKey) -> Option<ServiceHandle<S>> {
        self.handles.get(key).cloned().map(|inner| ServiceHandle {
            inner,
            _service: PhantomData,
        })
    }
}

#[derive(Default)]
pub struct ServiceRegistry {
    slots: BTreeMap<ServiceKey, (PluginIdentity, Arc<EndpointSlot>)>,
}

impl ServiceRegistry {
    pub fn stage(
        &mut self,
        key: ServiceKey,
        provider: PluginIdentity,
    ) -> Result<Arc<EndpointSlot>, PluginError> {
        if let Some((owner, slot)) = self.slots.get(&key) {
            if owner != &provider {
                return Err(PluginError::new(
                    ErrorKind::ServiceConflict,
                    provider,
                    LifecycleState::Resolved,
                    "stage_service",
                    format!("provider conflicts with {owner}"),
                ));
            }
            return Ok(slot.clone());
        }
        let slot = Arc::new(EndpointSlot::new());
        self.slots.insert(key, (provider, slot.clone()));
        Ok(slot)
    }

    pub fn bind(&self, key: &ServiceKey) -> Option<UntypedServiceHandle> {
        self.slots
            .get(key)
            .map(|(provider, slot)| UntypedServiceHandle {
                key: key.clone(),
                provider: provider.clone(),
                slot: slot.clone(),
            })
    }

    pub fn publish(
        &self,
        key: &ServiceKey,
        endpoint: Arc<dyn ServiceEndpoint>,
        gate: Arc<ActivationGate>,
    ) -> Result<u64, PluginError> {
        self.slots
            .get(key)
            .map(|(_, slot)| slot.publish(endpoint, gate))
            .ok_or_else(|| {
                PluginError::new(
                    ErrorKind::ServiceUnavailable,
                    PluginIdentity::new("unknown", "provider"),
                    LifecycleState::Starting,
                    "publish_service",
                    "service was not staged",
                )
            })
    }

    pub fn make_unavailable(&self, key: &ServiceKey) {
        if let Some((_, slot)) = self.slots.get(key) {
            slot.set_unavailable();
        }
    }

    pub fn remove(&mut self, key: &ServiceKey) {
        if let Some((_, slot)) = self.slots.remove(key) {
            slot.set_unavailable();
        }
    }
}

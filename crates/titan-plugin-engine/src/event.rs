use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    ActivationGate, ErrorKind, EventQos, LifecycleState, PluginError, PluginIdentity,
    ResourceScopeHandle, TraceContext,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionSpec {
    pub event_type: Arc<str>,
    pub schema_version: u32,
    pub qos: EventQos,
    pub capacity: usize,
    pub routing_keys: Arc<[u64]>,
}

pub trait EventHandler: Send + Sync + 'static {
    fn handle(&self, event: EventView<'_>) -> Result<(), PluginError>;
}

#[derive(Clone, Copy)]
pub struct EventView<'a> {
    pub event_type: &'a str,
    pub schema_version: u32,
    pub payload: &'a [u8],
    pub trace: TraceContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventPublishMetadata {
    pub source_id: u32,
    pub source_sequence: u64,
    pub exchange_ts: i64,
    pub receive_ts: i64,
    pub publish_ts: i64,
    pub routing_key: u64,
    pub flags: u32,
}

#[derive(Clone)]
pub struct SubscriptionBinding {
    pub spec: SubscriptionSpec,
    pub handler: Arc<dyn EventHandler>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteVersion(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteTransaction(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionCandidate(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionToken(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Delivered,
    Idle,
    Closed,
}

/// Consumer side of a SubscriberChannel. Implementations retain the EventLease while invoking
/// the handler; EventEngine implementations must never invoke handlers themselves.
pub trait EventReceiver: Send + Sync + 'static {
    fn dispatch_next(
        &self,
        handler: &dyn EventHandler,
        idle_wait: Duration,
    ) -> Result<DispatchOutcome, PluginError>;
}

#[derive(Clone)]
pub struct CommittedSubscription {
    pub token: SubscriptionToken,
    pub receiver: Arc<dyn EventReceiver>,
}

impl std::fmt::Debug for CommittedSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedSubscription")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

pub trait EventControl: Send + Sync + 'static {
    fn api_version(&self) -> crate::ApiVersion;
    fn current_route_version(&self) -> RouteVersion;
    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, PluginError>;
    fn stage_subscription(
        &self,
        transaction: RouteTransaction,
        owner: &PluginIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, PluginError>;
    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), PluginError>;
    fn abort(&self, transaction: RouteTransaction);
    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), PluginError>;
    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError>;
    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let _ = metadata;
        self.publish(event_type, schema_version, payload, trace)
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    owner: PluginIdentity,
    allowed: Arc<std::collections::BTreeMap<Arc<str>, BTreeSet<u32>>>,
    gate: Arc<ActivationGate>,
    control: Arc<dyn EventControl>,
}

impl EventPublisher {
    pub(crate) fn new(
        owner: PluginIdentity,
        allowed: std::collections::BTreeMap<Arc<str>, BTreeSet<u32>>,
        gate: Arc<ActivationGate>,
        control: Arc<dyn EventControl>,
    ) -> Self {
        Self {
            owner,
            allowed: Arc::new(allowed),
            gate,
            control,
        }
    }

    pub fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        self.publish_with_metadata(
            event_type,
            schema_version,
            payload,
            EventPublishMetadata::default(),
            trace,
        )
    }

    pub fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        if !self
            .allowed
            .get(event_type)
            .is_some_and(|versions| versions.contains(&schema_version))
        {
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "publish_event",
                format!("event {event_type}@{schema_version} is not authorized"),
            ));
        }
        if !self.gate.is_active() {
            return Err(PluginError::new(
                ErrorKind::RuntimeNotActive,
                self.owner.clone(),
                LifecycleState::Starting,
                "publish_event",
                "activation gate is closed",
            )
            .recoverable(true));
        }
        self.control
            .publish_with_metadata(event_type, schema_version, payload, metadata, trace)
    }
}

#[derive(Clone)]
pub struct ScopedEventRouter {
    owner: PluginIdentity,
    allowed: Arc<std::collections::BTreeMap<(Arc<str>, u32), BTreeSet<EventQos>>>,
    max_capacity: usize,
    allowed_qos: Arc<BTreeSet<EventQos>>,
    gate: Arc<ActivationGate>,
    control: Arc<dyn EventControl>,
    resources: ResourceScopeHandle,
}

impl ScopedEventRouter {
    pub(crate) fn new(
        owner: PluginIdentity,
        allowed: std::collections::BTreeMap<(Arc<str>, u32), BTreeSet<EventQos>>,
        max_capacity: usize,
        allowed_qos: BTreeSet<EventQos>,
        gate: Arc<ActivationGate>,
        control: Arc<dyn EventControl>,
        resources: ResourceScopeHandle,
    ) -> Self {
        Self {
            owner,
            allowed: Arc::new(allowed),
            max_capacity,
            allowed_qos: Arc::new(allowed_qos),
            gate,
            control,
            resources,
        }
    }

    pub fn subscribe(
        &self,
        spec: SubscriptionSpec,
        handler: Arc<dyn EventHandler>,
    ) -> Result<SubscriptionToken, PluginError> {
        let allowed = self
            .allowed
            .get(&(spec.event_type.clone(), spec.schema_version));
        if spec.capacity == 0
            || spec.capacity > self.max_capacity
            || !self.allowed_qos.contains(&spec.qos)
            || !allowed.is_some_and(|qos| qos.contains(&spec.qos))
        {
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "subscription exceeds granted capability",
            ));
        }
        let tx = self
            .control
            .begin_route_update(self.control.current_route_version())?;
        if let Err(error) = self.control.stage_subscription(tx, &self.owner, &spec) {
            self.control.abort(tx);
            return Err(error);
        }
        let (_, mut committed) = self.control.commit_at_safe_point(tx)?;
        if committed.len() != 1 {
            for subscription in committed {
                let _ = self.control.retire_subscription(subscription.token);
            }
            return Err(PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "event engine returned an invalid subscription set",
            ));
        }
        let subscription = committed.pop().ok_or_else(|| {
            PluginError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                LifecycleState::Running,
                "dynamic_subscribe",
                "event engine did not return a token",
            )
        })?;
        let token = subscription.token;
        let runtime = SubscriptionRuntime::start(
            self.owner.clone(),
            self.gate.clone(),
            subscription.receiver,
            handler,
            self.control.clone(),
            token,
        )?;
        self.resources.register("dynamic-subscription", runtime)?;
        Ok(token)
    }
}

pub(crate) struct SubscriptionRuntime {
    identity: PluginIdentity,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    control: Arc<dyn EventControl>,
    token: SubscriptionToken,
}

impl SubscriptionRuntime {
    pub(crate) fn start(
        identity: PluginIdentity,
        gate: Arc<ActivationGate>,
        receiver: Arc<dyn EventReceiver>,
        handler: Arc<dyn EventHandler>,
        control: Arc<dyn EventControl>,
        token: SubscriptionToken,
    ) -> Result<Self, PluginError> {
        let stop = Arc::new(AtomicBool::new(false));
        let runtime_stop = stop.clone();
        let error_identity = identity.clone();
        let thread = std::thread::Builder::new()
            .name(format!("titan-subscriber-{}", identity.instance_id))
            .spawn(move || {
                if gate.wait_until_active() != crate::ActivationState::Active {
                    return;
                }
                while !runtime_stop.load(Ordering::Acquire) && gate.is_active() {
                    match receiver.dispatch_next(handler.as_ref(), Duration::from_millis(1)) {
                        Ok(DispatchOutcome::Delivered | DispatchOutcome::Idle) => {}
                        Ok(DispatchOutcome::Closed) | Err(_) => break,
                    }
                }
            })
            .map_err(|error| {
                PluginError::new(
                    ErrorKind::RuntimeStartFailed,
                    error_identity,
                    LifecycleState::Starting,
                    "start_subscriber_runtime",
                    error.to_string(),
                )
            })?;
        Ok(Self {
            identity,
            stop,
            thread: Some(thread),
            control,
            token,
        })
    }
}

impl crate::Resource for SubscriptionRuntime {
    fn close(&mut self) -> Result<(), PluginError> {
        self.stop.store(true, Ordering::Release);
        let join_error = self.thread.take().and_then(|thread| {
            thread.join().is_err().then(|| {
                PluginError::new(
                    ErrorKind::PluginFailed,
                    self.identity.clone(),
                    LifecycleState::Stopping,
                    "join_subscriber_runtime",
                    "subscriber runtime panicked",
                )
            })
        });
        let retire_result = self.control.retire_subscription(self.token);
        if let Some(error) = join_error {
            return Err(error);
        }
        retire_result
    }
}

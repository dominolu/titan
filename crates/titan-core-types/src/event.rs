use std::{collections::BTreeSet, sync::Arc, time::Duration};

use crate::{
    ActivationGate, ComponentIdentity, ComponentState, CoreError, ErrorKind, EventQos, TraceContext,
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
    fn handle(&self, event: EventView<'_>) -> Result<(), CoreError>;
}

#[derive(Clone, Copy)]
pub struct EventView<'a> {
    pub event_type: &'a str,
    pub schema_version: u32,
    pub payload: &'a [u8],
    /// Canonical publication metadata. Keeping this attached to the borrowed view prevents
    /// downstream adapters from losing source identity, sequencing and snapshot flags.
    pub metadata: EventPublishMetadata,
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

/// Capabilities whose semantics are part of Core Runtime API v2.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventApiCapabilities(pub u64);

impl EventApiCapabilities {
    pub const PRIMARY_ASYNC_DELIVERY: u64 = 1 << 0;
    pub const RELIABLE_PENDING: u64 = 1 << 1;
    pub const SUBSCRIBER_WATERMARKS: u64 = 1 << 2;
    pub const SUBSCRIBER_HEALTH: u64 = 1 << 3;
    pub const SNAPSHOT_BARRIER: u64 = 1 << 4;
    pub const LANE_SAFE_POINT: u64 = 1 << 5;

    pub const V2_REQUIRED: Self = Self(
        Self::PRIMARY_ASYNC_DELIVERY
            | Self::RELIABLE_PENDING
            | Self::SUBSCRIBER_WATERMARKS
            | Self::SUBSCRIBER_HEALTH
            | Self::SNAPSHOT_BARRIER
            | Self::LANE_SAFE_POINT,
    );

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Delivered,
    Idle,
    Closed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventReceiverDiagnostics {
    pub channel_depth: usize,
    pub pending_depth: usize,
    pub outstanding_handles: usize,
}

/// Consumer side of a SubscriberChannel. Implementations retain the EventLease while invoking
/// the handler; EventEngine implementations must never invoke handlers themselves.
pub trait EventReceiver: Send + Sync + 'static {
    fn dispatch_next(
        &self,
        handler: &dyn EventHandler,
        idle_wait: Duration,
    ) -> Result<DispatchOutcome, CoreError>;
    fn diagnostics(&self) -> EventReceiverDiagnostics {
        EventReceiverDiagnostics::default()
    }
}

/// An authorized, fixed-size market payload reserved from the runtime event arena.
/// Dropping without committing returns the reservation without publishing it.
pub trait EventPayloadReservation: Send {
    fn payload_mut(&mut self) -> &mut [u8];
    fn commit(self: Box<Self>) -> Result<(), CoreError>;
}

#[derive(Clone)]
pub struct CommittedSubscription {
    pub token: SubscriptionToken,
    pub mailbox_id: u64,
    pub receiver: Arc<dyn EventReceiver>,
}

impl std::fmt::Debug for CommittedSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedSubscription")
            .field("token", &self.token)
            .field("mailbox_id", &self.mailbox_id)
            .finish_non_exhaustive()
    }
}

pub trait EventControl: Send + Sync + 'static {
    fn api_version(&self) -> crate::ApiVersion;
    fn api_capabilities(&self) -> EventApiCapabilities {
        EventApiCapabilities::default()
    }
    fn current_route_version(&self) -> RouteVersion;
    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, CoreError>;
    fn stage_subscription(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError>;
    fn stage_subscription_in_mailbox(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        mailbox: &str,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError> {
        let _ = mailbox;
        self.stage_subscription(transaction, owner, spec)
    }
    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), CoreError>;
    fn abort(&self, transaction: RouteTransaction);
    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), CoreError>;
    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), CoreError>;
    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), CoreError> {
        let _ = metadata;
        self.publish(event_type, schema_version, payload, trace)
    }
    fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        let _ = (event_type, schema_version, payload_length, metadata, trace);
        Err(CoreError::new(
            ErrorKind::SubscriptionRejected,
            ComponentIdentity::new("titan.core", "event-control"),
            ComponentState::Running,
            "reserve_market_batch",
            "event control does not support market batch reservations",
        ))
    }

    fn reserve_event_payload(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        let _ = (event_type, schema_version, payload_length, metadata, trace);
        Err(CoreError::new(
            ErrorKind::SubscriptionRejected,
            ComponentIdentity::new("titan.core", "event-control"),
            ComponentState::Running,
            "reserve_event_payload",
            "event control does not support event payload reservations",
        ))
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    owner: ComponentIdentity,
    allowed: Arc<std::collections::BTreeMap<Arc<str>, BTreeSet<u32>>>,
    gate: Arc<ActivationGate>,
    control: Arc<dyn EventControl>,
}

impl EventPublisher {
    fn authorize(&self, event_type: &str, schema_version: u32) -> Result<(), CoreError> {
        if !self
            .allowed
            .get(event_type)
            .is_some_and(|versions| versions.contains(&schema_version))
        {
            return Err(CoreError::new(
                ErrorKind::SubscriptionRejected,
                self.owner.clone(),
                ComponentState::Running,
                "publish_event",
                format!("event {event_type}@{schema_version} is not authorized"),
            ));
        }
        if !self.gate.is_active() {
            return Err(CoreError::new(
                ErrorKind::RuntimeNotActive,
                self.owner.clone(),
                ComponentState::Starting,
                "publish_event",
                "activation gate is closed",
            )
            .recoverable(true));
        }
        Ok(())
    }

    pub fn new(
        owner: ComponentIdentity,
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
    ) -> Result<(), CoreError> {
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
    ) -> Result<(), CoreError> {
        self.authorize(event_type, schema_version)?;
        self.control
            .publish_with_metadata(event_type, schema_version, payload, metadata, trace)
    }

    pub fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        self.authorize(event_type, schema_version)?;
        self.control.reserve_market_batch(
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }

    /// Reserves the event's declared arena pool and publishes only when the returned reservation
    /// is committed. Dropping it rolls the block back without emitting a partial event.
    pub fn reserve_event_payload(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        self.authorize(event_type, schema_version)?;
        self.control.reserve_event_payload(
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }
}

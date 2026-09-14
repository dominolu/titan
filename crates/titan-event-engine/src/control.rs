use std::{sync::Arc, time::Duration};

use crossbeam_channel::bounded;
use titan_core_types::{
    ApiVersion, CommittedSubscription, ComponentIdentity, ComponentState, CoreError, ErrorKind,
    EventApiCapabilities, EventControl, EventPayloadReservation, EventPublishMetadata,
    RouteTransaction, RouteVersion, SubscriptionCandidate, SubscriptionSpec, SubscriptionToken,
    TraceContext,
};

use crate::{
    ControlCommand, EngineError, EventEngineHandle, PublishError, PublishRequest,
    StagedSubscription,
};

/// Explicit compatibility surface for v1.3-era normal-route consumers.
///
/// The adapter deliberately advertises Core Runtime API v1 and no v2 capabilities.  A v2
/// Older hosts can continue to use the normal route during a controlled migration. PRIMARY lanes
/// and snapshot barriers are intentionally absent.
#[derive(Clone)]
pub struct V13EventControlAdapter {
    inner: EventEngineHandle,
}

impl V13EventControlAdapter {
    pub fn new(inner: EventEngineHandle) -> Self {
        Self { inner }
    }
}

impl EventControl for V13EventControlAdapter {
    fn api_version(&self) -> ApiVersion {
        titan_core_types::CORE_RUNTIME_V1_COMPAT_VERSION
    }

    fn current_route_version(&self) -> RouteVersion {
        EventControl::current_route_version(&self.inner)
    }

    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, CoreError> {
        EventControl::begin_route_update(&self.inner, base)
    }

    fn stage_subscription(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError> {
        EventControl::stage_subscription(&self.inner, transaction, owner, spec)
    }

    fn stage_subscription_in_mailbox(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        mailbox: &str,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError> {
        EventControl::stage_subscription_in_mailbox(&self.inner, transaction, owner, mailbox, spec)
    }

    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), CoreError> {
        EventControl::commit_at_safe_point(&self.inner, transaction)
    }

    fn abort(&self, transaction: RouteTransaction) {
        EventControl::abort(&self.inner, transaction);
    }

    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), CoreError> {
        EventControl::retire_subscription(&self.inner, token)
    }

    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), CoreError> {
        EventControl::publish(&self.inner, event_type, schema_version, payload, trace)
    }

    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), CoreError> {
        EventControl::publish_with_metadata(
            &self.inner,
            event_type,
            schema_version,
            payload,
            metadata,
            trace,
        )
    }
}

impl EventControl for EventEngineHandle {
    fn api_version(&self) -> ApiVersion {
        titan_core_types::CORE_RUNTIME_API_VERSION
    }

    fn api_capabilities(&self) -> EventApiCapabilities {
        EventApiCapabilities::V2_REQUIRED
    }

    fn current_route_version(&self) -> RouteVersion {
        RouteVersion(
            self.shared
                .route_version
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, CoreError> {
        // Route candidates are intentionally stageable before the event loop starts. Titan main
        // validates the complete runtime graph first, starts EventEngine, and only then commits the
        // candidate at a safe point. Publication and commit still reject a stopped engine.
        if base != self.current_route_version() {
            return Err(control_error(
                ErrorKind::SubscriptionRejected,
                "begin_route_update",
                "route base version is stale",
                true,
            ));
        }
        let id = self
            .shared
            .next_transaction
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.transactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, (base.0, Vec::new()));
        Ok(RouteTransaction(id))
    }

    fn stage_subscription(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError> {
        stage_subscription(self, transaction, owner, None, spec)
    }

    fn stage_subscription_in_mailbox(
        &self,
        transaction: RouteTransaction,
        owner: &ComponentIdentity,
        mailbox: &str,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, CoreError> {
        stage_subscription(self, transaction, owner, Some(Arc::from(mailbox)), spec)
    }

    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), CoreError> {
        if !self
            .shared
            .running
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(control_error(
                ErrorKind::RuntimeNotActive,
                "commit_at_safe_point",
                "event loop is not running",
                true,
            ));
        }
        let (base_version, staged) = self
            .transactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&transaction.0)
            .ok_or_else(|| {
                control_error(
                    ErrorKind::SubscriptionRejected,
                    "commit_at_safe_point",
                    "unknown route transaction",
                    false,
                )
            })?;
        let (reply_tx, reply_rx) = bounded(1);
        self.shared
            .control_tx
            .try_send(ControlCommand::Commit {
                base_version,
                staged,
                reply: reply_tx,
            })
            .map_err(|_| {
                control_error(
                    ErrorKind::ControlQueueFull,
                    "commit_at_safe_point",
                    "event control queue is full",
                    true,
                )
            })?;
        let (version, tokens) = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| {
                control_error(
                    ErrorKind::ControlDeadlineExceeded,
                    "commit_at_safe_point",
                    "event loop did not reach a safe point before the deadline",
                    true,
                )
            })?
            .map_err(engine_control_error)?;
        Ok((
            RouteVersion(version),
            tokens
                .into_iter()
                .map(|(token, mailbox_id, channel)| CommittedSubscription {
                    token: SubscriptionToken(token),
                    mailbox_id,
                    receiver: channel,
                })
                .collect(),
        ))
    }

    fn abort(&self, transaction: RouteTransaction) {
        self.transactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&transaction.0);
    }

    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), CoreError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.shared
            .control_tx
            .try_send(ControlCommand::Retire {
                token: token.0,
                reply: reply_tx,
            })
            .map_err(|_| {
                control_error(
                    ErrorKind::ControlQueueFull,
                    "retire_subscription",
                    "event control queue is full",
                    true,
                )
            })?;
        let (channel, stop_channel) = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| {
                control_error(
                    ErrorKind::ControlDeadlineExceeded,
                    "retire_subscription",
                    "event loop did not retire the route before the deadline",
                    true,
                )
            })?
            .map_err(engine_control_error)?;
        if stop_channel {
            channel.stop_and_drain();
        }
        Ok(())
    }

    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), CoreError> {
        publish(self, event_type, schema_version, payload, trace)
    }

    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), CoreError> {
        publish_with_metadata(self, event_type, schema_version, payload, metadata, trace)
    }

    fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        reserve_market_batch(
            self,
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }

    fn reserve_event_payload(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
        reserve_event_payload(
            self,
            event_type,
            schema_version,
            payload_length,
            metadata,
            trace,
        )
    }
}

fn stage_subscription(
    handle: &EventEngineHandle,
    transaction: RouteTransaction,
    owner: &ComponentIdentity,
    mailbox: Option<Arc<str>>,
    spec: &SubscriptionSpec,
) -> Result<SubscriptionCandidate, CoreError> {
    if spec.capacity == 0
        || spec.capacity <= handle.shared.config.subscribers.critical_reserve
        || spec.capacity > handle.shared.config.subscribers.default_capacity
        || handle
            .shared
            .descriptor(&spec.event_type, spec.schema_version)
            .is_none()
    {
        return Err(control_error(
            ErrorKind::SubscriptionRejected,
            "stage_subscription",
            "event is not registered or subscription capacity is invalid",
            false,
        ));
    }
    let mut transactions = handle
        .transactions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (_, staged) = transactions.get_mut(&transaction.0).ok_or_else(|| {
        control_error(
            ErrorKind::SubscriptionRejected,
            "stage_subscription",
            "unknown route transaction",
            false,
        )
    })?;
    staged.push(StagedSubscription {
        owner: owner.clone(),
        mailbox,
        spec: spec.clone(),
    });
    let candidate = handle
        .shared
        .next_candidate
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(SubscriptionCandidate(candidate))
}

fn publish(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload: &[u8],
    trace: TraceContext,
) -> Result<(), CoreError> {
    let mut request = PublishRequest::new(event_type, schema_version, payload);
    request.trace = trace;
    handle.try_publish(request).map_err(publish_control_error)
}

fn publish_with_metadata(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload: &[u8],
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<(), CoreError> {
    let mut request = PublishRequest::new(event_type, schema_version, payload);
    request.source_id = metadata.source_id;
    request.source_sequence = metadata.source_sequence;
    request.exchange_ts = metadata.exchange_ts;
    request.receive_ts = metadata.receive_ts;
    request.publish_ts = metadata.publish_ts;
    request.routing_key = metadata.routing_key;
    request.flags = metadata.flags;
    request.trace = trace;
    handle.try_publish(request).map_err(publish_control_error)
}

fn reserve_market_batch(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload_length: usize,
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
    let mut request = crate::ReserveRequest::new(event_type, schema_version, payload_length);
    request.source_id = metadata.source_id;
    request.source_sequence = metadata.source_sequence;
    request.exchange_ts = metadata.exchange_ts;
    request.receive_ts = metadata.receive_ts;
    request.publish_ts = metadata.publish_ts;
    request.routing_key = metadata.routing_key;
    request.flags = metadata.flags;
    request.trace = trace;
    let reservation =
        EventEngineHandle::reserve_market_batch(handle, request).map_err(publish_control_error)?;
    Ok(Box::new(PluginMarketBatchReservation(Some(reservation))))
}

fn reserve_event_payload(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload_length: usize,
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<Box<dyn EventPayloadReservation>, CoreError> {
    let mut request = crate::ReserveRequest::new(event_type, schema_version, payload_length);
    request.source_id = metadata.source_id;
    request.source_sequence = metadata.source_sequence;
    request.exchange_ts = metadata.exchange_ts;
    request.receive_ts = metadata.receive_ts;
    request.publish_ts = metadata.publish_ts;
    request.routing_key = metadata.routing_key;
    request.flags = metadata.flags;
    request.trace = trace;
    let reservation =
        EventEngineHandle::reserve_event_payload(handle, request).map_err(publish_control_error)?;
    Ok(Box::new(PluginMarketBatchReservation(Some(reservation))))
}

struct PluginMarketBatchReservation(Option<crate::MarketBatchReservation>);

impl EventPayloadReservation for PluginMarketBatchReservation {
    fn payload_mut(&mut self) -> &mut [u8] {
        self.0
            .as_mut()
            .expect("reservation is consumed only by commit")
            .payload_mut()
    }

    fn commit(mut self: Box<Self>) -> Result<(), CoreError> {
        self.0
            .take()
            .expect("reservation is committed once")
            .commit()
            .map_err(publish_control_error)
    }
}

fn control_error(
    kind: ErrorKind,
    operation: &'static str,
    message: impl Into<Arc<str>>,
    recoverable: bool,
) -> CoreError {
    CoreError::new(
        kind,
        ComponentIdentity::new("titan.core.event-engine", "event-engine"),
        ComponentState::Running,
        operation,
        message,
    )
    .recoverable(recoverable)
}

fn engine_control_error(error: EngineError) -> CoreError {
    let kind = match &error {
        EngineError::ControlQueueFull => ErrorKind::ControlQueueFull,
        EngineError::ControlTimeout => ErrorKind::ControlDeadlineExceeded,
        _ => ErrorKind::SubscriptionRejected,
    };
    control_error(kind, "event_control", error.to_string(), true)
}

fn publish_control_error(error: PublishError) -> CoreError {
    let kind = match error {
        PublishError::Stopped => ErrorKind::RuntimeNotActive,
        PublishError::InvalidEvent | PublishError::PayloadTooLarge { .. } => {
            ErrorKind::SubscriptionRejected
        }
        PublishError::EventArenaExhausted(_)
        | PublishError::CriticalIngressFull
        | PublishError::MarketIngressFull => ErrorKind::ControlQueueFull,
    };
    control_error(kind, "publish_event", error.to_string(), true)
}

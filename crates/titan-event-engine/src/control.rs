use std::{sync::Arc, time::Duration};

use crossbeam_channel::bounded;
use titan_plugin_engine::{
    ApiVersion, CommittedSubscription, ErrorKind, EventControl, EventPayloadReservation,
    EventPublishMetadata, LifecycleState, PluginError, PluginIdentity, RouteTransaction,
    RouteVersion, SubscriptionCandidate, SubscriptionSpec, SubscriptionToken, TraceContext,
};

use crate::{
    ControlCommand, EngineError, EventEngineHandle, PublishError, PublishRequest,
    StagedSubscription,
};

impl EventControl for EventEngineHandle {
    fn api_version(&self) -> ApiVersion {
        titan_plugin_engine::CORE_RUNTIME_API_VERSION
    }

    fn current_route_version(&self) -> RouteVersion {
        RouteVersion(
            self.shared
                .route_version
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn begin_route_update(&self, base: RouteVersion) -> Result<RouteTransaction, PluginError> {
        if !self
            .shared
            .running
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(plugin_error(
                ErrorKind::RuntimeNotActive,
                "begin_route_update",
                "event engine is not running",
                true,
            ));
        }
        if base != self.current_route_version() {
            return Err(plugin_error(
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
        owner: &PluginIdentity,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, PluginError> {
        stage_subscription(self, transaction, owner, None, spec)
    }

    fn stage_subscription_in_mailbox(
        &self,
        transaction: RouteTransaction,
        owner: &PluginIdentity,
        mailbox: &str,
        spec: &SubscriptionSpec,
    ) -> Result<SubscriptionCandidate, PluginError> {
        stage_subscription(self, transaction, owner, Some(Arc::from(mailbox)), spec)
    }

    fn commit_at_safe_point(
        &self,
        transaction: RouteTransaction,
    ) -> Result<(RouteVersion, Vec<CommittedSubscription>), PluginError> {
        let (base_version, staged) = self
            .transactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&transaction.0)
            .ok_or_else(|| {
                plugin_error(
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
                plugin_error(
                    ErrorKind::ControlQueueFull,
                    "commit_at_safe_point",
                    "event control queue is full",
                    true,
                )
            })?;
        let (version, tokens) = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| {
                plugin_error(
                    ErrorKind::ControlDeadlineExceeded,
                    "commit_at_safe_point",
                    "event loop did not reach a safe point before the deadline",
                    true,
                )
            })?
            .map_err(engine_plugin_error)?;
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

    fn retire_subscription(&self, token: SubscriptionToken) -> Result<(), PluginError> {
        let (reply_tx, reply_rx) = bounded(1);
        self.shared
            .control_tx
            .try_send(ControlCommand::Retire {
                token: token.0,
                reply: reply_tx,
            })
            .map_err(|_| {
                plugin_error(
                    ErrorKind::ControlQueueFull,
                    "retire_subscription",
                    "event control queue is full",
                    true,
                )
            })?;
        let (channel, stop_channel) = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| {
                plugin_error(
                    ErrorKind::ControlDeadlineExceeded,
                    "retire_subscription",
                    "event loop did not retire the route before the deadline",
                    true,
                )
            })?
            .map_err(engine_plugin_error)?;
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
    ) -> Result<(), PluginError> {
        publish(self, event_type, schema_version, payload, trace)
    }

    fn publish_with_metadata(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        publish_with_metadata(self, event_type, schema_version, payload, metadata, trace)
    }

    fn reserve_market_batch(
        &self,
        event_type: &str,
        schema_version: u32,
        payload_length: usize,
        metadata: EventPublishMetadata,
        trace: TraceContext,
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
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
    ) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
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
    owner: &PluginIdentity,
    mailbox: Option<Arc<str>>,
    spec: &SubscriptionSpec,
) -> Result<SubscriptionCandidate, PluginError> {
    if spec.capacity == 0
        || spec.capacity <= handle.shared.config.subscribers.critical_reserve
        || spec.capacity > handle.shared.config.subscribers.default_capacity
        || handle
            .shared
            .descriptor(&spec.event_type, spec.schema_version)
            .is_none()
    {
        return Err(plugin_error(
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
        plugin_error(
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
) -> Result<(), PluginError> {
    let mut request = PublishRequest::new(event_type, schema_version, payload);
    request.trace = trace;
    handle.try_publish(request).map_err(publish_plugin_error)
}

fn publish_with_metadata(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload: &[u8],
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<(), PluginError> {
    let mut request = PublishRequest::new(event_type, schema_version, payload);
    request.source_id = metadata.source_id;
    request.source_sequence = metadata.source_sequence;
    request.exchange_ts = metadata.exchange_ts;
    request.receive_ts = metadata.receive_ts;
    request.publish_ts = metadata.publish_ts;
    request.routing_key = metadata.routing_key;
    request.flags = metadata.flags;
    request.trace = trace;
    handle.try_publish(request).map_err(publish_plugin_error)
}

fn reserve_market_batch(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload_length: usize,
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
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
        EventEngineHandle::reserve_market_batch(handle, request).map_err(publish_plugin_error)?;
    Ok(Box::new(PluginMarketBatchReservation(Some(reservation))))
}

fn reserve_event_payload(
    handle: &EventEngineHandle,
    event_type: &str,
    schema_version: u32,
    payload_length: usize,
    metadata: EventPublishMetadata,
    trace: TraceContext,
) -> Result<Box<dyn EventPayloadReservation>, PluginError> {
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
        EventEngineHandle::reserve_event_payload(handle, request).map_err(publish_plugin_error)?;
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

    fn commit(mut self: Box<Self>) -> Result<(), PluginError> {
        self.0
            .take()
            .expect("reservation is committed once")
            .commit()
            .map_err(publish_plugin_error)
    }
}

fn plugin_error(
    kind: ErrorKind,
    operation: &'static str,
    message: impl Into<Arc<str>>,
    recoverable: bool,
) -> PluginError {
    PluginError::new(
        kind,
        PluginIdentity::new("titan.core.event-engine", "event-engine"),
        LifecycleState::Running,
        operation,
        message,
    )
    .recoverable(recoverable)
}

fn engine_plugin_error(error: EngineError) -> PluginError {
    let kind = match &error {
        EngineError::ControlQueueFull => ErrorKind::ControlQueueFull,
        EngineError::ControlTimeout => ErrorKind::ControlDeadlineExceeded,
        _ => ErrorKind::SubscriptionRejected,
    };
    plugin_error(kind, "event_control", error.to_string(), true)
}

fn publish_plugin_error(error: PublishError) -> PluginError {
    let kind = match error {
        PublishError::Stopped => ErrorKind::RuntimeNotActive,
        PublishError::InvalidEvent | PublishError::PayloadTooLarge { .. } => {
            ErrorKind::SubscriptionRejected
        }
        PublishError::EventArenaExhausted(_)
        | PublishError::CriticalIngressFull
        | PublishError::MarketIngressFull => ErrorKind::ControlQueueFull,
    };
    plugin_error(kind, "publish_event", error.to_string(), true)
}

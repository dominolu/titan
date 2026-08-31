use std::{sync::Arc, time::Duration};

use crossbeam_channel::bounded;
use titan_plugin_engine::{
    ApiVersion, CommittedSubscription, ErrorKind, EventControl, EventPublishMetadata,
    LifecycleState, PluginError, PluginIdentity, RouteTransaction, RouteVersion,
    SubscriptionCandidate, SubscriptionSpec, SubscriptionToken, TraceContext,
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
        if spec.capacity == 0
            || spec.capacity <= self.shared.config.subscribers.critical_reserve
            || spec.capacity > self.shared.config.subscribers.default_capacity
            || self
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
        let mut transactions = self
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
            spec: spec.clone(),
        });
        let candidate = self
            .shared
            .next_candidate
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(SubscriptionCandidate(candidate))
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
                .map(|(token, channel)| CommittedSubscription {
                    token: SubscriptionToken(token),
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
        let channel = reply_rx
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
        channel.stop_and_drain();
        Ok(())
    }

    fn publish(
        &self,
        event_type: &str,
        schema_version: u32,
        payload: &[u8],
        trace: TraceContext,
    ) -> Result<(), PluginError> {
        let mut request = PublishRequest::new(event_type, schema_version, payload);
        request.trace = trace;
        self.try_publish(request).map_err(publish_plugin_error)
    }

    fn publish_with_metadata(
        &self,
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
        self.try_publish(request).map_err(publish_plugin_error)
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

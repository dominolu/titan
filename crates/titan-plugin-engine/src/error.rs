use std::{sync::Arc, time::SystemTime};

use thiserror::Error;

use crate::{LifecycleState, PluginIdentity, TraceContext};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    ManifestInvalid,
    ConfigInvalid,
    ApiVersionMismatch,
    AbiVersionMismatch,
    ManifestSchemaMismatch,
    UnsupportedAbiFeature,
    DependencyMissing,
    DependencyCycle,
    ServiceConflict,
    ServiceUnavailable,
    RuntimeNotActive,
    ControlQueueFull,
    ControlDeadlineExceeded,
    SubscriptionRejected,
    RuntimeStartFailed,
    StartTimeout,
    StopTimeout,
    ResourceReleaseFailed,
    CallbackBudgetExceeded,
    CallbackStalled,
    PluginFailed,
}

#[derive(Clone, Debug, Error)]
#[error("{kind:?} during {operation} for {identity}: {message}")]
pub struct PluginError {
    pub kind: ErrorKind,
    pub identity: PluginIdentity,
    pub lifecycle_state: LifecycleState,
    pub operation: Arc<str>,
    pub message: Arc<str>,
    pub cause_chain: Vec<Arc<str>>,
    pub occurred_at: SystemTime,
    pub recoverable: bool,
    pub request_id: Option<u64>,
    pub trace_context: Option<TraceContext>,
}

impl PluginError {
    pub fn new(
        kind: ErrorKind,
        identity: PluginIdentity,
        state: LifecycleState,
        operation: impl Into<Arc<str>>,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            kind,
            identity,
            lifecycle_state: state,
            operation: operation.into(),
            message: message.into(),
            cause_chain: Vec::new(),
            occurred_at: SystemTime::now(),
            recoverable: false,
            request_id: None,
            trace_context: None,
        }
    }

    pub fn recoverable(mut self, recoverable: bool) -> Self {
        self.recoverable = recoverable;
        self
    }
}

pub(crate) fn engine_error(
    kind: ErrorKind,
    operation: &str,
    message: impl Into<Arc<str>>,
) -> PluginError {
    PluginError::new(
        kind,
        PluginIdentity::new("titan.core.plugin-engine", "plugin-engine"),
        LifecycleState::Discovered,
        operation,
        message,
    )
}

use std::{sync::Arc, time::SystemTime};

use thiserror::Error;

use crate::{ComponentIdentity, ComponentState, TraceContext};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    ComponentFailed,
}

#[derive(Clone, Debug, Error)]
#[error("{kind:?} during {operation} for {identity}: {message}")]
pub struct CoreError {
    pub kind: ErrorKind,
    pub identity: ComponentIdentity,
    pub lifecycle_state: ComponentState,
    pub operation: Arc<str>,
    pub message: Arc<str>,
    pub cause_chain: Vec<Arc<str>>,
    pub occurred_at: SystemTime,
    pub recoverable: bool,
    pub request_id: Option<u64>,
    pub trace_context: Option<TraceContext>,
}

impl CoreError {
    pub fn new(
        kind: ErrorKind,
        identity: ComponentIdentity,
        state: ComponentState,
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

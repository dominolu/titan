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
#[error(transparent)]
pub struct CoreError(Box<CoreErrorDetails>);

/// Cold-path diagnostic data; boxed to keep Result return values compact.
#[derive(Clone, Debug, Error)]
#[error("{kind:?} during {operation} for {identity}: {message}")]
pub struct CoreErrorDetails {
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
        Self(Box::new(CoreErrorDetails {
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
        }))
    }

    pub fn recoverable(mut self, recoverable: bool) -> Self {
        self.recoverable = recoverable;
        self
    }
}

impl std::ops::Deref for CoreError {
    type Target = CoreErrorDetails;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for CoreError {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn error_is_compact_and_preserves_mutable_diagnostic_context() {
        assert_eq!(
            std::mem::size_of::<CoreError>(),
            std::mem::size_of::<usize>()
        );
        let mut error = CoreError::new(
            ErrorKind::ComponentFailed,
            ComponentIdentity::new("test", "component"),
            ComponentState::Failed,
            "stop",
            "network error",
        )
        .recoverable(true);
        error.request_id = Some(42);
        error.cause_chain.push(Arc::from("venue unavailable"));
        let cloned = error.clone();
        error.message = Arc::from("changed");
        assert_eq!(&*cloned.message, "network error");
        assert_eq!(cloned.request_id, Some(42));
        assert!(cloned.recoverable);
        assert!(cloned.to_string().contains("network error"));
    }
}

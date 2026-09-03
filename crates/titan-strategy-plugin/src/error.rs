use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyErrorKind {
    InvalidDefinition,
    PackageNotFound,
    DigestMismatch,
    SignatureInvalid,
    ParameterInvalid,
    UnsupportedCapability,
    AbiMismatch,
    CompileFailed,
    LoadFailed,
    DependencyUnavailable,
    StaleHandle,
    InvalidState,
    RouteFailed,
    AsyncLaneQueueFull,
    SubscriberResyncRequired,
    CallbackFailed,
    CallbackTimeout,
    RiskRejected,
    ExecutionQueueFull,
    CheckpointFailed,
    StopTimeout,
    AlreadyExists,
    CapacityExceeded,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyError {
    pub kind: StrategyErrorKind,
    pub strategy_id: Option<u32>,
    pub generation: Option<u64>,
    pub operation_id: Option<u64>,
    pub stage: Arc<str>,
    pub reason_code: Arc<str>,
    pub message: Arc<str>,
}

impl StrategyError {
    pub fn new(
        kind: StrategyErrorKind,
        stage: impl Into<Arc<str>>,
        reason_code: impl Into<Arc<str>>,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            kind,
            strategy_id: None,
            generation: None,
            operation_id: None,
            stage: stage.into(),
            reason_code: reason_code.into(),
            message: message.into(),
        }
    }

    pub fn for_handle(mut self, handle: crate::StrategyHandle) -> Self {
        self.strategy_id = Some(handle.strategy_id.0);
        self.generation = Some(handle.generation);
        self
    }

    pub fn for_operation(mut self, operation: crate::StrategyOperationId) -> Self {
        self.operation_id = Some(operation.0);
        self
    }
}

impl fmt::Display for StrategyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} [{}:{}]: {}",
            self.kind, self.stage, self.reason_code, self.message
        )
    }
}

impl std::error::Error for StrategyError {}

pub type LocalResult<T> = Result<T, StrategyError>;

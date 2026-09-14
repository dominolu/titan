use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketErrorKind {
    FactoryNotFound,
    InvalidDefinition,
    CapacityExceeded,
    SourceNotFound,
    StaleHandle,
    AlreadyExists,
    QueueFull,
    ConnectorRejected,
    DeadlineExceeded,
    ResourceReleaseFailed,
    RuntimeNotActive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketError {
    pub kind: MarketErrorKind,
    pub message: Arc<str>,
}

impl MarketError {
    pub fn new(kind: MarketErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for MarketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for MarketError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorError {
    pub message: Arc<str>,
}

impl ConnectorError {
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConnectorError {}

pub type LocalResult<T> = Result<T, MarketError>;

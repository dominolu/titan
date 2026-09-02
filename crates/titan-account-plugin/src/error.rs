use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountErrorKind {
    FactoryNotFound,
    InvalidDefinition,
    CredentialUnavailable,
    CapacityExceeded,
    AccountNotFound,
    StaleHandle,
    AlreadyExists,
    NotReady,
    CommandConflict,
    QueueFull,
    ConnectorRejected,
    DeadlineExceeded,
    ResourceReleaseFailed,
    RuntimeNotActive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountError {
    pub kind: AccountErrorKind,
    pub message: Arc<str>,
}

impl AccountError {
    pub fn new(kind: AccountErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for AccountError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountConnectorError {
    pub kind: AccountErrorKind,
    pub message: Arc<str>,
}

impl AccountConnectorError {
    pub fn new(kind: AccountErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn rejected(message: impl Into<Arc<str>>) -> Self {
        Self::new(AccountErrorKind::ConnectorRejected, message)
    }
}

impl fmt::Display for AccountConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Connector implementations are responsible for redacting their public messages.
        f.write_str(&self.message)
    }
}

impl std::error::Error for AccountConnectorError {}

pub type LocalResult<T> = Result<T, AccountError>;

pub(crate) fn connector_error(action: &str, error: AccountConnectorError) -> AccountError {
    AccountError::new(error.kind, format!("{action}: {}", error.message))
}

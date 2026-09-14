use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ApiVersion {
    pub major: u16,
    pub minor: u16,
}

impl ApiVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub const fn supports(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

/// Stable identity used for event ownership and diagnostics.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ComponentIdentity {
    pub component_type: Arc<str>,
    pub instance_id: Arc<str>,
}

impl ComponentIdentity {
    pub fn new(component_type: impl Into<Arc<str>>, instance_id: impl Into<Arc<str>>) -> Self {
        Self {
            component_type: component_type.into(),
            instance_id: instance_id.into(),
        }
    }
}

impl fmt::Display for ComponentIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.component_type, self.instance_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentState {
    Discovered,
    Validated,
    Resolved,
    Starting,
    Running,
    Quiescing,
    Stopping,
    Stopped,
    Failed,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum EventQos {
    Latest,
    ReliableOrdered,
    BestEffort,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: u64,
    pub causation_id: u64,
}

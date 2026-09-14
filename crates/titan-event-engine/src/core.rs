use std::sync::Arc;

use thiserror::Error;

use crate::{EngineError, EventEngine, EventEngineConfig, EventEngineHandle};

#[derive(Debug, Error)]
pub enum CoreRuntimeError {
    #[error(transparent)]
    Event(#[from] EngineError),
}

/// Minimal owner for EventEngine.
///
/// Application services are composed by `TradingRuntime`; EventEngine deliberately has no
/// knowledge of application plans, service registries, or lifecycle adapters.
pub struct TitanCoreRuntime {
    events: Arc<EventEngine>,
    event_handle: Arc<EventEngineHandle>,
}

impl TitanCoreRuntime {
    pub fn new(event_config: EventEngineConfig) -> Result<Self, CoreRuntimeError> {
        let events = Arc::new(EventEngine::new(event_config)?);
        let event_handle = Arc::new(events.handle());
        Ok(Self {
            events,
            event_handle,
        })
    }

    pub fn start(&self) -> Result<(), CoreRuntimeError> {
        self.events.start()?;
        Ok(())
    }

    pub fn events(&self) -> &Arc<EventEngine> {
        &self.events
    }

    pub fn event_handle(&self) -> &Arc<EventEngineHandle> {
        &self.event_handle
    }

    pub fn shutdown(&self) -> Result<(), CoreRuntimeError> {
        self.events.stop()?;
        Ok(())
    }
}

impl Drop for TitanCoreRuntime {
    fn drop(&mut self) {
        let _ = self.events.stop();
    }
}

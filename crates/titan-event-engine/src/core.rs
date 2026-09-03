use std::sync::Arc;

use thiserror::Error;
use titan_plugin_engine::{ApiVersion, PluginEngine, PluginError, PluginSpec, StopReason};

use crate::{EngineError, EventEngine, EventEngineConfig, EventEngineHandle};

#[derive(Debug, Error)]
pub enum CoreRuntimeError {
    #[error(transparent)]
    Event(#[from] EngineError),
    #[error(transparent)]
    Plugin(#[from] PluginError),
    #[error("one or more plugins failed to stop: {0:?}")]
    PluginShutdown(Vec<PluginError>),
}

/// Owns the two peer core components and enforces their startup/shutdown order.
pub struct TitanCoreRuntime {
    events: Arc<EventEngine>,
    event_handle: Arc<EventEngineHandle>,
    plugins: PluginEngine,
}

impl TitanCoreRuntime {
    pub fn new(
        event_config: EventEngineConfig,
        host_abi: ApiVersion,
    ) -> Result<Self, CoreRuntimeError> {
        let events = Arc::new(EventEngine::new(event_config)?);
        let event_handle = Arc::new(events.handle());
        let plugins = PluginEngine::new(event_handle.clone(), host_abi)?;
        Ok(Self {
            events,
            event_handle,
            plugins,
        })
    }

    /// Starts EventEngine before any plugin plan is applied.
    pub fn start(&self) -> Result<(), CoreRuntimeError> {
        self.events.start()?;
        Ok(())
    }

    /// Performs the Core Runtime startup transaction in contract order: prepare and validate all
    /// plugin bundles while their gates are closed, start EventEngine, then commit and activate the
    /// prepared plugin graph. Either startup failure rolls back every prepared local resource.
    pub fn start_with_plugins(&mut self, specs: &[PluginSpec]) -> Result<(), CoreRuntimeError> {
        self.plugins.prepare(specs)?;
        if let Err(error) = self.events.start() {
            self.plugins.abort_prepared();
            return Err(error.into());
        }
        if let Err(error) = self.plugins.start_prepared() {
            let _ = self.events.stop();
            return Err(error.into());
        }
        Ok(())
    }

    pub fn events(&self) -> &Arc<EventEngine> {
        &self.events
    }

    pub fn event_handle(&self) -> &Arc<EventEngineHandle> {
        &self.event_handle
    }

    pub fn plugins(&self) -> &PluginEngine {
        &self.plugins
    }

    pub fn plugins_mut(&mut self) -> &mut PluginEngine {
        &mut self.plugins
    }

    /// Quiesces and stops plugins before draining and stopping EventEngine.
    pub fn shutdown(&mut self, reason: StopReason) -> Result<(), CoreRuntimeError> {
        let plugin_result = self
            .plugins
            .shutdown(reason)
            .map_err(CoreRuntimeError::PluginShutdown);
        let event_result = self.events.stop().map_err(CoreRuntimeError::Event);
        plugin_result.and(event_result)
    }
}

impl Drop for TitanCoreRuntime {
    fn drop(&mut self) {
        let _ = self.plugins.shutdown(StopReason::Shutdown);
        let _ = self.events.stop();
    }
}
